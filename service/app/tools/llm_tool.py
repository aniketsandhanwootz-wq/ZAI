# service/app/tools/llm_tool.py
from __future__ import annotations

import base64
import json
import logging
import os
import random
import re
import time
from typing import Any, Dict, List

import requests

from ..config import Settings
from .langsmith_trace import mk_http_meta, traceable_wrap, tracing_context

logger = logging.getLogger("zai.llm")


def _b64(b: bytes) -> str:
    return base64.b64encode(b).decode("utf-8")


def _extract_json(text: str) -> Dict[str, Any]:
    s = (text or "").strip()
    if not s:
        return {}

    if s.startswith("```"):
        s = s.strip().strip("`").strip()
        if s.lower().startswith("json"):
            s = s[4:].strip()

    if s.startswith("{") and s.endswith("}"):
        try:
            return json.loads(s)
        except Exception:
            return {}

    i = s.find("{")
    j = s.rfind("}")
    if i >= 0 and j > i:
        try:
            return json.loads(s[i : j + 1])
        except Exception:
            return {}

    return {}


def _json_bytes(payload: Any) -> int:
    try:
        return len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    except Exception:
        return 0


def _env_int(name: str, default: int) -> int:
    try:
        v = int(str(os.getenv(name, "") or "").strip() or default)
        return v
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        v = float(str(os.getenv(name, "") or "").strip() or default)
        return v
    except Exception:
        return default


def _sleep_backoff(attempt: int, *, base: float = 1.0, cap: float = 20.0) -> None:
    # attempt: 0,1,2...
    d = min(cap, base * (2 ** attempt))
    d = d * (0.8 + random.random() * 0.4)  # jitter 0.8x - 1.2x
    time.sleep(d)


def _is_retryable_http(status: int) -> bool:
    return status in (408, 429, 500, 502, 503, 504)


def _split_csv(raw: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for part in str(raw or "").split(","):
        p = part.strip()
        if not p or p in seen:
            continue
        out.append(p)
        seen.add(p)
    return out


def _model_candidates(primary: str, fallback_csv: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for m in [primary] + _split_csv(fallback_csv):
        mm = (m or "").strip()
        if not mm or mm in seen:
            continue
        out.append(mm)
        seen.add(mm)
    return out


def _extract_http_codes(msg: str) -> set[int]:
    out: set[int] = set()
    for tok in re.findall(r"\b(4\d\d|5\d\d)\b", str(msg or "")):
        try:
            out.add(int(tok))
        except Exception:
            continue
    return out


def _is_fallback_eligible_error(err: Exception) -> bool:
    """
    Fallback only on rate-limit/quota/transient failures.
    Avoid fallback for auth/config/model errors.
    """
    s = str(err or "").lower()

    hard_no = (
        "invalid api key",
        "permission denied",
        "unauthorized",
        "forbidden",
        "not found",
        "unsupported model",
        "unknown model",
        "invalid argument",
    )
    if any(h in s for h in hard_no):
        return False

    hints = (
        "rate limit",
        "too many requests",
        "resource_exhausted",
        "quota",
        "exceeded your current quota",
        "temporarily unavailable",
        "overloaded",
        "deadline exceeded",
        "timed out",
        "timeout",
        "connectionerror",
        "connect timeout",
        "read timeout",
    )
    if any(h in s for h in hints):
        return True

    codes = _extract_http_codes(s)
    if any(c in (401, 403, 404, 422) for c in codes):
        return False
    if any(c in (408, 429, 500, 502, 503, 504) for c in codes):
        return True

    return False


def _post_with_retry(
    session: requests.Session,
    *,
    url: str,
    payload: dict,
    headers: Dict[str, str] | None = None,
    timeout_s: float,
    max_attempts: int,
) -> requests.Response:
    last_err: Exception | None = None

    for attempt in range(max_attempts):
        try:
            with tracing_context(metadata={"http": mk_http_meta(url=url, payload=payload, timeout_s=timeout_s)}):
                r = session.post(url, json=payload, headers=headers, timeout=timeout_s)
            if r.ok:
                return r

            if _is_retryable_http(int(r.status_code)) and attempt < max_attempts - 1:
                logger.warning("LLM HTTP retryable error: %s %s (attempt %d/%d)", r.status_code, url, attempt + 1, max_attempts)
                _sleep_backoff(attempt)
                continue

            # non-retryable or last attempt
            raise RuntimeError(f"LLM HTTP failed: {r.status_code} {r.text}")

        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            if attempt < max_attempts - 1:
                logger.warning("LLM network/timeout: %s (attempt %d/%d)", type(e).__name__, attempt + 1, max_attempts)
                _sleep_backoff(attempt)
                continue
            raise

        except Exception as e:
            last_err = e
            if attempt < max_attempts - 1:
                logger.warning("LLM exception: %s (attempt %d/%d)", type(e).__name__, attempt + 1, max_attempts)
                _sleep_backoff(attempt)
                continue
            raise

    if last_err:
        raise last_err
    raise RuntimeError("LLM call failed (unknown)")


class LLMTool:
    """
    Supports:
      - openai_compat (text only)
      - gemini (text + multimodal inlineData)
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._session = requests.Session()

    def _candidate_models(self, default_model: str) -> List[str]:
        primary = (self.settings.llm_model or "").strip() or default_model
        fallback_csv = (getattr(self.settings, "llm_fallback_models", "") or "").strip()
        return _model_candidates(primary, fallback_csv)

    def generate_text(self, prompt: str) -> str:
        provider = self.settings.llm_provider

        if provider == "openai_compat":
            base = os.getenv("LLM_BASE_URL", "https://api.openai.com").rstrip("/")
            url = f"{base}/v1/chat/completions"
            headers = {"Authorization": f"Bearer {self.settings.llm_api_key}"}
            timeout_s = _env_float("LLM_TIMEOUT_S", 120.0)
            max_attempts = _env_int("LLM_MAX_ATTEMPTS", 4)
            models = self._candidate_models(default_model="gpt-4o-mini")

            last_err: Exception | None = None
            for i, model in enumerate(models):
                payload = {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": "You are a helpful manufacturing quality assistant."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.2,
                }

                def _call() -> str:
                    pbytes = _json_bytes(payload)
                    logger.info("openai_compat.generate_text payload_bytes=%d model=%s", pbytes, model)

                    r = _post_with_retry(
                        self._session,
                        url=url,
                        payload=payload,
                        headers=headers,
                        timeout_s=timeout_s,
                        max_attempts=max_attempts,
                    )

                    data = r.json()
                    return data["choices"][0]["message"]["content"].strip()

                traced = traceable_wrap(_call, name="llm.openai_compat.generate_text", run_type="llm")
                try:
                    out = traced()
                    if i > 0:
                        logger.warning(
                            "LLM fallback success provider=openai_compat selected_model=%s primary_model=%s",
                            model,
                            models[0],
                        )
                    return out
                except Exception as e:
                    last_err = e
                    if i < len(models) - 1 and _is_fallback_eligible_error(e):
                        logger.warning(
                            "LLM fallback triggered provider=openai_compat from_model=%s to_model=%s err=%s",
                            model,
                            models[i + 1],
                            str(e)[:280],
                        )
                        continue
                    raise

            if last_err:
                raise last_err
            raise RuntimeError("openai_compat.generate_text failed with no error details")

        if provider == "gemini":
            base = os.getenv("LLM_BASE_URL", "https://generativelanguage.googleapis.com").rstrip("/")
            models = self._candidate_models(default_model="gemini-2.5-flash")
            key = self.settings.llm_api_key
            timeout_s = _env_float("LLM_TIMEOUT_S", 120.0)
            max_attempts = _env_int("LLM_MAX_ATTEMPTS", 4)

            last_err: Exception | None = None
            for i, model in enumerate(models):
                url = f"{base}/v1beta/models/{model}:generateContent?key={key}"
                payload = {
                    "systemInstruction": {"parts": [{"text": "You are a helpful manufacturing quality assistant."}]},
                    "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": 0.2},
                }

                def _call() -> str:
                    pbytes = _json_bytes(payload)
                    logger.info("gemini.generate_text payload_bytes=%d model=%s", pbytes, model)

                    r = _post_with_retry(
                        self._session,
                        url=url,
                        payload=payload,
                        timeout_s=timeout_s,
                        max_attempts=max_attempts,
                    )

                    data = r.json()
                    candidates = data.get("candidates", []) or []
                    if not candidates:
                        return ""
                    parts = candidates[0].get("content", {}).get("parts", []) or []
                    return "".join([p.get("text", "") for p in parts if isinstance(p, dict)]).strip()

                traced = traceable_wrap(_call, name="llm.gemini.generate_text", run_type="llm")
                try:
                    out = traced()
                    if i > 0:
                        logger.warning(
                            "LLM fallback success provider=gemini selected_model=%s primary_model=%s",
                            model,
                            models[0],
                        )
                    return out
                except Exception as e:
                    last_err = e
                    if i < len(models) - 1 and _is_fallback_eligible_error(e):
                        logger.warning(
                            "LLM fallback triggered provider=gemini from_model=%s to_model=%s err=%s",
                            model,
                            models[i + 1],
                            str(e)[:280],
                        )
                        continue
                    raise

            if last_err:
                raise last_err
            raise RuntimeError("gemini.generate_text failed with no error details")
        raise RuntimeError(f"Unsupported LLM_PROVIDER={provider}")

    def generate_json_with_images(
        self,
        *,
        prompt: str,
        images: List[Dict[str, Any]],
        temperature: float = 0.0,
    ) -> Dict[str, Any]:
        """
        Gemini multimodal call:
          images = [{ "image_index": 0, "mime_type": "image/jpeg", "image_bytes": b"..." }, ...]
        Returns parsed JSON dict ({} if parse fails).
        """
        if self.settings.llm_provider != "gemini":
            return _extract_json(self.generate_text(prompt))

        base = os.getenv("LLM_BASE_URL", "https://generativelanguage.googleapis.com").rstrip("/")
        models = self._candidate_models(default_model="gemini-2.5-flash")
        key = self.settings.llm_api_key
        timeout_s = _env_float("LLM_VISION_TIMEOUT_S", 180.0)
        max_attempts = _env_int("LLM_MAX_ATTEMPTS", 4)

        parts: List[Dict[str, Any]] = [{"text": prompt}]
        for img in images or []:
            b = img.get("image_bytes")
            if not isinstance(b, (bytes, bytearray)) or not b:
                continue
            mime = (img.get("mime_type") or "image/jpeg").strip()
            parts.append({"inlineData": {"mimeType": mime, "data": _b64(bytes(b))}})

        last_err: Exception | None = None
        for i, model in enumerate(models):
            url = f"{base}/v1beta/models/{model}:generateContent?key={key}"
            payload = {
                "systemInstruction": {"parts": [{"text": "You are a helpful manufacturing quality assistant."}]},
                "contents": [{"role": "user", "parts": parts}],
                "generationConfig": {"temperature": float(temperature)},
            }

            def _call() -> Dict[str, Any]:
                pbytes = _json_bytes(payload)
                logger.info(
                    "gemini.generate_json_with_images payload_bytes=%d model=%s images=%d",
                    pbytes,
                    model,
                    len(images or []),
                )

                r = _post_with_retry(
                    self._session,
                    url=url,
                    payload=payload,
                    timeout_s=timeout_s,
                    max_attempts=max_attempts,
                )

                data = r.json()
                candidates = data.get("candidates", []) or []
                if not candidates:
                    return {}

                out_parts = candidates[0].get("content", {}).get("parts", []) or []
                text = "".join([p.get("text", "") for p in out_parts if isinstance(p, dict)]).strip()
                return _extract_json(text)

            traced = traceable_wrap(_call, name="llm.gemini.generate_json_with_images", run_type="llm")
            try:
                out = traced()
                if i > 0:
                    logger.warning(
                        "LLM fallback success provider=gemini multimodal selected_model=%s primary_model=%s",
                        model,
                        models[0],
                    )
                return out
            except Exception as e:
                last_err = e
                if i < len(models) - 1 and _is_fallback_eligible_error(e):
                    logger.warning(
                        "LLM fallback triggered provider=gemini multimodal from_model=%s to_model=%s err=%s",
                        model,
                        models[i + 1],
                        str(e)[:280],
                    )
                    continue
                raise

        if last_err:
            raise last_err
        raise RuntimeError("gemini.generate_json_with_images failed with no error details")
