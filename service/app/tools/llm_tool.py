# service/app/tools/llm_tool.py
from __future__ import annotations

import base64
import json
import os
from typing import Any, Dict, List
from .langsmith_trace import traceable_wrap, mk_http_meta
import requests
import base64
import json
import os
import logging
import random
import time
from typing import Any, Dict, List
from .langsmith_trace import traceable_wrap, mk_http_meta, tracing_context
import requests
from ..config import Settings

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


def _post_with_retry(
    session: requests.Session,
    *,
    url: str,
    payload: dict,
    timeout_s: float,
    max_attempts: int,
) -> requests.Response:
    last_err: Exception | None = None

    for attempt in range(max_attempts):
        try:
            with tracing_context(metadata={"http": mk_http_meta(url=url, payload=payload, timeout_s=timeout_s)}):
                r = session.post(url, json=payload, timeout=timeout_s)
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

    def generate_text(self, prompt: str) -> str:
        provider = self.settings.llm_provider

        if provider == "openai_compat":
            base = os.getenv("LLM_BASE_URL", "https://api.openai.com").rstrip("/")
            url = f"{base}/v1/chat/completions"
            headers = {"Authorization": f"Bearer {self.settings.llm_api_key}"}
            payload = {
                "model": self.settings.llm_model,
                "messages": [
                    {"role": "system", "content": "You are a helpful manufacturing quality assistant."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
            }
            def _call() -> str:
                r = requests.post(url, json=payload, headers=headers, timeout=120)
                r.raise_for_status()
                data = r.json()
                return data["choices"][0]["message"]["content"].strip()

            traced = traceable_wrap(_call, name="llm.openai_compat.generate_text", run_type="llm")
            return traced()

        if provider == "gemini":
            base = os.getenv("LLM_BASE_URL", "https://generativelanguage.googleapis.com").rstrip("/")
            model = self.settings.llm_model or "gemini-2.5-flash"
            key = self.settings.llm_api_key
            url = f"{base}/v1beta/models/{model}:generateContent?key={key}"

            payload = {
                "systemInstruction": {"parts": [{"text": "You are a helpful manufacturing quality assistant."}]},
                "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2},
            }

            def _call() -> str:
                timeout_s = _env_float("LLM_TIMEOUT_S", 120.0)
                max_attempts = _env_int("LLM_MAX_ATTEMPTS", 4)

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
        model = self.settings.llm_model or "gemini-2.5-flash"
        key = self.settings.llm_api_key
        url = f"{base}/v1beta/models/{model}:generateContent?key={key}"

        parts: List[Dict[str, Any]] = [{"text": prompt}]
        for img in images or []:
            b = img.get("image_bytes")
            if not isinstance(b, (bytes, bytearray)) or not b:
                continue
            mime = (img.get("mime_type") or "image/jpeg").strip()
            parts.append({"inlineData": {"mimeType": mime, "data": _b64(bytes(b))}})

        payload = {
            "systemInstruction": {"parts": [{"text": "You are a helpful manufacturing quality assistant."}]},
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": {"temperature": float(temperature)},
        }

        def _call() -> Dict[str, Any]:
            timeout_s = _env_float("LLM_VISION_TIMEOUT_S", 180.0)
            max_attempts = _env_int("LLM_MAX_ATTEMPTS", 4)

            pbytes = _json_bytes(payload)
            logger.info("gemini.generate_json_with_images payload_bytes=%d model=%s images=%d", pbytes, model, len(images or []))

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
        return traced()
