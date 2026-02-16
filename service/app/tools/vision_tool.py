from __future__ import annotations

from typing import Any, Dict, Optional, Union
import base64
import json
import os
from pathlib import Path
import requests

from ..config import Settings  # allow init from Settings
from .langsmith_trace import traceable_wrap


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
            pass

    i = s.find("{")
    j = s.rfind("}")
    if i >= 0 and j > i:
        chunk = s[i : j + 1]
        try:
            return json.loads(chunk)
        except Exception:
            return {}

    return {}


def _b64_image(image_bytes: bytes) -> str:
    return base64.b64encode(image_bytes).decode("utf-8")


def _repo_root() -> Path:
    # service/app/tools -> parents[3] = repo root
    return Path(__file__).resolve().parents[3]


def _load_prompt_template(name: str) -> str:
    """
    Loads prompt from packages/prompts.
    No prompt text is embedded in code by design.
    """
    p = _repo_root() / "packages" / "prompts" / name
    return p.read_text(encoding="utf-8")


def _render_template_safe(template: str, vars: Dict[str, str]) -> str:
    out = template
    for k, v in (vars or {}).items():
        out = out.replace("{" + k + "}", v or "")
    return out


class VisionTool:
    """
    Gemini-based image caption tool (retrieval captions only).
    Caption is used for vector DB; prompt lives in packages/prompts.

    Defect detection + bboxes are produced only by checkin_reply.md pipeline.
    """

    def __init__(
        self,
        settings_or_api_key: Union[Settings, str, None] = None,
        *,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
        prompt_file: Optional[str] = None,
    ):
        # --- Init from Settings ---
        if isinstance(settings_or_api_key, Settings):
            s = settings_or_api_key
            self.api_key = (getattr(s, "vision_api_key", "") or "").strip()
            self.model = (getattr(s, "vision_model", "") or "gemini-2.0-flash").strip()
            self.base = (
                (base_url or os.getenv("VISION_BASE_URL") or os.getenv("LLM_BASE_URL") or "https://generativelanguage.googleapis.com")
            ).rstrip("/")
            self.prompt_file = (prompt_file or os.getenv("VISION_CAPTION_PROMPT_FILE") or "vision_caption_6line.md").strip()
            return

        # --- Init from explicit args (backward-compatible) ---
        if isinstance(settings_or_api_key, str) and settings_or_api_key.strip() and not api_key:
            api_key = settings_or_api_key.strip()

        self.api_key = (api_key or "").strip()
        self.model = (model or "gemini-2.0-flash").strip()
        self.base = (
            base_url
            or os.getenv("VISION_BASE_URL")
            or os.getenv("LLM_BASE_URL")
            or "https://generativelanguage.googleapis.com"
        ).rstrip("/")
        self.prompt_file = (prompt_file or os.getenv("VISION_CAPTION_PROMPT_FILE") or "vision_caption_6line.md").strip()

    def _url(self, model: Optional[str] = None) -> str:
        m = (model or self.model or "gemini-2.0-flash").strip()
        return f"{self.base}/v1beta/models/{m}:generateContent?key={self.api_key}"

    # Compatibility alias: CCP ingest expects this name/signature
    def caption_image(self, *, image_bytes: bytes, mime_type: str, context: str = "") -> str:
        return self.caption_for_retrieval(image_bytes=image_bytes, mime_type=mime_type, context_hint=context)

    def caption_for_retrieval(
        self,
        *,
        image_bytes: bytes,
        mime_type: str,
        context_hint: str = "",
        model: Optional[str] = None,
    ) -> str:
        """
        Returns text caption (expected to be 6-line format).
        Prompt comes ONLY from packages/prompts/<prompt_file>.
        No in-code fallback prompt.
        """
        # If API key missing, fail safe without prompting.
        # (Callers should handle empty caption.)
        if not (self.api_key or "").strip():
            return ""

        prompt_t = _load_prompt_template(self.prompt_file)
        prompt = _render_template_safe(prompt_t, {"context_hint": (context_hint or "").strip()})

        url = self._url(model or os.getenv("VISION_CAPTION_MODEL") or self.model)
        mime = (mime_type or "image/jpeg").strip()

        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": prompt},
                        {"inlineData": {"mimeType": mime, "data": _b64_image(image_bytes)}},
                    ],
                }
            ],
            "generationConfig": {"temperature": 0.0},
        }

        def _call() -> str:
            r = requests.post(url, json=payload, timeout=120)
            if not r.ok:
                raise RuntimeError(f"Vision caption failed: {r.status_code} {r.text}")

            data = r.json()
            candidates = data.get("candidates", []) or []
            if not candidates:
                return ""

            parts = candidates[0].get("content", {}).get("parts", []) or []
            return "".join([p.get("text", "") for p in parts if isinstance(p, dict)]).strip()

        traced = traceable_wrap(_call, name="vision.caption_for_retrieval", run_type="llm")
        return traced()

    def detect_defects(
        self,
        *,
        image_bytes: bytes,
        mime_type: str,
        context_hint: str = "",
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        raise RuntimeError(
            "VisionTool.detect_defects() is disabled. "
            "Use checkin_reply.md (multimodal) to generate defects_by_image."
        )