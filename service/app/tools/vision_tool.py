# service/app/tools/vision_tool.py
from __future__ import annotations

from typing import Any, Dict, Optional, List, Union
import base64
import json
import os
import requests

from ..config import Settings  # ✅ allow init from Settings


def _extract_json(text: str) -> Dict[str, Any]:
    """
    Best-effort: find first {...} block and parse JSON.
    """
    s = (text or "").strip()
    if not s:
        return {}

    # common: model wraps JSON in ```json ... ```
    if s.startswith("```"):
        s = s.strip()
        s = s.strip("`").strip()
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


def _clamp01(x: float) -> float:
    try:
        x = float(x)
    except Exception:
        x = 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


class VisionTool:
    """
    Gemini-based image caption tool (retrieval captions only).

    NOTE:
      - Defect detection + bounding boxes MUST be produced by the main multimodal LLM prompt:
        packages/prompts/checkin_reply.md via generate_ai_reply.py
      - This tool intentionally does NOT run defect detection.
    """

    def __init__(
        self,
        settings_or_api_key: Union[Settings, str, None] = None,
        *,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        # --- Init from Settings ---
        if isinstance(settings_or_api_key, Settings):
            s = settings_or_api_key
            self.api_key = (getattr(s, "vision_api_key", "") or "").strip()
            self.model = (getattr(s, "vision_model", "") or "gemini-2.0-flash").strip()
            self.base = (
                (base_url or os.getenv("VISION_BASE_URL") or os.getenv("LLM_BASE_URL") or "https://generativelanguage.googleapis.com")
            ).rstrip("/")
            return

        # --- Init from explicit args (backward-compatible with your current file) ---
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

    def _url(self, model: Optional[str] = None) -> str:
        m = (model or self.model or "gemini-2.0-flash").strip()
        return f"{self.base}/v1beta/models/{m}:generateContent?key={self.api_key}"

    # ✅ Compatibility alias: CCP ingest expects this name/signature
    def caption_image(self, *, image_bytes: bytes, mime_type: str, context: str = "") -> str:
        return self.caption_for_retrieval(
            image_bytes=image_bytes,
            mime_type=mime_type,
            context_hint=context,
        )

    def caption_for_retrieval(
        self,
        *,
        image_bytes: bytes,
        mime_type: str,
        context_hint: str = "",
        model: Optional[str] = None,
    ) -> str:
        """
        Returns EXACTLY 6 lines (plain text), stable for embedding.
        """
        url = self._url(model or os.getenv("VISION_CAPTION_MODEL") or self.model)
        mime = (mime_type or "image/jpeg").strip()

        prompt = (
            "ROLE: Manufacturing quality assistant.\n"
            "TASK: Create a RETRIEVAL CAPTION for this image.\n\n"
            "OUTPUT FORMAT (strict):\n"
            "Return EXACTLY 6 lines, each starting with the label:\n"
            "PART:\n"
            "PROCESS:\n"
            "DEFECT:\n"
            "LOCATION:\n"
            "MEASUREMENT:\n"
            "EVIDENCE:\n\n"
            "RULES:\n"
            "- Be factual. Do NOT guess or invent.\n"
            "- If unknown/unclear, write 'unclear'.\n"
            "- Keep each line <= 18 words.\n"
            "- Use manufacturing vocabulary when applicable.\n"
        )
        if (context_hint or "").strip():
            prompt += "\nCONTEXT (use only if relevant; do not copy blindly):\n" + context_hint.strip() + "\n"

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

        r = requests.post(url, json=payload, timeout=120)
        if not r.ok:
            raise RuntimeError(f"Vision caption failed: {r.status_code} {r.text}")

        data = r.json()
        candidates = data.get("candidates", []) or []
        if not candidates:
            return ""

        parts = candidates[0].get("content", {}).get("parts", []) or []
        text = "".join([p.get("text", "") for p in parts if isinstance(p, dict)]).strip()
        return text

    def detect_defects(
        self,
        *,
        image_bytes: bytes,
        mime_type: str,
        context_hint: str = "",
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Disabled by design.

        Defect detection + bounding boxes are generated ONLY in the main prompt:
        packages/prompts/checkin_reply.md (via LLMTool.generate_json_with_images()).
        """
        raise RuntimeError(
            "VisionTool.detect_defects() is disabled. "
            "Use checkin_reply.md (multimodal) to generate defects_by_image."
        )
