# service/app/tools/vision_tool.py
from __future__ import annotations

from typing import Any, Dict, Optional, Union
import base64
import os
import requests
from pathlib import Path

from ..config import Settings
from .langsmith_trace import traceable_wrap


def _b64_image(image_bytes: bytes) -> str:
    return base64.b64encode(image_bytes or b"").decode("utf-8")


def _repo_root() -> Path:
    # vision_tool.py -> tools -> app -> service -> repo root
    return Path(__file__).resolve().parents[3]


def _read_prompt_file(rel_path: str) -> str:
    p = _repo_root() / rel_path
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        return ""


def _render_prompt(template: str, *, context_hint: str) -> str:
    t = (template or "").strip()
    if not t:
        return ""
    return t.replace("{context_hint}", (context_hint or "").strip())


class VisionTool:
    """
    Gemini-based vision helper with TWO modes:
      1) retrieval caption (exact 6 lines) for IMAGE_CAPTION storage + image retrieval
      2) OCR-document extraction (dense text) for scanned/table-like PDF pages

    IMPORTANT:
      - Defect detection + bounding boxes must still be done by the main multimodal prompt:
        packages/prompts/checkin_reply.md via LLMTool.generate_json_with_images()
    """

    def __init__(
        self,
        settings_or_api_key: Union[Settings, str, None] = None,
        *,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        if isinstance(settings_or_api_key, Settings):
            s = settings_or_api_key
            self.api_key = (getattr(s, "vision_api_key", "") or "").strip()
            self.model = (getattr(s, "vision_model", "") or "gemini-2.0-flash").strip()
            self.base = (
                (base_url or os.getenv("VISION_BASE_URL") or os.getenv("LLM_BASE_URL") or "https://generativelanguage.googleapis.com")
            ).rstrip("/")
            return

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

    # Backwards-compatible alias
    def caption_image(self, *, image_bytes: bytes, mime_type: str, context: str = "") -> str:
        return self.caption_for_retrieval(image_bytes=image_bytes, mime_type=mime_type, context_hint=context)

    def _call_gemini(self, *, prompt: str, image_bytes: bytes, mime_type: str, model: Optional[str]) -> str:
        url = self._url(model or self.model)
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
                raise RuntimeError(f"Vision call failed: {r.status_code} {r.text}")
            data = r.json()
            candidates = data.get("candidates", []) or []
            if not candidates:
                return ""
            parts = candidates[0].get("content", {}).get("parts", []) or []
            return "".join([p.get("text", "") for p in parts if isinstance(p, dict)]).strip()

        traced = traceable_wrap(_call, name="vision.generate_content", run_type="llm")
        return traced()

    def _is_ocr_mode(self, context_hint: str) -> bool:
        # We only switch to OCR mode when explicitly asked by caller (pdf_extractor).
        # This avoids corrupting normal image captioning + embeddings.
        return "OCR_MODE:1" in (context_hint or "")

    def caption_for_retrieval(
        self,
        *,
        image_bytes: bytes,
        mime_type: str,
        context_hint: str = "",
        model: Optional[str] = None,
    ) -> str:
        """
        Default: 6-line retrieval caption (stable for embedding).
        OCR mode: dense document extraction (for scanned/table-like PDF pages).
        """
        ctx = (context_hint or "").strip()

        if self._is_ocr_mode(ctx):
            tmpl = _read_prompt_file("packages/prompts/vision_ocr_document.md")
            if not tmpl:
                # fallback (should not happen once file exists)
                tmpl = (
                    "Extract key fields and tables from this document image. "
                    "Return headings: DOC_TYPE, ENTITY, IDENTIFIERS, DATES, MATERIAL/SPEC, TESTS/RESULTS, CONFORMITY/REMARKS, UNCERTAINTIES."
                )
            prompt = _render_prompt(tmpl, context_hint=ctx) or tmpl
            return self._call_gemini(prompt=prompt, image_bytes=image_bytes, mime_type=mime_type, model=model)

        # Normal caption mode (exactly 6 lines)
        tmpl = _read_prompt_file("packages/prompts/vision_caption_6line.md")
        if not tmpl:
            # fallback (should not happen once file exists)
            tmpl = (
                "Return EXACTLY 6 lines starting with: PART:, PROCESS:, DEFECT:, LOCATION:, MEASUREMENT:, EVIDENCE:. "
                "If unclear write 'unclear'."
            )

        prompt = _render_prompt(tmpl, context_hint=ctx) or tmpl
        out = self._call_gemini(prompt=prompt, image_bytes=image_bytes, mime_type=mime_type, model=model)

        # Hard safety: enforce 6 lines to keep embeddings stable
        lines = [ln.strip() for ln in (out or "").splitlines() if ln.strip()]
        if len(lines) == 6 and all((":" in ln) for ln in lines):
            return "\n".join(lines)

        # If model deviates, coerce to 6 labeled lines (best-effort without inventing)
        want = ["PART:", "PROCESS:", "DEFECT:", "LOCATION:", "MEASUREMENT:", "EVIDENCE:"]
        fixed = []
        have = {ln.split(":", 1)[0].strip().upper(): ln for ln in lines if ":" in ln}
        for w in want:
            key = w[:-1]
            cand = have.get(key)
            if cand:
                fixed.append(cand if cand.startswith(w) else f"{w} {cand.split(':',1)[1].strip()}")
            else:
                fixed.append(f"{w} unclear")
        return "\n".join(fixed)

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
            "Use packages/prompts/checkin_reply.md (multimodal) to generate defects_by_image."
        )