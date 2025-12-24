# service/app/tools/vision_tool.py
from __future__ import annotations

from typing import Any, Dict, Optional, List
import base64
import json
import os
import requests


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
    Gemini-based image caption + defect detection (boxes).

    IMPORTANT: We separate them into two calls:
      - caption_for_retrieval(): returns 6-line caption only (plain text)
      - detect_defects(): returns JSON { "defects": [...] } only
    """

    def __init__(self, *, api_key: str, model: str, base_url: Optional[str] = None):
        self.api_key = api_key
        self.model = model or "gemini-2.0-flash"
        self.base = (
            base_url
            or os.getenv("VISION_BASE_URL")
            or os.getenv("LLM_BASE_URL")
            or "https://generativelanguage.googleapis.com"
        ).rstrip("/")

    def _url(self, model: Optional[str] = None) -> str:
        m = (model or self.model or "gemini-2.0-flash").strip()
        return f"{self.base}/v1beta/models/{m}:generateContent?key={self.api_key}"

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
        Returns strict JSON dict:
          { "defects": [ {label, confidence, box:{x1,y1,x2,y2}} ] }
        """
        url = self._url(model or os.getenv("VISION_DETECT_MODEL") or self.model)
        mime = (mime_type or "image/jpeg").strip()

        prompt = (
            "You are a manufacturing quality inspection assistant.\n\n"
            "TASK:\n"
            "- Detect CLEARLY VISIBLE manufacturing defects.\n"
            "- Return bounding boxes (normalized 0..1).\n\n"
            "HARD RULES:\n"
            "- Output MUST be VALID JSON ONLY. No markdown. No extra text.\n"
            "- If you are unsure OR no defect is clearly visible, return: {\"defects\": []}\n"
            "- Boxes must be normalized floats in [0,1].\n\n"
            "Allowed defect labels (pick best match):\n"
            "scratch, dent, crack, burr, chip, rust, discoloration, contamination, "
            "weld_porosity, weld_lack_of_fusion, weld_crater, weld_spatter, misalignment, other\n\n"
            "JSON schema:\n"
            "{\n"
            '  "defects": [\n'
            "    {\n"
            '      "label": "scratch|dent|crack|burr|chip|rust|discoloration|contamination|weld_porosity|weld_lack_of_fusion|weld_crater|weld_spatter|misalignment|other",\n'
            '      "confidence": 0.0,\n'
            '      "box": {"x1": 0.0, "y1": 0.0, "x2": 0.0, "y2": 0.0}\n'
            "    }\n"
            "  ]\n"
            "}\n"
        )
        if (context_hint or "").strip():
            prompt += "\nContext hint (use only if relevant; do not copy blindly):\n" + context_hint.strip() + "\n"

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
            raise RuntimeError(f"Vision defect detect failed: {r.status_code} {r.text}")

        data = r.json()
        candidates = data.get("candidates", []) or []
        if not candidates:
            return {"defects": []}

        parts = candidates[0].get("content", {}).get("parts", []) or []
        text = "".join([p.get("text", "") for p in parts if isinstance(p, dict)]).strip()

        out = _extract_json(text)
        if not isinstance(out, dict):
            out = {}

        defects = out.get("defects") or []
        if not isinstance(defects, list):
            defects = []

        cleaned: List[Dict[str, Any]] = []
        for d in defects:
            if not isinstance(d, dict):
                continue
            box = d.get("box") or {}
            if not isinstance(box, dict):
                box = {}

            x1 = _clamp01(box.get("x1", 0.0))
            y1 = _clamp01(box.get("y1", 0.0))
            x2 = _clamp01(box.get("x2", 0.0))
            y2 = _clamp01(box.get("y2", 0.0))

            # ensure ordering
            if x2 < x1:
                x1, x2 = x2, x1
            if y2 < y1:
                y1, y2 = y2, y1

            cleaned.append(
                {
                    "label": str(d.get("label") or "other"),
                    "confidence": float(d.get("confidence") or 0.0),
                    "box": {"x1": x1, "y1": y1, "x2": x2, "y2": y2},
                }
            )

        return {"defects": cleaned}