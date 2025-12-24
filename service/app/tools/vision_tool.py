from __future__ import annotations

from typing import Any, Dict, List, Optional
import base64
import json
import os
import requests


def _extract_json(text: str) -> Dict[str, Any]:
    """
    Best-effort: find first {...} block and parse.
    """
    s = (text or "").strip()
    if not s:
        return {}
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


class VisionTool:
    """
    Gemini-based caption + defect boxes.
    Returns: {caption: str, defects: [{label, confidence, box:{x1,y1,x2,y2}}]}
    """

    def __init__(self, *, api_key: str, model: str, base_url: Optional[str] = None):
        self.api_key = api_key
        self.model = model or "gemini-2.0-flash"
        self.base = (base_url or os.getenv("LLM_BASE_URL", "https://generativelanguage.googleapis.com")).rstrip("/")

    def analyze_defects(
        self,
        *,
        image_bytes: bytes,
        mime_type: str,
        context_hint: str,
    ) -> Dict[str, Any]:
        url = f"{self.base}/v1beta/models/{self.model}:generateContent?key={self.api_key}"

        prompt = f"""
You are a manufacturing quality inspection assistant.

Task:
1) Write a short, consistent caption for the image (manufacturing defect oriented).
2) If any visible defect exists, return bounding boxes.

Rules:
- Output MUST be valid JSON only.
- Coordinates must be normalized floats in [0,1] relative to image width/height.
- If unsure/no defect, return defects=[].

Context:
{context_hint}

JSON schema:
{{
  "caption": "string",
  "defects": [
    {{
      "label": "scratch|dent|weld_defect|porosity|crack|burr|chip|discoloration|hole|rust|misalignment|other",
      "confidence": 0.0,
      "box": {{"x1":0.0,"y1":0.0,"x2":0.0,"y2":0.0}}
    }}
  ]
}}
""".strip()

        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": prompt},
                        {
                            "inlineData": {
                                "mimeType": mime_type,
                                "data": base64.b64encode(image_bytes).decode("utf-8"),
                            }
                        },
                    ],
                }
            ],
            "generationConfig": {"temperature": 0.0},
        }

        r = requests.post(url, json=payload, timeout=120)
        if not r.ok:
            raise RuntimeError(f"Vision generateContent failed: {r.status_code} {r.text}")

        data = r.json()
        candidates = data.get("candidates", []) or []
        if not candidates:
            return {"caption": "", "defects": []}

        parts = candidates[0].get("content", {}).get("parts", []) or []
        text = "".join([p.get("text", "") for p in parts if isinstance(p, dict)]).strip()

        out = _extract_json(text)
        if not isinstance(out, dict):
            out = {}
        out.setdefault("caption", "")
        out.setdefault("defects", [])
        return out