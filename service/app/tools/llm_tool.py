# service/app/tools/llm_tool.py
from __future__ import annotations

import base64
import json
import os
from typing import Any, Dict, List
from .langsmith_trace import traceable_wrap, mk_http_meta
import requests

from ..config import Settings


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


class LLMTool:
    """
    Supports:
      - openai_compat (text only)
      - gemini (text + multimodal inlineData)
    """

    def __init__(self, settings: Settings):
        self.settings = settings

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
                r = requests.post(url, json=payload, timeout=120)
                if not r.ok:
                    raise RuntimeError(f"Gemini generateContent failed: {r.status_code} {r.text}")

                data = r.json()
                candidates = data.get("candidates", []) or []
                if not candidates:
                    return ""
                parts = candidates[0].get("content", {}).get("parts", []) or []
                return "".join([p.get("text", "") for p in parts if isinstance(p, dict)]).strip()

            traced = traceable_wrap(_call, name="llm.gemini.generate_text", run_type="llm")
            return traced()

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
            r = requests.post(url, json=payload, timeout=180)
            if not r.ok:
                raise RuntimeError(f"Gemini generateContent failed: {r.status_code} {r.text}")

            data = r.json()
            candidates = data.get("candidates", []) or []
            if not candidates:
                return {}

            out_parts = candidates[0].get("content", {}).get("parts", []) or []
            text = "".join([p.get("text", "") for p in out_parts if isinstance(p, dict)]).strip()
            return _extract_json(text)

        traced = traceable_wrap(_call, name="llm.gemini.generate_json_with_images", run_type="llm")
        return traced()
