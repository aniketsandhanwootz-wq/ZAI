# service/app/tools/llm_tool.py
import os
import requests
from ..config import Settings


class LLMTool:
    """
    Supports:
      - openai_compat (existing)
      - gemini (Google Generative Language API)
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
            r = requests.post(url, json=payload, headers=headers, timeout=120)
            r.raise_for_status()
            data = r.json()
            return data["choices"][0]["message"]["content"].strip()

        if provider == "gemini":
            base = os.getenv("LLM_BASE_URL", "https://generativelanguage.googleapis.com").rstrip("/")
            model = self.settings.llm_model or "gemini-2.5-flash"
            key = self.settings.llm_api_key

            url = f"{base}/v1beta/models/{model}:generateContent?key={key}"
            payload = {
                "systemInstruction": {
                    "parts": [{"text": "You are a helpful manufacturing quality assistant."}]
                },
                "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2},
            }

            r = requests.post(url, json=payload, timeout=120)
            if not r.ok:
                raise RuntimeError(f"Gemini generateContent failed: {r.status_code} {r.text}")

            data = r.json()
            candidates = data.get("candidates", [])
            if not candidates:
                return ""
            parts = candidates[0].get("content", {}).get("parts", [])
            return "".join([p.get("text", "") for p in parts]).strip()

        raise RuntimeError(f"Unsupported LLM_PROVIDER={provider}")