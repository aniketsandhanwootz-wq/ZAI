from typing import List
import os
import requests
from ..config import Settings


class EmbedTool:
    """
    Supports:
      - openai_compat (existing)
      - gemini (Google Generative Language API embedContent)
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def embed_text(self, text: str) -> List[float]:
        provider = self.settings.embedding_provider

        if provider == "openai_compat":
            base = os.getenv("EMBEDDING_BASE_URL", "https://api.openai.com").rstrip("/")
            url = f"{base}/v1/embeddings"
            headers = {"Authorization": f"Bearer {self.settings.embedding_api_key}"}
            payload = {"model": self.settings.embedding_model, "input": text}
            r = requests.post(url, json=payload, headers=headers, timeout=60)
            r.raise_for_status()
            data = r.json()
            return data["data"][0]["embedding"]

        if provider == "gemini":
            base = os.getenv("EMBEDDING_BASE_URL", "https://generativelanguage.googleapis.com").rstrip("/")
            model = self.settings.embedding_model or "text-embedding-004"
            key = self.settings.embedding_api_key

            url = f"{base}/v1beta/models/{model}:embedContent?key={key}"
            payload = {
                "content": {
                    "parts": [{"text": text}]
                }
            }

            r = requests.post(url, json=payload, timeout=60)
            if not r.ok:
                raise RuntimeError(f"Gemini embedContent failed: {r.status_code} {r.text}")

            data = r.json()
            return data["embedding"]["values"]

        raise RuntimeError(f"Unsupported EMBEDDING_PROVIDER={provider}")
