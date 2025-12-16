from typing import List
import requests

from ..config import Settings


class EmbedTool:
    """
    MVP: use an OpenAI-compatible embeddings endpoint (works for many providers),
    or later swap to local embeddings.
    """
    def __init__(self, settings: Settings):
        self.settings = settings

    def embed_text(self, text: str) -> List[float]:
        provider = self.settings.embedding_provider

        if provider == "openai_compat":
            # expects OPENAI-compatible endpoint in EMBEDDING_BASE_URL (optional)
            base = (requests.utils.urlparse(
                self._env("EMBEDDING_BASE_URL", "https://api.openai.com")
            ).geturl()).rstrip("/")
            url = f"{base}/v1/embeddings"
            headers = {"Authorization": f"Bearer {self.settings.embedding_api_key}"}
            payload = {"model": self.settings.embedding_model, "input": text}

            r = requests.post(url, json=payload, headers=headers, timeout=60)
            r.raise_for_status()
            data = r.json()
            return data["data"][0]["embedding"]

        raise RuntimeError(f"Unsupported EMBEDDING_PROVIDER={provider}")

    def _env(self, name: str, default: str) -> str:
        import os
        return os.getenv(name, default)
