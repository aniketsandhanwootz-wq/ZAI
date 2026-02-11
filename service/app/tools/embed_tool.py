from typing import List
import os
import requests

from ..config import Settings
from .langsmith_trace import traceable_wrap

class EmbedTool:
    """
    Supports:
      - openai_compat (OpenAI-compatible /v1/embeddings)
      - gemini (Google Generative Language API embedContent)
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def _assert_dims(self, emb: List[float]) -> None:
        expected = int(getattr(self.settings, "embedding_dims", 0) or 0)
        if expected and len(emb) != expected:
            raise RuntimeError(
                f"Embedding dims mismatch: expected {expected}, got {len(emb)}. "
                f"Fix EMBEDDING_DIMS / model settings."
            )

    def embed_text(self, text: str) -> List[float]:
        """
        Default = document embeddings (store in DB).
        For query-time retrieval, use embed_query().
        """
        return self._embed_gemini_or_openai(text, task_type="RETRIEVAL_DOCUMENT")

    def embed_query(self, text: str) -> List[float]:
        """
        Query embedding (used for similarity search).
        """
        return self._embed_gemini_or_openai(text, task_type="RETRIEVAL_QUERY")

    def _embed_gemini_or_openai(self, text: str, task_type: str) -> List[float]:
        provider = self.settings.embedding_provider

        if provider == "openai_compat":
            base = os.getenv("EMBEDDING_BASE_URL", "https://api.openai.com").rstrip("/")
            url = f"{base}/v1/embeddings"
            headers = {"Authorization": f"Bearer {self.settings.embedding_api_key}"}
            payload = {"model": self.settings.embedding_model, "input": text}
            def _call():
                r = requests.post(url, json=payload, headers=headers, timeout=60)
                r.raise_for_status()
                data = r.json()
                return data["data"][0]["embedding"]

            traced = traceable_wrap(_call, name="embed.openai_compat", run_type="tool")
            emb = traced()
            self._assert_dims(emb)
            return emb

        if provider == "gemini":
            base = os.getenv("EMBEDDING_BASE_URL", "https://generativelanguage.googleapis.com").rstrip("/")
            model = self.settings.embedding_model or "gemini-embedding-001"
            key = self.settings.embedding_api_key
            url = f"{base}/v1beta/models/{model}:embedContent?key={key}"

            # ✅ outputDimensionality set to EMBEDDING_DIMS (1536)
            payload = {
                "content": {"parts": [{"text": text}]},
                "taskType": task_type,
                "outputDimensionality": int(self.settings.embedding_dims),
            }

            def _call():
                r = requests.post(url, json=payload, timeout=60)
                if not r.ok:
                    raise RuntimeError(f"Gemini embedContent failed: {r.status_code} {r.text}")
                data = r.json()
                return data["embedding"]["values"]

            traced = traceable_wrap(_call, name=f"embed.gemini.{task_type}", run_type="tool")
            emb = traced()
            self._assert_dims(emb)
            return emb

        raise RuntimeError(f"Unsupported EMBEDDING_PROVIDER={provider}")
