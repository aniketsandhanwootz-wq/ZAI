import requests
from ..config import Settings


class LLMTool:
    """
    MVP: OpenAI-compatible chat endpoint.
    Later: add Gemini/OpenAI SDK/local LLM.
    """
    def __init__(self, settings: Settings):
        self.settings = settings

    def generate_text(self, prompt: str) -> str:
        provider = self.settings.llm_provider

        if provider == "openai_compat":
            base = (requests.utils.urlparse(
                self._env("LLM_BASE_URL", "https://api.openai.com")
            ).geturl()).rstrip("/")
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

        raise RuntimeError(f"Unsupported LLM_PROVIDER={provider}")

    def _env(self, name: str, default: str) -> str:
        import os
        return os.getenv(name, default)
