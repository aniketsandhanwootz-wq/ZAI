import json
import os
from dataclasses import dataclass
from typing import Optional


def _get_env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return val  # type: ignore


@dataclass(frozen=True)
class Settings:
    # Core
    database_url: str
    redis_url: str

    # Sheets
    spreadsheet_id: str
    google_service_account_json: str  # raw JSON string (recommended)

    # Webhook security
    appsheet_webhook_secret: str

    # LLM + embeddings
    llm_provider: str
    llm_api_key: str
    llm_model: str

    embedding_provider: str
    embedding_api_key: str
    embedding_model: str
    embedding_dims: int

    # Runtime toggles
    run_consumer: bool
    consumer_queues: str


def load_settings() -> Settings:
    # Note: for staging, you may keep LLM/Embedding empty and just test ingestion/writeback first.
    llm_provider = _get_env("LLM_PROVIDER", "openai_compat")
    llm_api_key = _get_env("LLM_API_KEY", "")
    llm_model = _get_env("LLM_MODEL", "gpt-4o-mini")

    embedding_provider = _get_env("EMBEDDING_PROVIDER", "openai_compat")
    embedding_api_key = _get_env("EMBEDDING_API_KEY", llm_api_key)
    embedding_model = _get_env("EMBEDDING_MODEL", "text-embedding-3-small")
    embedding_dims = int(_get_env("EMBEDDING_DIMS", "1536"))

    run_consumer = _get_env("RUN_CONSUMER", "1").lower() in ("1", "true", "yes")
    consumer_queues = _get_env("CONSUMER_QUEUES", "default")

    return Settings(
        database_url=_get_env("DATABASE_URL", required=True),
        redis_url=_get_env("REDIS_URL", required=True),
        spreadsheet_id=_get_env("GOOGLE_SHEET_ID", required=True),
        google_service_account_json=_get_env("GOOGLE_SERVICE_ACCOUNT_JSON", required=True),
        appsheet_webhook_secret=_get_env("APPSHEET_WEBHOOK_SECRET", required=True),
        llm_provider=llm_provider,
        llm_api_key=llm_api_key,
        llm_model=llm_model,
        embedding_provider=embedding_provider,
        embedding_api_key=embedding_api_key,
        embedding_model=embedding_model,
        embedding_dims=embedding_dims,
        run_consumer=run_consumer,
        consumer_queues=consumer_queues,
    )


def parse_service_account_info(raw: str) -> dict:
    """
    GOOGLE_SERVICE_ACCOUNT_JSON should be the full JSON key content as a string.
    """
    try:
        return json.loads(raw)
    except Exception as e:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON must be valid JSON string.") from e
