import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def _get_env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return str(val)  # intentional string coercion


@dataclass(frozen=True)
class Settings:
    # Core
    database_url: str
    redis_url: str

    # Sheets
    spreadsheet_id: str
    google_service_account_json: str  # raw JSON string OR file path
     # Drive
    google_drive_root_folder_id: str

    # Vision (image caption + boxes)
    vision_provider: str
    vision_api_key: str
    vision_model: str

    # Teams
    teams_webhook_url: str
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

    # Migrations toggle
    run_migrations: bool


def load_settings() -> Settings:
    # -----------------------
    # LLM (optional Phase-0/1)
    # -----------------------
    llm_provider = _get_env("LLM_PROVIDER", "openai_compat")
    llm_api_key = _get_env("LLM_API_KEY", "")
    llm_model = _get_env("LLM_MODEL", "gpt-4o-mini")

    # -----------------------
    # Embeddings (Gemini)
    # -----------------------
    # Gemini officially supports 1536-dim embeddings
    embedding_provider = _get_env("EMBEDDING_PROVIDER", "gemini")
    embedding_api_key = _get_env("EMBEDDING_API_KEY", llm_api_key)
    embedding_model = _get_env("EMBEDDING_MODEL", "models/embedding-001")
    embedding_dims = int(_get_env("EMBEDDING_DIMS", "1536"))

    # -----------------------
    # Runtime toggles
    # -----------------------
    run_consumer = _get_env("RUN_CONSUMER", "1").lower() in ("1", "true", "yes")
    consumer_queues = _get_env("CONSUMER_QUEUES", "default")
    run_migrations = _get_env("RUN_MIGRATIONS", "0").lower() in ("1", "true", "yes")

    # -----------------------
    # Drive + Vision + Teams
    # -----------------------
    google_drive_root_folder_id = _get_env("GOOGLE_DRIVE_ROOT_FOLDER_ID", "")

    vision_provider = _get_env("VISION_PROVIDER", "gemini")
    vision_api_key = _get_env("VISION_API_KEY", llm_api_key)
    vision_model = _get_env("VISION_MODEL", "gemini-2.0-flash")

    teams_webhook_url = _get_env("TEAMS_WEBHOOK_URL", "")
    # -----------------------
    # Sheets auth (FIXED)
    # -----------------------
    # IMPORTANT:
    # os.getenv avoids the "str(None) -> 'None'" bug
    sa_raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    sa_raw = (sa_raw or "").strip()

    # Treat literal "None" as empty
    if not sa_raw or sa_raw.lower() == "none":
        sa_raw = _get_env("GOOGLE_SERVICE_ACCOUNT_FILE", required=True)

    return Settings(
        database_url=_get_env("DATABASE_URL", required=True),
        redis_url=_get_env("REDIS_URL", required=True),
        spreadsheet_id=_get_env("GOOGLE_SHEET_ID", required=True),
        google_service_account_json=sa_raw,
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
        run_migrations=run_migrations,
        google_drive_root_folder_id=google_drive_root_folder_id,
        vision_provider=vision_provider,
        vision_api_key=vision_api_key,
        vision_model=vision_model,
        teams_webhook_url=teams_webhook_url,
    )


def parse_service_account_info(raw: str) -> dict:
    raw = (raw or "").strip()
    if not raw or raw.lower() == "none":
        raise RuntimeError(
            "Missing GOOGLE_SERVICE_ACCOUNT_JSON / GOOGLE_SERVICE_ACCOUNT_FILE"
        )

    # Case 1: raw JSON string (best for Render)
    if raw.startswith("{"):
        return json.loads(raw)

    # Case 2: file path
    p = Path(raw).expanduser()
    candidates = [p]

    if not p.is_absolute():
        candidates.append((Path.cwd() / p).resolve())

    service_dir = Path(__file__).resolve().parents[1]
    candidates.append((service_dir / p).resolve())

    for c in candidates:
        if c.exists() and c.is_file():
            return json.loads(c.read_text(encoding="utf-8"))

    raise RuntimeError(
        "Invalid service account input. "
        f"Got='{raw}'. Tried paths: {[str(x) for x in candidates]}"
    )
