import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict


def _get_env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required env var: {name}")

    s = str(val or "")
    s = s.strip()

    # strip wrapping quotes from .env like KEY="value"
    if len(s) >= 2 and ((s[0] == s[-1]) and s[0] in ("'", '"')):
        s = s[1:-1].strip()

    return s


_DRIVE_ID_RX = re.compile(r"^[a-zA-Z0-9_-]{10,}$")


def _is_valid_drive_id(v: str) -> bool:
    s = (v or "").strip()
    if not s:
        return False
    if s.startswith("<<") and s.endswith(">>"):
        return False
    if "folderId" in s or "<" in s or ">" in s:
        return False
    return bool(_DRIVE_ID_RX.match(s))


def _parse_prefix_map(raw: str) -> Dict[str, str]:
    raw = (raw or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {}
        out: Dict[str, str] = {}
        for k, v in data.items():
            kk = str(k or "").strip().strip("/")
            vv = str(v or "").strip()
            if not kk or not vv:
                continue
            if not _is_valid_drive_id(vv):
                continue
            out[kk] = vv
        return out
    except Exception:
        return {}


@dataclass(frozen=True)
class Settings:
    # Core
    database_url: str
    redis_url: str

    # Sheets
    spreadsheet_id: str
    google_service_account_json: str  # raw JSON string OR file path

    # Optional: separate spreadsheet for additional photos
    additional_photos_spreadsheet_id: str
    additional_photos_tab_name: str

    # Drive
    google_drive_root_folder_id: str
    google_drive_annotated_folder_id: str
    drive_prefix_map: Dict[str, str]

    # Vision
    vision_provider: str
    vision_api_key: str
    vision_model: str

    # Teams
    teams_webhook_url: str

    # Power Automate (Teams routing flow webhook)
    power_automate_webhook_url: str

    # ✅ Single webhook secret for Apps Script
    webhook_secret: str

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

    # Glide
    glide_api_key: str
    glide_app_id: str
    glide_company_table: str
    glide_company_rowid_column: str
    glide_company_name_column: str
    glide_company_desc_column: str
    glide_base_url: str
    # Phase 2: additional Glide KB tables
    glide_project_table: str
    glide_raw_material_table: str
    glide_processes_table: str
    glide_boughtouts_table: str

    # Optional: allow overriding key column names per table
    glide_project_tenant_column: str
    glide_project_rowid_column: str
    glide_project_name_column: str
    glide_project_part_number_column: str
    glide_project_legacy_id_column: str
    glide_project_title_column: str

    glide_raw_material_tenant_column: str
    glide_raw_material_rowid_column: str
    glide_raw_material_project_name_column: str
    glide_raw_material_part_number_column: str
    glide_raw_material_legacy_id_column: str
    glide_raw_material_project_row_id_column: str
    glide_raw_material_title_column: str

    glide_processes_tenant_column: str
    glide_processes_rowid_column: str
    glide_processes_project_name_column: str
    glide_processes_part_number_column: str
    glide_processes_legacy_id_column: str
    glide_processes_project_row_id_column: str
    glide_processes_title_column: str

    glide_boughtouts_tenant_column: str
    glide_boughtouts_rowid_column: str
    glide_boughtouts_project_name_column: str
    glide_boughtouts_part_number_column: str
    glide_boughtouts_legacy_id_column: str
    glide_boughtouts_project_row_id_column: str
    glide_boughtouts_title_column: str

def load_settings() -> Settings:
    llm_provider = _get_env("LLM_PROVIDER", "openai_compat")
    llm_api_key = _get_env("LLM_API_KEY", "")
    llm_model = _get_env("LLM_MODEL", "gpt-4o-mini")

    embedding_provider = _get_env("EMBEDDING_PROVIDER", "gemini")
    embedding_api_key = _get_env("EMBEDDING_API_KEY", llm_api_key)
    embedding_model = _get_env("EMBEDDING_MODEL", "models/embedding-001")
    embedding_dims = int(_get_env("EMBEDDING_DIMS", "1536"))

    run_consumer = _get_env("RUN_CONSUMER", "1").lower() in ("1", "true", "yes")
    consumer_queues = _get_env("CONSUMER_QUEUES", "default")
    run_migrations = _get_env("RUN_MIGRATIONS", "0").lower() in ("1", "true", "yes")

    google_drive_root_folder_id = _get_env("GOOGLE_DRIVE_ROOT_FOLDER_ID", "")
    google_drive_annotated_folder_id = _get_env("GOOGLE_DRIVE_ANNOTATED_FOLDER_ID", "")

    vision_provider = _get_env("VISION_PROVIDER", "gemini")
    vision_api_key = _get_env("VISION_API_KEY", llm_api_key)
    vision_model = _get_env("VISION_MODEL", "gemini-2.0-flash")

    teams_webhook_url = _get_env("TEAMS_WEBHOOK_URL", "")

    power_automate_webhook_url = _get_env("POWER_AUTOMATE_WEBHOOK_URL", teams_webhook_url)

    # Sheets auth
    sa_raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    sa_raw = (sa_raw or "").strip()
    if not sa_raw or sa_raw.lower() == "none":
        sa_raw = _get_env("GOOGLE_SERVICE_ACCOUNT_FILE", required=True)

    # Additional photos sheet (can default to main)
    additional_photos_spreadsheet_id = _get_env(
        "GOOGLE_SHEET_ADDITIONAL_PHOTOS_ID",
        _get_env("GOOGLE_SHEET_ID", required=True),
    )
    additional_photos_tab_name = _get_env("ADDITIONAL_PHOTOS_TAB_NAME", "Checkin Additional photos")

    drive_prefix_map = _parse_prefix_map(_get_env("DRIVE_PREFIX_MAP_JSON", ""))

    # ✅ webhook secret: prefer WEBHOOK_SECRET; fallback to old APPSHEET_WEBHOOK_SECRET
    webhook_secret = _get_env("WEBHOOK_SECRET", _get_env("APPSHEET_WEBHOOK_SECRET", ""), required=True)

    # Glide
    glide_api_key = _get_env("GLIDE_API_KEY", "")
    glide_app_id = _get_env("GLIDE_APP_ID", "")
    glide_company_table = _get_env("GLIDE_COMPANY_TABLE", "")
    glide_company_rowid_column = _get_env("GLIDE_COMPANY_ROWID_COLUMN", "$rowID")
    glide_company_name_column = _get_env("GLIDE_COMPANY_NAME_COLUMN", "Name")
    glide_company_desc_column = _get_env("GLIDE_COMPANY_DESC_COLUMN", "Short client description")
    glide_base_url = _get_env("GLIDE_BASE_URL", "https://api.glideapp.io").rstrip("/")

    # Phase 2: KB tables
    glide_project_table = _get_env("GLIDE_PROJECT_TABLE", "")
    glide_raw_material_table = _get_env("GLIDE_RAW_MATERIAL_TABLE", "")
    glide_processes_table = _get_env("GLIDE_PROCESSES_TABLE", "")
    glide_boughtouts_table = _get_env("GLIDE_BOUGHTOUTS_TABLE", "")

    # Optional column overrides (safe defaults)
    glide_project_tenant_column = _get_env("GLIDE_PROJECT_TENANT_COLUMN", "Company Row ID")
    glide_project_rowid_column = _get_env("GLIDE_PROJECT_ROWID_COLUMN", "row ID")
    glide_project_name_column = _get_env("GLIDE_PROJECT_NAME_COLUMN", "Project")
    glide_project_part_number_column = _get_env("GLIDE_PROJECT_PART_NUMBER_COLUMN", "Part Number")
    glide_project_legacy_id_column = _get_env("GLIDE_PROJECT_LEGACY_ID_COLUMN", "Legacy ID")
    glide_project_title_column = _get_env("GLIDE_PROJECT_TITLE_COLUMN", "Project")

    glide_raw_material_tenant_column = _get_env("GLIDE_RAW_MATERIAL_TENANT_COLUMN", "Company Row ID")
    glide_raw_material_rowid_column = _get_env("GLIDE_RAW_MATERIAL_ROWID_COLUMN", "row ID")
    glide_raw_material_project_name_column = _get_env("GLIDE_RAW_MATERIAL_PROJECT_NAME_COLUMN", "Project")
    glide_raw_material_part_number_column = _get_env("GLIDE_RAW_MATERIAL_PART_NUMBER_COLUMN", "Part Number")
    glide_raw_material_legacy_id_column = _get_env("GLIDE_RAW_MATERIAL_LEGACY_ID_COLUMN", "Legacy ID")
    glide_raw_material_project_row_id_column = _get_env("GLIDE_RAW_MATERIAL_PROJECT_ROW_ID_COLUMN", "Project Row ID")
    glide_raw_material_title_column = _get_env("GLIDE_RAW_MATERIAL_TITLE_COLUMN", "Name")

    glide_processes_tenant_column = _get_env("GLIDE_PROCESSES_TENANT_COLUMN", "Company Row ID")
    glide_processes_rowid_column = _get_env("GLIDE_PROCESSES_ROWID_COLUMN", "row ID")
    glide_processes_project_name_column = _get_env("GLIDE_PROCESSES_PROJECT_NAME_COLUMN", "Project")
    glide_processes_part_number_column = _get_env("GLIDE_PROCESSES_PART_NUMBER_COLUMN", "Part Number")
    glide_processes_legacy_id_column = _get_env("GLIDE_PROCESSES_LEGACY_ID_COLUMN", "Legacy ID")
    glide_processes_project_row_id_column = _get_env("GLIDE_PROCESSES_PROJECT_ROW_ID_COLUMN", "Project Row ID")
    glide_processes_title_column = _get_env("GLIDE_PROCESSES_TITLE_COLUMN", "Process Name")

    glide_boughtouts_tenant_column = _get_env("GLIDE_BOUGHTOUTS_TENANT_COLUMN", "Company Row ID")
    glide_boughtouts_rowid_column = _get_env("GLIDE_BOUGHTOUTS_ROWID_COLUMN", "row ID")
    glide_boughtouts_project_name_column = _get_env("GLIDE_BOUGHTOUTS_PROJECT_NAME_COLUMN", "Project")
    glide_boughtouts_part_number_column = _get_env("GLIDE_BOUGHTOUTS_PART_NUMBER_COLUMN", "Part Number")
    glide_boughtouts_legacy_id_column = _get_env("GLIDE_BOUGHTOUTS_LEGACY_ID_COLUMN", "Legacy ID")
    glide_boughtouts_project_row_id_column = _get_env("GLIDE_BOUGHTOUTS_PROJECT_ROW_ID_COLUMN", "Project Row ID")
    glide_boughtouts_title_column = _get_env("GLIDE_BOUGHTOUTS_TITLE_COLUMN", "Name")

    return Settings(
        database_url=_get_env("DATABASE_URL", required=True),
        redis_url=_get_env("REDIS_URL", required=True),
        spreadsheet_id=_get_env("GOOGLE_SHEET_ID", required=True),
        google_service_account_json=sa_raw,
        additional_photos_spreadsheet_id=additional_photos_spreadsheet_id,
        additional_photos_tab_name=additional_photos_tab_name,
        google_drive_root_folder_id=google_drive_root_folder_id,
        google_drive_annotated_folder_id=google_drive_annotated_folder_id,
        drive_prefix_map=drive_prefix_map,
        vision_provider=vision_provider,
        vision_api_key=vision_api_key,
        vision_model=vision_model,
        teams_webhook_url=teams_webhook_url,
        power_automate_webhook_url=power_automate_webhook_url,
        webhook_secret=webhook_secret,
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
        glide_api_key=glide_api_key,
        glide_app_id=glide_app_id,
        glide_company_table=glide_company_table,
        glide_company_rowid_column=glide_company_rowid_column,
        glide_company_name_column=glide_company_name_column,
        glide_company_desc_column=glide_company_desc_column,
        glide_base_url=glide_base_url,
        glide_project_table=glide_project_table,
        glide_raw_material_table=glide_raw_material_table,
        glide_processes_table=glide_processes_table,
        glide_boughtouts_table=glide_boughtouts_table,

        glide_project_tenant_column=glide_project_tenant_column,
        glide_project_rowid_column=glide_project_rowid_column,
        glide_project_name_column=glide_project_name_column,
        glide_project_part_number_column=glide_project_part_number_column,
        glide_project_legacy_id_column=glide_project_legacy_id_column,
        glide_project_title_column=glide_project_title_column,

        glide_raw_material_tenant_column=glide_raw_material_tenant_column,
        glide_raw_material_rowid_column=glide_raw_material_rowid_column,
        glide_raw_material_project_name_column=glide_raw_material_project_name_column,
        glide_raw_material_part_number_column=glide_raw_material_part_number_column,
        glide_raw_material_legacy_id_column=glide_raw_material_legacy_id_column,
        glide_raw_material_project_row_id_column=glide_raw_material_project_row_id_column,
        glide_raw_material_title_column=glide_raw_material_title_column,

        glide_processes_tenant_column=glide_processes_tenant_column,
        glide_processes_rowid_column=glide_processes_rowid_column,
        glide_processes_project_name_column=glide_processes_project_name_column,
        glide_processes_part_number_column=glide_processes_part_number_column,
        glide_processes_legacy_id_column=glide_processes_legacy_id_column,
        glide_processes_project_row_id_column=glide_processes_project_row_id_column,
        glide_processes_title_column=glide_processes_title_column,

        glide_boughtouts_tenant_column=glide_boughtouts_tenant_column,
        glide_boughtouts_rowid_column=glide_boughtouts_rowid_column,
        glide_boughtouts_project_name_column=glide_boughtouts_project_name_column,
        glide_boughtouts_part_number_column=glide_boughtouts_part_number_column,
        glide_boughtouts_legacy_id_column=glide_boughtouts_legacy_id_column,
        glide_boughtouts_project_row_id_column=glide_boughtouts_project_row_id_column,
        glide_boughtouts_title_column=glide_boughtouts_title_column,
    )


def parse_service_account_info(raw: str) -> dict:
    raw = (raw or "").strip()
    if not raw or raw.lower() == "none":
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON / GOOGLE_SERVICE_ACCOUNT_FILE")

    if raw.startswith("{"):
        return json.loads(raw)

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
