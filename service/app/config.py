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

def _parse_json_env(raw: str) -> dict:
    raw = (raw or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}

def _deep_get(d: dict, path: list[str], default: str = "") -> str:
    cur: object = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    if cur is None:
        return default
    return str(cur).strip()

def _apply_glide_json_overrides(*, base: dict, fallback_env_get) -> dict:
    """
    Supports both JSON schemas:

    A) Old:
      {
        "api_key": "...",
        "app_id": "...",
        "base_url": "...",
        "company": { "table": "...", "columns": { "rowid": "$rowID", "name": "Name", "desc": "nszR1" } },
        "tables": { ... }
      }

    B) New (your current):
      {
        "api_key": "...",
        "app_id": "...",
        "base_url": "...",
        "tables": {
          "company": { "table": "...", "columns": { "row_id": "$rowID", "name": "Name", "description": "nszR1" } },
          "raw_material": { ... },
          ...
        }
      }
    """
    overrides: dict = {}

    def _deep_get_any(d: dict, paths: list[list[str]], default: str = "") -> str:
        for p in paths:
            v = _deep_get(d, p, default="")
            if str(v or "").strip():
                return str(v).strip()
        return default

    # top-level
    overrides["glide_api_key"] = _deep_get(base, ["api_key"], fallback_env_get("GLIDE_API_KEY", ""))
    overrides["glide_app_id"] = _deep_get(base, ["app_id"], fallback_env_get("GLIDE_APP_ID", ""))
    overrides["glide_base_url"] = _deep_get(base, ["base_url"], fallback_env_get("GLIDE_BASE_URL", "https://api.glideapp.io")).rstrip("/")

    # ---- Company: support both locations + key names ----
    overrides["glide_company_table"] = _deep_get_any(
        base,
        [["company", "table"], ["tables", "company", "table"]],
        fallback_env_get("GLIDE_COMPANY_TABLE", ""),
    )

    overrides["glide_company_rowid_column"] = _deep_get_any(
        base,
        [
            ["company", "columns", "rowid"],
            ["company", "columns", "row_id"],
            ["tables", "company", "columns", "rowid"],
            ["tables", "company", "columns", "row_id"],
        ],
        fallback_env_get("GLIDE_COMPANY_ROWID_COLUMN", "$rowID"),
    )

    overrides["glide_company_name_column"] = _deep_get_any(
        base,
        [
            ["company", "columns", "name"],
            ["tables", "company", "columns", "name"],
        ],
        fallback_env_get("GLIDE_COMPANY_NAME_COLUMN", "Name"),
    )

    overrides["glide_company_desc_column"] = _deep_get_any(
        base,
        [
            ["company", "columns", "desc"],
            ["company", "columns", "description"],
            ["tables", "company", "columns", "desc"],
            ["tables", "company", "columns", "description"],
        ],
        fallback_env_get("GLIDE_COMPANY_DESC_COLUMN", "Short client description"),
    )

    # tables helper
    def t_table(key: str, env_name: str) -> str:
        return _deep_get(base, ["tables", key, "table"], fallback_env_get(env_name, ""))

    def t_col(key: str, json_col: str, env_name: str, default: str) -> str:
        # accept both "rowid" and "row_id" style keys
        return _deep_get_any(
            base,
            [
                ["tables", key, "columns", json_col],
                ["tables", key, "columns", json_col.replace("rowid", "row_id")],
            ],
            fallback_env_get(env_name, default),
        )

    # raw_material
    overrides["glide_raw_material_table"] = t_table("raw_material", "GLIDE_RAW_MATERIAL_TABLE")
    overrides["glide_raw_material_tenant_column"] = t_col("raw_material", "tenant", "GLIDE_RAW_MATERIAL_TENANT_COLUMN", "Company Row ID")
    overrides["glide_raw_material_rowid_column"] = t_col("raw_material", "rowid", "GLIDE_RAW_MATERIAL_ROWID_COLUMN", "$rowID")
    overrides["glide_raw_material_project_name_column"] = t_col("raw_material", "project", "GLIDE_RAW_MATERIAL_PROJECT_COLUMN", "Project number")
    overrides["glide_raw_material_part_number_column"] = t_col("raw_material", "part_number", "GLIDE_RAW_MATERIAL_PART_NUMBER_COLUMN", "Part number")
    overrides["glide_raw_material_legacy_id_column"] = t_col("raw_material", "legacy_id", "GLIDE_RAW_MATERIAL_LEGACY_ID_COLUMN", "Legacy ID")
    overrides["glide_raw_material_project_row_id_column"] = t_col("raw_material", "project_row_id", "GLIDE_RAW_MATERIAL_PROJECT_ROW_ID_COLUMN", "Project Row ID")
    overrides["glide_raw_material_title_column"] = t_col("raw_material", "title", "GLIDE_RAW_MATERIAL_TITLE_COLUMN", "Part name")

    # processes
    overrides["glide_processes_table"] = t_table("processes", "GLIDE_PROCESSES_TABLE")
    overrides["glide_processes_tenant_column"] = t_col("processes", "tenant", "GLIDE_PROCESSES_TENANT_COLUMN", "Company Row ID")
    overrides["glide_processes_rowid_column"] = t_col("processes", "rowid", "GLIDE_PROCESSES_ROWID_COLUMN", "$rowID")
    overrides["glide_processes_project_name_column"] = t_col("processes", "project", "GLIDE_PROCESSES_PROJECT_COLUMN", "Project name")
    overrides["glide_processes_part_number_column"] = t_col("processes", "part_number", "GLIDE_PROCESSES_PART_NUMBER_COLUMN", "Part number")
    overrides["glide_processes_legacy_id_column"] = t_col("processes", "legacy_id", "GLIDE_PROCESSES_LEGACY_ID_COLUMN", "Legacy ID")
    overrides["glide_processes_project_row_id_column"] = t_col("processes", "project_row_id", "GLIDE_PROCESSES_PROJECT_ROW_ID_COLUMN", "Project Row ID")
    overrides["glide_processes_title_column"] = t_col("processes", "title", "GLIDE_PROCESSES_TITLE_COLUMN", "Process")

    # boughtouts
    overrides["glide_boughtouts_table"] = t_table("boughtouts", "GLIDE_BOUGHTOUTS_TABLE")
    overrides["glide_boughtouts_tenant_column"] = t_col("boughtouts", "tenant", "GLIDE_BOUGHTOUTS_TENANT_COLUMN", "Company Row ID")
    overrides["glide_boughtouts_rowid_column"] = t_col("boughtouts", "rowid", "GLIDE_BOUGHTOUTS_ROWID_COLUMN", "$rowID")
    overrides["glide_boughtouts_project_name_column"] = t_col("boughtouts", "project", "GLIDE_BOUGHTOUTS_PROJECT_COLUMN", "Project")
    overrides["glide_boughtouts_part_number_column"] = t_col("boughtouts", "part_number", "GLIDE_BOUGHTOUTS_PART_NUMBER_COLUMN", "Part Number")
    overrides["glide_boughtouts_legacy_id_column"] = t_col("boughtouts", "legacy_id", "GLIDE_BOUGHTOUTS_LEGACY_ID_COLUMN", "Legacy ID")
    overrides["glide_boughtouts_project_row_id_column"] = t_col("boughtouts", "project_row_id", "GLIDE_BOUGHTOUTS_PROJECT_ROW_ID_COLUMN", "Project Row ID")
    overrides["glide_boughtouts_title_column"] = t_col("boughtouts", "title", "GLIDE_BOUGHTOUTS_TITLE_COLUMN", "Name")

    # project table (optional)
    overrides["glide_project_table"] = _deep_get(base, ["tables", "project", "table"], fallback_env_get("GLIDE_PROJECT_TABLE", ""))

    return overrides
@dataclass(frozen=True)
class Settings:
    # Core
    database_url: str
    redis_url: str

    # Sheets
    spreadsheet_id: str
    google_service_account_json: str  # raw JSON string OR file path
    sheets_mapping_path: str
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

    # AppSheet (Cues)
    appsheet_base_url: str
    appsheet_app_id: str
    appsheet_access_key: str
    appsheet_cues_table: str

    # Optional column overrides (if your table uses different headers)
    appsheet_cues_col_cue: str
    appsheet_cues_col_cue_id: str
    appsheet_cues_col_id: str
    appsheet_cues_col_generated_at: str

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

    # AppSheet (Cues)
    appsheet_base_url = _get_env("APPSHEET_BASE_URL", "https://api.appsheet.com").rstrip("/")
    appsheet_app_id = _get_env("APPSHEET_APP_ID", "")
    appsheet_access_key = _get_env("APPSHEET_ACCESS_KEY", "")
    appsheet_cues_table = _get_env("APPSHEET_CUES_TABLE", "")

    # Column names (override only if your AppSheet columns differ)
    appsheet_cues_col_cue = _get_env("APPSHEET_CUES_COL_CUE", "Cue")
    appsheet_cues_col_cue_id = _get_env("APPSHEET_CUES_COL_CUE_ID", "Cue ID")
    appsheet_cues_col_id = _get_env("APPSHEET_CUES_COL_ID", "ID")

    # support BOTH names
    appsheet_cues_col_generated_at = _get_env(
        "APPSHEET_CUES_COL_GENERATED_AT",
        _get_env("APPSHEET_CUES_COL_DATE", "Date"),
    )

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

    # Glide (env OR single JSON)
    glide_json = _parse_json_env(_get_env("GLIDE_CONFIG_JSON", ""))

    ov = _apply_glide_json_overrides(base=glide_json, fallback_env_get=_get_env)

    glide_api_key = ov["glide_api_key"]
    glide_app_id = ov["glide_app_id"]
    glide_base_url = ov["glide_base_url"]

    glide_company_table = ov["glide_company_table"]
    glide_company_rowid_column = ov["glide_company_rowid_column"]
    glide_company_name_column = ov["glide_company_name_column"]
    glide_company_desc_column = ov["glide_company_desc_column"]

    glide_project_table = ov["glide_project_table"]
    glide_raw_material_table = ov["glide_raw_material_table"]
    glide_processes_table = ov["glide_processes_table"]
    glide_boughtouts_table = ov["glide_boughtouts_table"]

    glide_project_tenant_column = _get_env("GLIDE_PROJECT_TENANT_COLUMN", "Company Row ID")
    glide_project_rowid_column = _get_env("GLIDE_PROJECT_ROWID_COLUMN", "row ID")
    glide_project_name_column = _get_env("GLIDE_PROJECT_NAME_COLUMN", "Project")
    glide_project_part_number_column = _get_env("GLIDE_PROJECT_PART_NUMBER_COLUMN", "Part Number")
    glide_project_legacy_id_column = _get_env("GLIDE_PROJECT_LEGACY_ID_COLUMN", "Legacy ID")
    glide_project_title_column = _get_env("GLIDE_PROJECT_TITLE_COLUMN", "Project")

    glide_raw_material_tenant_column = ov["glide_raw_material_tenant_column"]
    glide_raw_material_rowid_column = ov["glide_raw_material_rowid_column"]
    glide_raw_material_project_name_column = ov["glide_raw_material_project_name_column"]
    glide_raw_material_part_number_column = ov["glide_raw_material_part_number_column"]
    glide_raw_material_legacy_id_column = ov["glide_raw_material_legacy_id_column"]
    glide_raw_material_project_row_id_column = ov["glide_raw_material_project_row_id_column"]
    glide_raw_material_title_column = ov["glide_raw_material_title_column"]

    glide_processes_tenant_column = ov["glide_processes_tenant_column"]
    glide_processes_rowid_column = ov["glide_processes_rowid_column"]
    glide_processes_project_name_column = ov["glide_processes_project_name_column"]
    glide_processes_part_number_column = ov["glide_processes_part_number_column"]
    glide_processes_legacy_id_column = ov["glide_processes_legacy_id_column"]
    glide_processes_project_row_id_column = ov["glide_processes_project_row_id_column"]
    glide_processes_title_column = ov["glide_processes_title_column"]

    glide_boughtouts_tenant_column = ov["glide_boughtouts_tenant_column"]
    glide_boughtouts_rowid_column = ov["glide_boughtouts_rowid_column"]
    glide_boughtouts_project_name_column = ov["glide_boughtouts_project_name_column"]
    glide_boughtouts_part_number_column = ov["glide_boughtouts_part_number_column"]
    glide_boughtouts_legacy_id_column = ov["glide_boughtouts_legacy_id_column"]
    glide_boughtouts_project_row_id_column = ov["glide_boughtouts_project_row_id_column"]
    glide_boughtouts_title_column = ov["glide_boughtouts_title_column"]
    sheets_mapping_path = _get_env("SHEETS_MAPPING_PATH", "packages/contracts/sheets_mapping.yaml")    
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
        appsheet_base_url=appsheet_base_url,
        appsheet_app_id=appsheet_app_id,
        appsheet_access_key=appsheet_access_key,
        appsheet_cues_table=appsheet_cues_table,
        appsheet_cues_col_cue=appsheet_cues_col_cue,
        appsheet_cues_col_cue_id=appsheet_cues_col_cue_id,
        appsheet_cues_col_id=appsheet_cues_col_id,
        appsheet_cues_col_generated_at=appsheet_cues_col_generated_at,
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
        sheets_mapping_path=sheets_mapping_path,
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
