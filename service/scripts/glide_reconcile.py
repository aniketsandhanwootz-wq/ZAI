# service/scripts/glide_reconcile.py
"""
Manual Glide reconciliation runner (minimum Glide calls).

Goal:
- One list_table_rows(table) per table (pagination inside GlideClient).
- NO per-row fetch (get_row_by_row_id) during full scans.
- Hard idempotency: row_hash skip.
- Vector correctness on edits: delete stale vectors NOT IN new chunk hashes.

Run:
  PYTHONPATH=. python3 service/scripts/glide_reconcile.py \
    --tables company,raw_material,processes,boughtouts
"""

from __future__ import annotations

import argparse
import sys
import os
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

# Ensure imports work when running as a script
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from service.app.config import load_settings
from service.app.integrations.glide_client import GlideClient

from service.app.pipeline.ingest.glide_ingest_company import upsert_glide_company_row_dict
from service.app.pipeline.ingest.glide_ingest_raw_material import raw_material_spec
from service.app.pipeline.ingest.glide_ingest_processes import processes_spec
from service.app.pipeline.ingest.glide_ingest_boughtouts import boughtouts_spec

from service.app.pipeline.ingest.glide_ingest_base import (
    build_company_index,
    build_sheet_project_indexes,
    ingest_rows,
)

from service.app.pipeline.ingest.run_log import RunLog
from service.app.logctx import bind_run_id


# ----------------------------
# .env loading (same as before)
# ----------------------------

def _load_env() -> None:
    load_dotenv(override=True)  # CWD
    load_dotenv(dotenv_path=str(REPO_ROOT / ".env"), override=True)
    load_dotenv(dotenv_path=str(REPO_ROOT / "service" / ".env"), override=True)


def _load_glide_config_json() -> Dict[str, Any] | None:
    raw = (os.getenv("GLIDE_CONFIG_JSON") or "").strip()
    if not raw:
        return None
    if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
        raw = raw[1:-1].strip()
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _ensure_glide_env_from_config_json() -> None:
    """
    Keep this so script works even if GLIDE_* vars aren't exported,
    but GLIDE_CONFIG_JSON is present.
    """
    cfg = _load_glide_config_json()
    if not cfg:
        return

    def _s(x: Any) -> str:
        return str(x or "").strip()

    if not (os.getenv("GLIDE_API_KEY") or "").strip():
        os.environ["GLIDE_API_KEY"] = _s(cfg.get("api_key"))
    if not (os.getenv("GLIDE_APP_ID") or "").strip():
        os.environ["GLIDE_APP_ID"] = _s(cfg.get("app_id"))
    if not (os.getenv("GLIDE_BASE_URL") or "").strip():
        os.environ["GLIDE_BASE_URL"] = _s(cfg.get("base_url") or "https://api.glideapp.io")

    tables = cfg.get("tables") if isinstance(cfg.get("tables"), dict) else {}

    def _cols(key: str) -> Dict[str, Any]:
        t = tables.get(key) if isinstance(tables, dict) else None
        if not isinstance(t, dict):
            return {}
        c = t.get("columns") or {}
        return c if isinstance(c, dict) else {}

    def _table_id(key: str) -> str:
        t = tables.get(key) if isinstance(tables, dict) else None
        if not isinstance(t, dict):
            return ""
        return _s(t.get("table"))

    # Company
    if _table_id("company") and not (os.getenv("GLIDE_COMPANY_TABLE") or "").strip():
        os.environ["GLIDE_COMPANY_TABLE"] = _table_id("company")
    if not (os.getenv("GLIDE_COMPANY_ROWID_COLUMN") or "").strip():
        os.environ["GLIDE_COMPANY_ROWID_COLUMN"] = _s(_cols("company").get("row_id") or "$rowID")
    if _cols("company").get("name") and not (os.getenv("GLIDE_COMPANY_NAME_COLUMN") or "").strip():
        os.environ["GLIDE_COMPANY_NAME_COLUMN"] = _s(_cols("company").get("name"))
    if _cols("company").get("description") and not (os.getenv("GLIDE_COMPANY_DESC_COLUMN") or "").strip():
        os.environ["GLIDE_COMPANY_DESC_COLUMN"] = _s(_cols("company").get("description"))

    # Raw material
    if _table_id("raw_material") and not (os.getenv("GLIDE_RAW_MATERIAL_TABLE") or "").strip():
        os.environ["GLIDE_RAW_MATERIAL_TABLE"] = _table_id("raw_material")
    if not (os.getenv("GLIDE_RAW_MATERIAL_ROWID_COLUMN") or "").strip():
        os.environ["GLIDE_RAW_MATERIAL_ROWID_COLUMN"] = _s(_cols("raw_material").get("row_id") or "$rowID")

    # Processes
    if _table_id("processes") and not (os.getenv("GLIDE_PROCESSES_TABLE") or "").strip():
        os.environ["GLIDE_PROCESSES_TABLE"] = _table_id("processes")
    if not (os.getenv("GLIDE_PROCESSES_ROWID_COLUMN") or "").strip():
        os.environ["GLIDE_PROCESSES_ROWID_COLUMN"] = _s(_cols("processes").get("row_id") or "$rowID")

    # Boughtouts
    if _table_id("boughtouts") and not (os.getenv("GLIDE_BOUGHTOUTS_TABLE") or "").strip():
        os.environ["GLIDE_BOUGHTOUTS_TABLE"] = _table_id("boughtouts")
    if not (os.getenv("GLIDE_BOUGHTOUTS_ROWID_COLUMN") or "").strip():
        os.environ["GLIDE_BOUGHTOUTS_ROWID_COLUMN"] = _s(_cols("boughtouts").get("row_id") or "$rowID")


def _norm_table_key(k: str) -> str:
    k = (k or "").strip().lower().replace("-", "_").replace(" ", "_")
    alias = {
        "rawmaterial": "raw_material",
        "raw_materials": "raw_material",
        "process": "processes",
        "proc": "processes",
        "boughtout": "boughtouts",
        "bought_out": "boughtouts",
        "bo": "boughtouts",
        "company": "company",
        "companies": "company",
    }
    return alias.get(k, k)


def _as_csv_list(s: str) -> List[str]:
    out: List[str] = []
    for x in (s or "").split(","):
        x = x.strip()
        if x:
            out.append(x)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", required=True, help="Comma-separated: company,raw_material,processes,boughtouts")
    ap.add_argument("--limit", type=int, default=0, help="Max rows per table (debug). 0=no limit")
    ap.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    args = ap.parse_args()

    _load_env()
    _ensure_glide_env_from_config_json()

    settings = load_settings()
    glide = GlideClient(settings)

    tables = [_norm_table_key(x) for x in _as_csv_list(args.tables)]

    runlog = RunLog(settings)

    # Build indexes ONCE
    # 1) Fetch company rows ONCE (used for both company ingest + tenant fallback mapping)
    company_rows: List[Dict[str, Any]] = []
    company_table = (settings.glide_company_table or "").strip()
    if company_table:
        company_rows = glide.list_table_rows(company_table)

    company_rowid_col = (settings.glide_company_rowid_column or "$rowID").strip()
    company_name_col = (settings.glide_company_name_column or "Name").strip()
    company_index = build_company_index(
        company_rows=company_rows,
        company_rowid_column=company_rowid_col,
        company_name_column=company_name_col,
    )

    # 2) Sheets project mapping ONCE (you said Projects table won't be ingested)
    sheet_by_trip, sheet_by_num = build_sheet_project_indexes(settings)

    grand_ok = 0
    grand_err = 0

    # ----------------------------
    # Company ingest (from fetched rows; NO per-row Glide fetch)
    # ----------------------------
    if "company" in tables:
        rows = company_rows
        if args.limit and args.limit > 0:
            rows = rows[: int(args.limit)]

        ok = 0
        err = 0
        for r in rows:
            rid = str((r or {}).get(company_rowid_col) or "").strip()
            primary = f"company:{rid}" if rid else "company:<missing>"
            run_id = runlog.start("UNKNOWN", "GLIDE_RECONCILE", primary)
            with bind_run_id(run_id):
                try:
                    if args.dry_run:
                        runlog.success(run_id)
                        ok += 1
                        continue
                    out = upsert_glide_company_row_dict(settings, row=r)
                    if out.get("ok"):
                        runlog.success(run_id)
                        ok += 1
                    else:
                        runlog.error(run_id, str(out.get("error") or "company upsert failed"))
                        err += 1
                except Exception as e:
                    runlog.error(run_id, str(e))
                    err += 1

        print(f"[DONE] company rows={len(rows)} ok={ok} err={err}")
        grand_ok += ok
        grand_err += err

    # ----------------------------
    # Glide KB tables: raw_material, processes, boughtouts
    # One list call per table; ingestion uses prebuilt indexes.
    # ----------------------------
    def run_kb_table(table_key: str) -> Tuple[int, int]:
        if table_key == "raw_material":
            spec = raw_material_spec(settings)
        elif table_key == "processes":
            spec = processes_spec(settings)
        elif table_key == "boughtouts":
            spec = boughtouts_spec(settings)
        else:
            return (0, 0)

        table_name = (spec.table_name or "").strip()
        if not table_name:
            print(f"[SKIP] {table_key}: missing table id in settings")
            return (0, 0)

        rows = glide.list_table_rows(table_name)
        if args.limit and args.limit > 0:
            rows = rows[: int(args.limit)]

        primary = f"{table_key}:FULLSCAN"
        run_id = runlog.start("UNKNOWN", "GLIDE_RECONCILE", primary)

        with bind_run_id(run_id):
            try:
                if args.dry_run:
                    runlog.success(run_id)
                    print(f"[DRY] {table_key} rows={len(rows)}")
                    return (1, 0)

                out = ingest_rows(
                    settings,
                    spec=spec,
                    rows=rows,
                    project_index_by_row_id={},
                    project_index_by_triplet={},
                    sheet_project_by_triplet=sheet_by_trip,
                    sheet_project_by_number=sheet_by_num,
                    company_index_by_name=company_index,
                    limit=0,
                )
                if out.get("ok"):
                    runlog.success(run_id)
                    print(
                        f"[DONE] {table_key} rows_seen={out.get('rows_seen')} ok={out.get('rows_ok')} "
                        f"skipped_unchanged={out.get('skipped_unchanged')} "
                        f"missing_tenant={out.get('skipped_missing_tenant')} errors={out.get('rows_error')}"
                    )
                    return (1, 0)
                else:
                    runlog.error(run_id, "ingest_rows returned ok=false")
                    return (0, 1)

            except Exception as e:
                runlog.error(run_id, str(e))
                return (0, 1)

    for tk in tables:
        if tk in ("raw_material", "processes", "boughtouts"):
            ok, err = run_kb_table(tk)
            grand_ok += ok
            grand_err += err

    print(f"[SUMMARY] ok={grand_ok} err={grand_err}")
    return 0 if grand_err == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())