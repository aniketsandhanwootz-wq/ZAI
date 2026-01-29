# service/scripts/glide_reconcile.py
"""
Manual Glide reconciliation runner.

Purpose:
- Safety net when a webhook is missed.
- Pull recent/limited rows from Glide tables, then re-run your existing
  single-row upsert functions (same logic as webhook path).

Run (from repo root):
  PYTHONPATH=. python service/scripts/glide_reconcile.py --tables company --since-hours 24
  PYTHONPATH=. python service/scripts/glide_reconcile.py --tables raw_material,processes,boughtouts --limit 200
  PYTHONPATH=. python service/scripts/glide_reconcile.py --tables company --row-ids "row1,row2,row3"

Notes:
- Glide "updatedAt" field is not guaranteed. This script tries several common keys.
  If it can't find timestamps, it will fall back to processing rows up to --limit.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# Ensure imports work when running as a script
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from service.app.config import load_settings
from service.app.integrations.glide_client import GlideClient

from service.app.pipeline.ingest.glide_ingest_raw_material import upsert_glide_raw_material_row
from service.app.pipeline.ingest.glide_ingest_processes import upsert_glide_process_row
from service.app.pipeline.ingest.glide_ingest_boughtouts import upsert_glide_boughtouts_row
from service.app.pipeline.ingest.glide_ingest_project import upsert_glide_project_row
from service.app.pipeline.ingest.glide_ingest_company import upsert_glide_company_row

from service.app.pipeline.ingest.run_log import RunLog
from service.app.logctx import bind_run_id


# ----------------------------
# Helpers
# ----------------------------

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
        "project": "project",
        "projects": "project",
        "company": "company",
        "companies": "company",
        "company_profile": "company",
        "company_profiles": "company",
    }
    return alias.get(k, k)


def _as_csv_list(s: str) -> List[str]:
    out: List[str] = []
    for x in (s or "").split(","):
        x = x.strip()
        if x:
            out.append(x)
    return out


def _pick_rowid_column(rows: List[Dict[str, Any]], fallback: str) -> str:
    """
    Best-effort identify the Row ID column present in the table rows.
    """
    if not rows:
        return fallback

    r0 = rows[0]
    candidates = [
        "$rowID",
        "Row ID",
        "row_id",
        "rowId",
        "rowID",
        "id",
        "$RowID",
    ]
    for c in candidates:
        if c in r0:
            return c
    return fallback


def _get_row_id(row: Dict[str, Any], rowid_column: str) -> str:
    v = row.get(rowid_column)
    s = str(v or "").strip()
    return s


def _parse_dt(x: Any) -> Optional[datetime]:
    """
    Parse multiple timestamp shapes:
      - ISO 8601 strings (with/without 'Z')
      - 'YYYY-MM-DD HH:MM:SS' (assumed UTC)
      - epoch seconds/ms (int-like)
    """
    if x is None:
        return None

    # epoch
    if isinstance(x, (int, float)):
        try:
            # heuristics: ms if too large
            v = float(x)
            if v > 1e12:
                v = v / 1000.0
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            return None

    s = str(x).strip()
    if not s:
        return None

    # epoch in string
    if s.isdigit():
        try:
            v = float(s)
            if v > 1e12:
                v = v / 1000.0
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            pass

    # ISO-ish
    try:
        s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # common fallback: "YYYY-MM-DD HH:MM:SS"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue

    return None


def _get_updated_at(row: Dict[str, Any]) -> Optional[datetime]:
    """
    Glide may or may not provide a usable updated timestamp.
    We try common keys (including Glide-like variations).
    """
    keys = [
        "$updatedAt",
        "$UpdatedAt",
        "updatedAt",
        "UpdatedAt",
        "updated_at",
        "Updated at",
        "Updated At",
        "Modified",
        "Modified At",
        "Last Updated",
        "last_updated",
    ]
    for k in keys:
        if k in row:
            dt = _parse_dt(row.get(k))
            if dt:
                return dt
    return None


def _resolve_table_name_from_settings(settings, table_key: str) -> str:
    """
    Map normalized table_key -> settings.<glide_..._table> string.
    """
    attr = {
        "raw_material": "glide_raw_material_table",
        "processes": "glide_processes_table",
        "boughtouts": "glide_boughtouts_table",
        "project": "glide_project_table",
        "company": "glide_company_table",
    }.get(table_key, "")

    if not attr:
        return ""

    return str(getattr(settings, attr, "") or "").strip()


def _upsert_one(settings, table_key: str, row_id: str) -> Dict[str, Any]:
    """
    Reuse the exact same ingest functions as webhook path.
    """
    if table_key == "raw_material":
        return upsert_glide_raw_material_row(settings, row_id=row_id)
    if table_key == "processes":
        return upsert_glide_process_row(settings, row_id=row_id)
    if table_key == "boughtouts":
        return upsert_glide_boughtouts_row(settings, row_id=row_id)
    if table_key == "project":
        return upsert_glide_project_row(settings, row_id=row_id)
    if table_key == "company":
        return upsert_glide_company_row(settings, row_id=row_id)

    raise RuntimeError(f"Unknown table_key='{table_key}'")


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--tables",
        required=True,
        help="Comma-separated: raw_material,processes,boughtouts,project,company",
    )
    ap.add_argument(
        "--since-hours",
        type=float,
        default=0.0,
        help="Best-effort filter by updated timestamp (if present). 0 = disabled",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max rows per table to process (after filtering). 0 = no limit",
    )
    ap.add_argument(
        "--row-ids",
        default="",
        help="Optional comma-separated explicit row IDs to upsert (skips listing table rows).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be processed without writing to DB.",
    )
    args = ap.parse_args()

    settings = load_settings()
    glide = GlideClient(settings)

    tables = [_norm_table_key(x) for x in _as_csv_list(args.tables)]
    explicit_row_ids = _as_csv_list(args.row_ids)

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=float(args.since_hours or 0.0))

    runlog = RunLog(settings)

    grand_ok = 0
    grand_err = 0

    for table_key in tables:
        table_name = _resolve_table_name_from_settings(settings, table_key)
        if not table_name:
            print(f"[SKIP] table_key={table_key}: missing settings table name")
            continue

        # Determine which row_ids to process
        row_ids: List[str] = []

        if explicit_row_ids:
            row_ids = explicit_row_ids[:]
        else:
            rows = glide.list_table_rows(table_name)
            if not rows:
                print(f"[OK] table_key={table_key} rows=0 (empty)")
                continue

            rowid_col_default = "$rowID" if table_key == "company" else "Row ID"
            rowid_col = _pick_rowid_column(rows, fallback=rowid_col_default)

            # optional updated filter
            if args.since_hours and args.since_hours > 0:
                filtered: List[Tuple[str, Optional[datetime]]] = []
                missing_ts = 0

                for r in rows:
                    rid = _get_row_id(r, rowid_col)
                    if not rid:
                        continue
                    dt = _get_updated_at(r)
                    if not dt:
                        missing_ts += 1
                        continue
                    if dt >= cutoff:
                        filtered.append((rid, dt))

                if filtered:
                    # newest first
                    filtered.sort(key=lambda x: x[1] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
                    row_ids = [rid for rid, _ in filtered]
                    print(
                        f"[INFO] table_key={table_key} picked={len(row_ids)} using timestamps (cutoff={cutoff.isoformat()}); "
                        f"missing_ts_rows={missing_ts}"
                    )
                else:
                    # no usable timestamps -> fallback to top-N rows
                    row_ids = [ _get_row_id(r, rowid_col) for r in rows if _get_row_id(r, rowid_col) ]
                    print(
                        f"[WARN] table_key={table_key}: no usable updated timestamps found; "
                        f"falling back to row order (rows={len(row_ids)})"
                    )
            else:
                row_ids = [ _get_row_id(r, rowid_col) for r in rows if _get_row_id(r, rowid_col) ]

        if args.limit and args.limit > 0:
            row_ids = row_ids[: int(args.limit)]

        if not row_ids:
            print(f"[OK] table_key={table_key} rows=0 (nothing to process)")
            continue

        print(f"[RUN] table_key={table_key} table={table_name} rows_to_process={len(row_ids)} dry_run={args.dry_run}")

        ok = 0
        err = 0

        for rid in row_ids:
            primary = f"{table_key}:{rid}"
            run_id = runlog.start("UNKNOWN", "GLIDE_RECONCILE", primary)

            with bind_run_id(run_id):
                try:
                    if args.dry_run:
                        runlog.success(run_id)
                        ok += 1
                        continue

                    _ = _upsert_one(settings, table_key, rid)
                    runlog.success(run_id)
                    ok += 1
                except Exception as e:
                    runlog.error(run_id, str(e))
                    err += 1

        print(f"[DONE] table_key={table_key} ok={ok} err={err}")
        grand_ok += ok
        grand_err += err

    print(f"[SUMMARY] ok={grand_ok} err={grand_err}")
    return 0 if grand_err == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())