# One time script to backfill assembly todo items for existing assemblies
#!/usr/bin/env python3
# Run:
#   python3 service/scripts/backfill_assembly_todo.py
#
# What it does:
# - Reads Project sheet rows
# - Filters projects whose status_assembly is "mfg" or "ready for dispatch" (default)
# - Triggers PROJECT_UPDATED event graph for each legacy_id (read-only on vectors, writes to Project ai_critcal_point)
#
# Optional env:
#   BACKFILL_SLEEP_SEC=0.25
#   BACKFILL_MAX_IDS=0
#   BACKFILL_STATUSES="mfg,ready for dispatch"   # override allowed statuses

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Any

from dotenv import load_dotenv

# -------------------------
# Ensure imports work from repo root
# -------------------------
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Load env from service/.env (same behavior as service/app/main.py)
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=True)

from service.app.config import load_settings
from service.app.tools.sheets_tool import SheetsTool, _key, _norm_value
from service.app.pipeline.graph import run_event_graph


def _status_norm(x: str) -> str:
    # normalize spacing + case so "Ready  for   Dispatch" also matches
    return " ".join((x or "").strip().lower().split())


def _allowed_statuses() -> set[str]:
    """
    Default: {"mfg", "ready for dispatch"}
    Override:
      export BACKFILL_STATUSES="mfg"
      export BACKFILL_STATUSES="ready for dispatch"
      export BACKFILL_STATUSES="mfg,ready for dispatch"
    """
    raw = os.getenv("BACKFILL_STATUSES", "mfg,ready for dispatch")
    out = {_status_norm(s) for s in raw.split(",") if _status_norm(s)}
    # fallback safety
    return out or {"mfg", "ready for dispatch"}


def _status_allowed(x: str) -> bool:
    return _status_norm(x) in _allowed_statuses()


def main() -> None:
    settings = load_settings()
    sheets = SheetsTool(settings)

    projects = sheets.list_projects()

    # Mapping-driven columns (no hardcode)
    col_legacy = sheets.map.col("project", "legacy_id")
    col_status = sheets.map.col("project", "status_assembly")

    k_legacy = _key(col_legacy)
    k_status = _key(col_status)

    allowed = _allowed_statuses()

    # Build list of legacy_ids to run + status counts for visibility
    ids: List[str] = []
    status_counts: Dict[str, int] = {}

    for pr in projects or []:
        legacy_id = _norm_value((pr or {}).get(k_legacy, ""))
        status_raw = _norm_value((pr or {}).get(k_status, ""))
        status = _status_norm(status_raw)

        if status:
            status_counts[status] = status_counts.get(status, 0) + 1

        if not legacy_id:
            continue

        if _status_allowed(status_raw):
            ids.append(legacy_id)

    # De-dupe in stable order
    seen = set()
    ids = [x for x in ids if not (x in seen or seen.add(x))]

    # Throttle to avoid Sheets quota + embedding spikes
    SLEEP_SEC = float(os.getenv("BACKFILL_SLEEP_SEC", "0.25"))
    MAX_IDS = int(os.getenv("BACKFILL_MAX_IDS", "0"))  # 0 = no limit

    # Log summary
    print(f"[backfill] projects_total={len(projects or [])}")
    print(f"[backfill] allowed_statuses={sorted(list(allowed))}")
    # show a few status counts (useful to validate spelling like "Ready for Dispatch")
    if status_counts:
        top = sorted(status_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:20]
        print("[backfill] status_counts_top20=" + ", ".join([f"{k}:{v}" for k, v in top]))
    print(f"[backfill] matched_ids={len(ids)} max_ids={MAX_IDS or '∞'} sleep_sec={SLEEP_SEC}")

    ok = 0
    fail = 0

    total_to_run = len(ids) if not MAX_IDS else min(len(ids), MAX_IDS)

    for i, legacy_id in enumerate(ids, start=1):
        if MAX_IDS and i > MAX_IDS:
            print(f"[backfill] reached BACKFILL_MAX_IDS={MAX_IDS}, stopping.")
            break

        payload = {
            "event_type": "PROJECT_UPDATED",
            "legacy_id": legacy_id,
            "meta": {
                "primary_id": legacy_id,
            },
        }

        try:
            out = run_event_graph(settings, payload)

            # "ok" semantics depend on graph; keep it tolerant but informative
            is_ok = bool(out.get("ok")) if isinstance(out, dict) else False
            wrote = (out.get("assembly_todo_written") is not False) if isinstance(out, dict) else False

            if is_ok and wrote:
                ok += 1
                print(f"[{i}/{total_to_run}] OK legacy_id={legacy_id}")
            else:
                fail += 1
                print(f"[{i}/{total_to_run}] FAIL legacy_id={legacy_id} out={out}")
        except Exception as e:
            fail += 1
            print(f"[{i}/{total_to_run}] EXCEPTION legacy_id={legacy_id} err={e}")

        time.sleep(SLEEP_SEC)

    print(f"[backfill] done ok={ok} fail={fail}")


if __name__ == "__main__":
    main()