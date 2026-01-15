#!/usr/bin/env python3
# To run this script, execute: python3 service/scripts/backfill_assembly_todo.py
# This script backfills the assembly todo list for manufacturing projects
# by processing projects marked as "MFG" in the status_assembly column.
# It uses the SheetsTool to fetch project data and triggers the event graph
# for each relevant project.
# Requires environment variables for configuration:
# - BACKFILL_SLEEP_SEC: seconds to sleep between processing each project (default:
#   0.25)
# - BACKFILL_MAX_IDS: maximum number of projects to process (default: 0, meaning no limit)  
# Ensure you have the necessary permissions and API access configured.
# Example usage:
#   export BACKFILL_SLEEP_SEC=0.5
#   export BACKFILL_MAX_IDS=100
#   python3 service/scripts/backfill_assembly_todo.py
# Note: This script assumes the existence of certain modules and functions
# within the service.app package, such as Settings, SheetsTool, and run_event_graph.


from __future__ import annotations

import os
import sys
import time
from typing import List, Dict, Any

# Ensure imports work when running from repo root
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from service.app.config import Settings
from service.app.tools.sheets_tool import SheetsTool, _key, _norm_value
from service.app.pipeline.graph import run_event_graph


def _is_mfg(x: str) -> bool:
    return (x or "").strip().lower() == "mfg"


def main():
    settings = Settings()
    sheets = SheetsTool(settings)

    projects = sheets.list_projects()

    # Mapping-driven columns (no hardcode)
    col_legacy = sheets.map.col("project", "legacy_id")
    col_status = sheets.map.col("project", "status_assembly")

    k_legacy = _key(col_legacy)
    k_status = _key(col_status)

    # Build list of legacy_ids to run
    ids: List[str] = []
    for pr in projects or []:
        legacy_id = _norm_value((pr or {}).get(k_legacy, ""))
        status = _norm_value((pr or {}).get(k_status, ""))
        if not legacy_id:
            continue
        # ✅ default: only MFG projects
        if _is_mfg(status):
            ids.append(legacy_id)

    # De-dupe in stable order
    seen = set()
    ids = [x for x in ids if not (x in seen or seen.add(x))]

    print(f"[backfill] projects_total={len(projects or [])} mfg_ids={len(ids)}")

    ok = 0
    fail = 0

    # Throttle to avoid Sheets quota + embedding spikes
    SLEEP_SEC = float(os.getenv("BACKFILL_SLEEP_SEC", "0.25"))
    MAX_IDS = int(os.getenv("BACKFILL_MAX_IDS", "0"))  # 0 = no limit

    for i, legacy_id in enumerate(ids, start=1):
        if MAX_IDS and i > MAX_IDS:
            print(f"[backfill] reached BACKFILL_MAX_IDS={MAX_IDS}, stopping.")
            break

        payload = {
            "event_type": "PROJECT_UPDATED",
            "legacy_id": legacy_id,
            "meta": {
                "tenant_id": "",  # optional; load_sheet_data isn't used here anyway
                "primary_id": legacy_id,
            },
        }

        try:
            out = run_event_graph(settings, payload)
            if out.get("ok") and out.get("assembly_todo_written") is not False:
                ok += 1
                print(f"[{i}/{len(ids)}] OK legacy_id={legacy_id}")
            else:
                fail += 1
                print(f"[{i}/{len(ids)}] FAIL legacy_id={legacy_id} out={out}")
        except Exception as e:
            fail += 1
            print(f"[{i}/{len(ids)}] EXCEPTION legacy_id={legacy_id} err={e}")

        time.sleep(SLEEP_SEC)

    print(f"[backfill] done ok={ok} fail={fail}")


if __name__ == "__main__":
    main()