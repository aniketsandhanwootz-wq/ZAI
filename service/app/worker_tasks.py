from typing import Any, Dict, List
import logging

from .config import load_settings
from .pipeline.graph import run_event_graph
from .logctx import bind_run_id
from .pipeline.ingest.run_log import RunLog

from .pipeline.ingest.glide_ingest_raw_material import upsert_glide_raw_material_row
from .pipeline.ingest.glide_ingest_processes import upsert_glide_process_row
from .pipeline.ingest.glide_ingest_boughtouts import upsert_glide_boughtouts_row
from .pipeline.ingest.glide_ingest_project import upsert_glide_project_row  # may be no-op if project table not configured
from .pipeline.ingest.glide_ingest_company import upsert_glide_company_row
logger = logging.getLogger("zai.worker")


def process_event_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Existing: executed by RQ worker for main event graph.
    """
    settings = load_settings()
    logger.info(
        "worker task started. event_type=%s checkin_id=%s convo_id=%s",
        payload.get("event_type"),
        payload.get("checkin_id"),
        payload.get("conversation_id"),
    )
    return run_event_graph(settings, payload)


def _normalize_table_key(k: str) -> str:
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


def process_glide_webhook_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Executed by RQ worker: incremental upsert for Glide rows.
    Payload:
      {
        "table_key": "raw_material" | "processes" | "boughtouts" | "project",
        "row_ids": ["..."],
        "event": "updated" | "created" | ...
      }
    """
    settings = load_settings()

    table_key = _normalize_table_key(str(payload.get("table_key") or payload.get("table") or "").strip())
    row_ids = payload.get("row_ids") or payload.get("rowIds") or payload.get("row_id") or payload.get("rowId") or []
    if isinstance(row_ids, str):
        row_ids = [row_ids]
    row_ids = [str(x or "").strip() for x in (row_ids or []) if str(x or "").strip()]

    if not table_key or not row_ids:
        return {"ok": False, "error": "Missing table_key or row_ids", "payload": payload}

    # RunLog idempotency: key per (table,row_id). Tenant unknown at start => UNKNOWN.
    runlog = RunLog(settings)

    results: List[Dict[str, Any]] = []
    ok = 0
    err = 0

    for rid in row_ids:
        primary = f"{table_key}:{rid}"
        run_id = runlog.start("UNKNOWN", "GLIDE_KB_UPSERT", primary)

        with bind_run_id(run_id):
            try:
                if table_key == "raw_material":
                    out = upsert_glide_raw_material_row(settings, row_id=rid)
                elif table_key == "processes":
                    out = upsert_glide_process_row(settings, row_id=rid)
                elif table_key == "boughtouts":
                    out = upsert_glide_boughtouts_row(settings, row_id=rid)
                elif table_key == "project":
                    # if GLIDE_PROJECT_TABLE not configured, your code already "skips" in full scan.
                    out = upsert_glide_project_row(settings, row_id=rid)
                elif table_key == "company":
                    out = upsert_glide_company_row(settings, row_id=rid)
                else:
                    raise RuntimeError(f"Unknown table_key='{table_key}' (expected raw_material/processes/boughtouts/project/company)")

                # best-effort: infer tenant_id from result if present
                # (your ingest_rows returns ok + counts; not tenant. that's fine)
                runlog.success(run_id)
                ok += 1
                results.append({"row_id": rid, "ok": True, "result": out})
            except Exception as e:
                runlog.error(run_id, str(e))
                err += 1
                results.append({"row_id": rid, "ok": False, "error": str(e)[:500]})

    return {
        "ok": err == 0,
        "table_key": table_key,
        "rows": len(row_ids),
        "rows_ok": ok,
        "rows_error": err,
        "results": results[:50],  # limit payload size
    }