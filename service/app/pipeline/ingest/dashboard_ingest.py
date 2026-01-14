from __future__ import annotations

from typing import Dict, Any, Tuple

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool


def ingest_dashboard(settings: Settings, limit: int = 2000) -> Dict[str, Any]:
    sheets = SheetsTool(settings)
    embedder = EmbedTool(settings)
    vec = VectorTool(settings)

    projects = sheets.list_projects()

    col_pid = sheets.map.col("project", "legacy_id")
    col_tenant = sheets.map.col("project", "company_row_id")
    col_pname = sheets.map.col("project", "project_name")
    col_pnum = sheets.map.col("project", "part_number")

    k_pid = _key(col_pid)
    k_tenant = _key(col_tenant)
    k_pname = _key(col_pname)
    k_pnum = _key(col_pnum)

    project_by_id: Dict[str, Dict[str, str]] = {}
    project_by_triplet: Dict[Tuple[str, str, str], Dict[str, str]] = {}

    for pr in projects:
        legacy_id = _norm_value(pr.get(k_pid, ""))
        tenant_id = _norm_value(pr.get(k_tenant, ""))
        pname = _norm_value(pr.get(k_pname, ""))
        pnum = _norm_value(pr.get(k_pnum, ""))

        if legacy_id:
            project_by_id[_key(legacy_id)] = {
                "tenant_id": tenant_id,
                "project_name": pname,
                "part_number": pnum,
                "legacy_id": legacy_id,
            }

        if legacy_id and pname and pnum:
            project_by_triplet[(_key(pname), _key(pnum), _key(legacy_id))] = {
                "tenant_id": tenant_id,
                "project_name": pname,
                "part_number": pnum,
                "legacy_id": legacy_id,
            }

    rows = sheets.list_dashboard_updates()
    if limit and limit > 0:
        rows = rows[:limit]

    col_proj = sheets.map.col("dashboard_update", "project_name")
    col_part = sheets.map.col("dashboard_update", "part_number")
    col_legacy = sheets.map.col("dashboard_update", "legacy_id")
    col_msg = sheets.map.col("dashboard_update", "update_message")
    # NEW: dashboard update unique row id (preferred)
    col_rowid = None
    try:
        col_rowid = sheets.map.col("dashboard_update", "dashboard_update_id")
    except Exception:
        col_rowid = "Row ID"  # Fallback if mapping missing

    k_proj = _key(col_proj)
    k_part = _key(col_part)
    k_legacy = _key(col_legacy)
    k_msg = _key(col_msg)
    k_rowid = _key(col_rowid) if col_rowid else _key("Row ID")


    seen = 0
    embedded = 0
    skipped = 0
    missing_legacy = 0
    missing_tenant = 0
    embed_errors = 0

    for r in rows:
        seen += 1
        legacy_id = _norm_value(r.get(k_legacy, ""))
        dash_row_id = _norm_value(r.get(k_rowid, "")) if k_rowid else ""
        if not legacy_id:
            missing_legacy += 1
            skipped += 1
            continue

        msg = _norm_value(r.get(k_msg, ""))
        if not msg:
            skipped += 1
            continue

        dash_project = _norm_value(r.get(k_proj, ""))
        dash_part = _norm_value(r.get(k_part, ""))

        pr = project_by_id.get(_key(legacy_id))
        if not pr and dash_project and dash_part:
            pr = project_by_triplet.get((_key(dash_project), _key(dash_part), _key(legacy_id)))

        tenant_id = _norm_value((pr or {}).get("tenant_id", ""))
        if not tenant_id:
            missing_tenant += 1
            skipped += 1
            continue

        project_name = _norm_value((pr or {}).get("project_name", "")) or dash_project
        part_number = _norm_value((pr or {}).get("part_number", "")) or dash_part

        text = f"[DASHBOARD UPDATE]\n{msg}".strip()

        try:
            emb = embedder.embed_text(text)

            # content_hash should be stable per dashboard row
            # If Row ID exists, it becomes the unique identity for that update
            if dash_row_id:
                ch = vec.hash_text(f"DASHBOARD|{tenant_id}|{dash_row_id}")
            else:
                # fallback (older sheets): stable on message + legacy linkage
                ch = vec.hash_text(f"DASHBOARD|{tenant_id}|{legacy_id}|{project_name}|{part_number}|{msg}")

            vec.upsert_dashboard_update(
                tenant_id=tenant_id,
                project_name=project_name or None,
                part_number=part_number or None,
                legacy_id=legacy_id or None,
                update_message=msg,
                embedding=emb,
                content_hash=ch,
            )
            embedded += 1
        except Exception:
            embed_errors += 1

    return {
        "source": "dashboard",
        "rows_seen": seen,
        "rows_embedded": embedded,
        "skipped": skipped,
        "missing_legacy_id": missing_legacy,
        "missing_tenant": missing_tenant,
        "embed_errors": embed_errors,
        "note": "Incremental ingestion via content_hash; safe to re-run anytime.",
    }


def ingest_dashboard_one(settings: Settings, *, legacy_id: str) -> Dict[str, Any]:
    """
    Incremental dashboard ingestion: ingest only the latest update row for this legacy_id.
    Called by event_type=DASHBOARD_UPDATED.
    """
    lid = (legacy_id or "").strip()
    if not lid:
        return {"ok": False, "error": "missing legacy_id"}

    sheets = SheetsTool(settings)
    embedder = EmbedTool(settings)
    vec = VectorTool(settings)

    # Build project lookup
    projects = sheets.list_projects()
    col_pid = sheets.map.col("project", "legacy_id")
    col_tenant = sheets.map.col("project", "company_row_id")
    col_pname = sheets.map.col("project", "project_name")
    col_pnum = sheets.map.col("project", "part_number")
    k_pid = _key(col_pid)
    k_tenant = _key(col_tenant)
    k_pname = _key(col_pname)
    k_pnum = _key(col_pnum)

    project_by_id: Dict[str, Dict[str, str]] = {}
    for pr in projects:
        pid = _norm_value(pr.get(k_pid, ""))
        if not pid:
            continue
        project_by_id[_key(pid)] = {
            "tenant_id": _norm_value(pr.get(k_tenant, "")),
            "project_name": _norm_value(pr.get(k_pname, "")),
            "part_number": _norm_value(pr.get(k_pnum, "")),
            "legacy_id": pid,
        }

    # Find latest dashboard update row for lid
    rows = sheets.list_dashboard_updates()
    col_legacy = sheets.map.col("dashboard_update", "legacy_id")
    col_msg = sheets.map.col("dashboard_update", "update_message")
    col_proj = sheets.map.col("dashboard_update", "project_name")
    col_part = sheets.map.col("dashboard_update", "part_number")

    k_legacy = _key(col_legacy)
    k_msg = _key(col_msg)
    k_proj = _key(col_proj)
    k_part = _key(col_part)

    hit = None
    for r in reversed(rows or []):
        if _norm_value((r or {}).get(k_legacy, "")) == lid:
            msg = _norm_value((r or {}).get(k_msg, ""))
            if msg:
                hit = r
                break

    if not hit:
        return {"ok": True, "skipped": True, "reason": f"no dashboard update found for legacy_id '{lid}'"}

    pr = project_by_id.get(_key(lid)) or {}
    tenant_id = _norm_value(pr.get("tenant_id", ""))
    if not tenant_id:
        return {"ok": True, "skipped": True, "reason": f"missing tenant for legacy_id '{lid}'"}

    project_name = _norm_value(pr.get("project_name", "")) or _norm_value(hit.get(k_proj, ""))
    part_number = _norm_value(pr.get("part_number", "")) or _norm_value(hit.get(k_part, ""))
    msg = _norm_value(hit.get(k_msg, ""))

    text = f"[DASHBOARD UPDATE]\n{msg}".strip()
    emb = embedder.embed_text(text)
    vec.upsert_dashboard_update(
        tenant_id=tenant_id,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=lid or None,
        update_message=msg,
        embedding=emb,
    )

    return {"ok": True, "legacy_id": lid, "rows_embedded": 1}

def ingest_dashboard_one_by_row_id(settings: Settings, *, dashboard_row_id: str) -> Dict[str, Any]:
    """
    Incremental dashboard ingestion: ingest the exact dashboard update row by Row ID.
    This is the correct trigger-based idempotency.
    Called by event_type=DASHBOARD_UPDATED when payload includes dashboard_update_id/row_id.
    """
    rid = (dashboard_row_id or "").strip()
    if not rid:
        return {"ok": False, "error": "missing dashboard_row_id"}

    sheets = SheetsTool(settings)
    embedder = EmbedTool(settings)
    vec = VectorTool(settings)

    # Build project lookup (legacy_id -> tenant)
    projects = sheets.list_projects()
    col_pid = sheets.map.col("project", "legacy_id")
    col_tenant = sheets.map.col("project", "company_row_id")
    col_pname = sheets.map.col("project", "project_name")
    col_pnum = sheets.map.col("project", "part_number")
    k_pid = _key(col_pid)
    k_tenant = _key(col_tenant)
    k_pname = _key(col_pname)
    k_pnum = _key(col_pnum)

    project_by_id: Dict[str, Dict[str, str]] = {}
    for pr in projects:
        pid = _norm_value(pr.get(k_pid, ""))
        if not pid:
            continue
        project_by_id[_key(pid)] = {
            "tenant_id": _norm_value(pr.get(k_tenant, "")),
            "project_name": _norm_value(pr.get(k_pname, "")),
            "part_number": _norm_value(pr.get(k_pnum, "")),
            "legacy_id": pid,
        }

    # Dashboard columns
    col_rowid = None
    try:
        col_rowid = sheets.map.col("dashboard_update", "dashboard_update_id")
    except Exception:
        col_rowid = "Row ID"

    col_legacy = sheets.map.col("dashboard_update", "legacy_id")
    col_msg = sheets.map.col("dashboard_update", "update_message")
    col_proj = sheets.map.col("dashboard_update", "project_name")
    col_part = sheets.map.col("dashboard_update", "part_number")

    k_rowid = _key(col_rowid)
    k_legacy = _key(col_legacy)
    k_msg = _key(col_msg)
    k_proj = _key(col_proj)
    k_part = _key(col_part)

    # Find exact row by Row ID
    rows = sheets.list_dashboard_updates()
    hit = None
    for r in rows or []:
        if _norm_value((r or {}).get(k_rowid, "")) == rid:
            msg = _norm_value((r or {}).get(k_msg, ""))
            if msg:
                hit = r
                break

    if not hit:
        return {"ok": True, "skipped": True, "reason": f"no dashboard update found for row_id '{rid}'"}

    legacy_id = _norm_value(hit.get(k_legacy, ""))
    msg = _norm_value(hit.get(k_msg, ""))
    dash_project = _norm_value(hit.get(k_proj, ""))
    dash_part = _norm_value(hit.get(k_part, ""))

    pr = project_by_id.get(_key(legacy_id)) or {}
    tenant_id = _norm_value(pr.get("tenant_id", ""))
    if not tenant_id:
        return {"ok": True, "skipped": True, "reason": f"missing tenant for legacy_id '{legacy_id}' (row_id={rid})"}

    project_name = _norm_value(pr.get("project_name", "")) or dash_project
    part_number = _norm_value(pr.get("part_number", "")) or dash_part

    text = f"[DASHBOARD UPDATE]\n{msg}".strip()
    emb = embedder.embed_text(text)

    ch = vec.hash_text(f"DASHBOARD|{tenant_id}|{rid}")

    vec.upsert_dashboard_update(
        tenant_id=tenant_id,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=legacy_id or None,
        update_message=msg,
        embedding=emb,
        content_hash=ch,
    )

    return {"ok": True, "dashboard_row_id": rid, "rows_embedded": 1, "tenant_id": tenant_id}