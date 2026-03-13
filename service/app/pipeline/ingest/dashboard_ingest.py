from __future__ import annotations

from typing import Dict, Any, Tuple, Optional

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool


def _get_dashboard_identity_keys(sheets: SheetsTool) -> tuple[str, Optional[str]]:
    """
    Returns:
      - mapped dashboard update id header key (preferred)
      - raw Row ID header key fallback (if present in sheet rows)
    """
    col_dash_id = sheets.map.col("dashboard_update", "dashboard_update_id")
    return _key(col_dash_id), _key("Row ID")


def _extract_dashboard_update_id(
    row: Dict[str, Any],
    *,
    k_dash_id: str,
    k_row_id: Optional[str] = None,
) -> str:
    """
    Preferred identity is Dashboard Update ID.
    Fallback to Row ID only for backward compatibility with older rows/sheets.
    """
    dash_id = _norm_value((row or {}).get(k_dash_id, ""))
    if dash_id:
        return dash_id

    if k_row_id:
        row_id = _norm_value((row or {}).get(k_row_id, ""))
        if row_id:
            return row_id

    return ""

def _match_dashboard_row_identity(
    row: Dict[str, Any],
    *,
    incoming_id: str,
    k_dash_id: str,
    k_row_id: Optional[str] = None,
) -> tuple[bool, str]:
    """
    Match incoming dashboard identity against BOTH raw columns:
      - Dashboard Update ID
      - Row ID (backward compatibility)

    Returns:
      (matched?, canonical_dashboard_update_id)

    Canonical id preference:
      1. Dashboard Update ID
      2. Row ID (only if Dashboard Update ID is absent on that row)
    """
    incoming = _norm_value(incoming_id)
    if not incoming:
        return False, ""

    raw_dash_id = _norm_value((row or {}).get(k_dash_id, ""))
    raw_row_id = _norm_value((row or {}).get(k_row_id, "")) if k_row_id else ""

    if raw_dash_id and raw_dash_id == incoming:
        return True, raw_dash_id

    if raw_row_id and raw_row_id == incoming:
        return True, raw_dash_id or raw_row_id

    return False, ""
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

    k_proj = _key(col_proj)
    k_part = _key(col_part)
    k_legacy = _key(col_legacy)
    k_msg = _key(col_msg)

    k_dash_id, k_row_id = _get_dashboard_identity_keys(sheets)

    seen = 0
    embedded = 0
    skipped = 0
    missing_legacy = 0
    missing_tenant = 0
    embed_errors = 0
    missing_dashboard_update_id = 0

    for r in rows:
        seen += 1

        legacy_id = _norm_value(r.get(k_legacy, ""))
        dashboard_update_id = _extract_dashboard_update_id(
            r,
            k_dash_id=k_dash_id,
            k_row_id=k_row_id,
        )

        if not dashboard_update_id:
            missing_dashboard_update_id += 1
            skipped += 1
            continue

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

            # Canonical uniqueness must now be Dashboard Update ID
            ch = vec.hash_text(f"DASHBOARD|{tenant_id}|{dashboard_update_id}")

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
        "missing_dashboard_update_id": missing_dashboard_update_id,
        "missing_legacy_id": missing_legacy,
        "missing_tenant": missing_tenant,
        "embed_errors": embed_errors,
        "note": "Incremental ingestion via content_hash using Dashboard Update ID; safe to re-run anytime.",
    }


def ingest_dashboard_one(settings: Settings, *, legacy_id: str) -> Dict[str, Any]:
    """
    Legacy fallback path:
    ingest only the latest dashboard update row for this legacy_id.

    This is NOT the preferred precise identity path.
    Preferred path is ingest_dashboard_one_by_dashboard_update_id().
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

    rows = sheets.list_dashboard_updates()
    col_legacy = sheets.map.col("dashboard_update", "legacy_id")
    col_msg = sheets.map.col("dashboard_update", "update_message")
    col_proj = sheets.map.col("dashboard_update", "project_name")
    col_part = sheets.map.col("dashboard_update", "part_number")

    k_legacy = _key(col_legacy)
    k_msg = _key(col_msg)
    k_proj = _key(col_proj)
    k_part = _key(col_part)

    k_dash_id, k_row_id = _get_dashboard_identity_keys(sheets)

    hit = None
    hit_dashboard_update_id = ""
    for r in reversed(rows or []):
        if _norm_value((r or {}).get(k_legacy, "")) == lid:
            msg = _norm_value((r or {}).get(k_msg, ""))
            if not msg:
                continue

            dashboard_update_id = _extract_dashboard_update_id(
                r,
                k_dash_id=k_dash_id,
                k_row_id=k_row_id,
            )
            if not dashboard_update_id:
                continue

            hit = r
            hit_dashboard_update_id = dashboard_update_id
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

    ch = vec.hash_text(f"DASHBOARD|{tenant_id}|{hit_dashboard_update_id}")

    vec.upsert_dashboard_update(
        tenant_id=tenant_id,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=lid or None,
        update_message=msg,
        embedding=emb,
        content_hash=ch,
    )

    return {
        "ok": True,
        "legacy_id": lid,
        "dashboard_update_id": hit_dashboard_update_id,
        "rows_embedded": 1,
    }


def ingest_dashboard_one_by_dashboard_update_id(
    settings: Settings,
    *,
    dashboard_update_id: str,
) -> Dict[str, Any]:
    """
    Preferred incremental dashboard ingestion:
    ingest the exact dashboard update row by Dashboard Update ID.
    """
    did = (dashboard_update_id or "").strip()
    if not did:
        return {"ok": False, "error": "missing dashboard_update_id"}

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
    col_legacy = sheets.map.col("dashboard_update", "legacy_id")
    col_msg = sheets.map.col("dashboard_update", "update_message")
    col_proj = sheets.map.col("dashboard_update", "project_name")
    col_part = sheets.map.col("dashboard_update", "part_number")

    k_legacy = _key(col_legacy)
    k_msg = _key(col_msg)
    k_proj = _key(col_proj)
    k_part = _key(col_part)

    k_dash_id, k_row_id = _get_dashboard_identity_keys(sheets)

    # Find exact row by incoming dashboard identity.
    # Incoming value may be either:
    #   - Dashboard Update ID (preferred)
    #   - old Row ID alias (backward compatibility)
    # Once matched, always normalize to canonical Dashboard Update ID if available.
    rows = sheets.list_dashboard_updates()
    hit = None
    resolved_id = ""
    for r in rows or []:
        matched, canonical_id = _match_dashboard_row_identity(
            r,
            incoming_id=did,
            k_dash_id=k_dash_id,
            k_row_id=k_row_id,
        )
        if not matched:
            continue

        msg = _norm_value((r or {}).get(k_msg, ""))
        if not msg:
            continue

        hit = r
        resolved_id = canonical_id
        break

    if not hit:
        return {"ok": True, "skipped": True, "reason": f"no dashboard update found for dashboard_update_id '{did}'"}

    legacy_id = _norm_value(hit.get(k_legacy, ""))
    msg = _norm_value(hit.get(k_msg, ""))
    dash_project = _norm_value(hit.get(k_proj, ""))
    dash_part = _norm_value(hit.get(k_part, ""))

    pr = project_by_id.get(_key(legacy_id)) or {}
    tenant_id = _norm_value(pr.get("tenant_id", ""))
    if not tenant_id:
        return {
            "ok": True,
            "skipped": True,
            "reason": f"missing tenant for legacy_id '{legacy_id}' (dashboard_update_id={did})",
        }

    project_name = _norm_value(pr.get("project_name", "")) or dash_project
    part_number = _norm_value(pr.get("part_number", "")) or dash_part

    text = f"[DASHBOARD UPDATE]\n{msg}".strip()
    emb = embedder.embed_text(text)

    ch = vec.hash_text(f"DASHBOARD|{tenant_id}|{resolved_id}")

    vec.upsert_dashboard_update(
        tenant_id=tenant_id,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=legacy_id or None,
        update_message=msg,
        embedding=emb,
        content_hash=ch,
    )

    return {
        "ok": True,
        "dashboard_update_id": resolved_id,
        "rows_embedded": 1,
        "tenant_id": tenant_id,
    }


def ingest_dashboard_one_by_row_id(settings: Settings, *, dashboard_row_id: str) -> Dict[str, Any]:
    """
    Backward-compatible wrapper.
    Older callers may still send dashboard_row_id, but the system now resolves
    dashboard rows using Dashboard Update ID as the canonical identifier.
    """
    rid = (dashboard_row_id or "").strip()
    if not rid:
        return {"ok": False, "error": "missing dashboard_row_id"}

    return ingest_dashboard_one_by_dashboard_update_id(
        settings,
        dashboard_update_id=rid,
    )