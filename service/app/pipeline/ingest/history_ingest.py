from __future__ import annotations

from typing import Dict, Any, List, Tuple

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool


def _build_snapshot(
    checkin_id: str,
    project_name: str,
    part_number: str,
    legacy_id: str,
    status: str,
    desc: str,
    convos: List[Dict[str, Any]],
    *,
    convo_remark_key: str,
    convo_status_key: str,
) -> str:
    recent: List[str] = []
    for c in convos[-15:]:
        remark = _norm_value(c.get(convo_remark_key, ""))
        st = _norm_value(c.get(convo_status_key, ""))
        if remark or st:
            recent.append(f"[{st}] {remark}".strip() if st else remark)

    header = f"CHECKIN_ID: {checkin_id}"
    meta = f"PROJECT: {project_name} | PART_NUMBER: {part_number} | LEGACY_ID: {legacy_id} | STATUS: {status}".strip()
    convo = "RECENT_CONVERSATION:\n- " + "\n- ".join(recent) if recent else "RECENT_CONVERSATION: (none)"
    body = f"DESCRIPTION:\n{desc}".strip() if desc else "DESCRIPTION: (empty)"

    return f"{header}\n{meta}\n{body}\n{convo}".strip()


def ingest_history(settings: Settings, limit: int = 500) -> Dict[str, Any]:
    sheets = SheetsTool(settings)
    embedder = EmbedTool(settings)
    vec = VectorTool(settings)

    # ---- Mapping column names (single source of truth) ----
    # CheckIN
    col_checkin_id = sheets.map.col("checkin", "checkin_id")
    col_legacy_id = sheets.map.col("checkin", "legacy_id")
    col_project_name = sheets.map.col("checkin", "project_name")
    col_part_number = sheets.map.col("checkin", "part_number")
    col_status = sheets.map.col("checkin", "status")
    col_desc = sheets.map.col("checkin", "description")

    k_checkin_id = _key(col_checkin_id)
    k_legacy_id = _key(col_legacy_id)
    k_project_name = _key(col_project_name)
    k_part_number = _key(col_part_number)
    k_status = _key(col_status)
    k_desc = _key(col_desc)

    # Project
    col_p_legacy = sheets.map.col("project", "legacy_id")
    col_p_tenant = sheets.map.col("project", "company_row_id")
    col_p_name = sheets.map.col("project", "project_name")
    col_p_part = sheets.map.col("project", "part_number")

    k_p_legacy = _key(col_p_legacy)
    k_p_tenant = _key(col_p_tenant)
    k_p_name = _key(col_p_name)
    k_p_part = _key(col_p_part)

    # Conversation
    col_convo_checkin_id = sheets.map.col("conversation", "checkin_id")
    col_convo_remark = sheets.map.col("conversation", "remark")
    col_convo_status = sheets.map.col("conversation", "status")

    k_convo_checkin_id = _key(col_convo_checkin_id)
    k_convo_remark = _key(col_convo_remark)
    k_convo_status = _key(col_convo_status)

    # ---- Load all once (cached; avoids quota spam) ----
    all_checkins = sheets.list_checkins()
    if limit and limit > 0:
        all_checkins = all_checkins[:limit]

    projects = sheets.list_projects()

    projects_missing_tenant = 0
    projects_missing_tenant_sample: List[str] = []

    for pr in projects:
        pid = _norm_value(pr.get(k_p_legacy, ""))
        tenant_id = _norm_value(pr.get(k_p_tenant, ""))
        if pid and not tenant_id:
            projects_missing_tenant += 1
            if len(projects_missing_tenant_sample) < 20:
                projects_missing_tenant_sample.append(pid)

    # ---- Build project indexes (ID-first + fallback triplet) ----
    project_by_id: Dict[str, Dict[str, str]] = {}
    project_by_triplet: Dict[Tuple[str, str, str], Dict[str, str]] = {}

    for pr in projects:
        pid = _norm_value(pr.get(k_p_legacy, ""))
        tenant_id = _norm_value(pr.get(k_p_tenant, ""))
        pname = _norm_value(pr.get(k_p_name, ""))
        pnum = _norm_value(pr.get(k_p_part, ""))

        if pid:
            project_by_id[_key(pid)] = {
                "tenant_id": tenant_id,
                "project_name": pname,
                "part_number": pnum,
                "legacy_id": pid,
            }

        if pid and pname and pnum:
            project_by_triplet[(_key(pname), _key(pnum), _key(pid))] = {
                "tenant_id": tenant_id,
                "project_name": pname,
                "part_number": pnum,
                "legacy_id": pid,
            }

    done = 0
    skipped = 0

    missing_checkin_id = 0
    missing_legacy_id = 0
    missing_project_match = 0
    missing_tenant = 0
    embed_errors = 0

    for c in all_checkins:
        checkin_id = _norm_value(c.get(k_checkin_id, ""))
        legacy_id = _norm_value(c.get(k_legacy_id, ""))
        project_name = _norm_value(c.get(k_project_name, ""))
        part_number = _norm_value(c.get(k_part_number, ""))
        status = _norm_value(c.get(k_status, ""))
        desc = _norm_value(c.get(k_desc, ""))

        if not checkin_id:
            missing_checkin_id += 1
            skipped += 1
            continue

        if not legacy_id:
            missing_legacy_id += 1
            skipped += 1
            continue

        # Resolve tenant_id (ID-first)
        proj = project_by_id.get(_key(legacy_id))
        if not proj and project_name and part_number:
            # fallback triplet
            proj = project_by_triplet.get((_key(project_name), _key(part_number), _key(legacy_id)))

        if not proj:
            missing_project_match += 1
            skipped += 1
            continue

        tenant_id = _norm_value(proj.get("tenant_id", ""))
        if not tenant_id:
            missing_tenant += 1
            skipped += 1
            continue

        # Fill missing values from Project tab
        if not project_name:
            project_name = _norm_value(proj.get("project_name", ""))
        if not part_number:
            part_number = _norm_value(proj.get("part_number", ""))

        # Conversations for this checkin
        # NOTE: sheets.get_conversations_for_checkin() already uses mapping inside SheetsTool,
        # so we can call it directly (cached).
        convos = sheets.get_conversations_for_checkin(checkin_id)

        snapshot = _build_snapshot(
            checkin_id=checkin_id,
            project_name=project_name,
            part_number=part_number,
            legacy_id=legacy_id,
            status=status,
            desc=desc,
            convos=convos,
            convo_remark_key=k_convo_remark,
            convo_status_key=k_convo_status,
        )

        try:
            emb = embedder.embed_text(snapshot)

            vec.upsert_incident_vector(
                tenant_id=tenant_id,
                checkin_id=checkin_id,
                vector_type="PROBLEM",
                embedding=emb,
                project_name=project_name,
                part_number=part_number,
                legacy_id=legacy_id,
                status=status,
                text=snapshot,
            )

            # Store RESOLUTION memory only when we have closure-like evidence in conversation.
            # This avoids polluting RESOLUTION vectors with generic problem snapshots.
            closure_lines: List[str] = []
            for cc in convos[-25:]:
                remark = _norm_value(cc.get(k_convo_remark, ""))
                st = _norm_value(cc.get(k_convo_status, ""))
                if not remark:
                    continue
                low = remark.lower()
                if st.strip().upper() in ("PASS", "OK", "CLOSED", "DONE", "RESOLVED") or any(
                    kw in low for kw in ("fixed", "resolved", "rework", "replaced", "offset", "tool", "fixture", "grind", "heat treat", "stress relieve", "measured", "cmm")
                ):
                    prefix = f"[{st}] " if st else ""
                    closure_lines.append(f"{prefix}{remark}".strip())

            # keep it tight
            closure_lines = closure_lines[-8:]

            if closure_lines:
                resolution_text = (
                    "CLOSURE SUMMARY (from conversation; factual):\n- "
                    + "\n- ".join(closure_lines)
                ).strip()

                emb_r = embedder.embed_text(resolution_text)
                vec.upsert_incident_vector(
                    tenant_id=tenant_id,
                    checkin_id=checkin_id,
                    vector_type="RESOLUTION",
                    embedding=emb_r,
                    project_name=project_name,
                    part_number=part_number,
                    legacy_id=legacy_id,
                    status=status,
                    text=resolution_text,
                )
            done += 1

        except Exception:
            embed_errors += 1
            skipped += 1

    return {
        "source": "history",
        "threads_embedded": done,
        "skipped": skipped,
        "limit": limit,
        "missing_checkin_id": missing_checkin_id,
        "missing_legacy_id": missing_legacy_id,
        "missing_project_match": missing_project_match,
        "missing_tenant": missing_tenant,
        "embed_errors": embed_errors,
        "note": "Uses mapping-driven + normalized keys; ID-first tenant resolution via Project tab.",
        "projects_missing_company_row_id": projects_missing_tenant,
        "projects_missing_company_row_id_sample_legacy_ids": projects_missing_tenant_sample,
    }
