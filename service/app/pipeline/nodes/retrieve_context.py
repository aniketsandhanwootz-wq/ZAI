from __future__ import annotations

from typing import Any, Dict, List

from ...config import Settings
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool


def _drop_self(rows: List[Dict[str, Any]], self_checkin_id: str) -> List[Dict[str, Any]]:
    if not rows:
        return []
    sid = (self_checkin_id or "").strip()
    if not sid:
        return rows
    return [r for r in rows if str(r.get("checkin_id") or "").strip() != sid]


def _compose_query_text(state: Dict[str, Any]) -> str:
    """
    Retrieval query should include the best signal:
    - snapshot text
    - checkin description (if not already embedded)
    - attachment_context (summaries produced by analyze_attachments)
    This improves correlation to project context + attachments.
    """
    parts: List[str] = []

    snap = (state.get("thread_snapshot_text") or "").strip()
    if snap:
        parts.append("THREAD SNAPSHOT:\n" + snap)

    desc = (state.get("checkin_description") or "").strip()
    if desc and desc not in snap:
        parts.append("CHECKIN DESCRIPTION:\n" + desc)

    att_ctx = (state.get("attachment_context") or "").strip()
    if att_ctx:
        # keep bounded so embeddings stay stable
        if len(att_ctx) > 6000:
            att_ctx = att_ctx[:6000] + "\n[TRUNCATED]"
        parts.append("ATTACHMENT EVIDENCE (Files column summaries):\n" + att_ctx)

    return "\n\n".join([p for p in parts if p.strip()]).strip()


def retrieve_context(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    tenant_id = (state.get("tenant_id") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()

    query_text = _compose_query_text(state)
    if not tenant_id or not query_text:
        (state.get("logs") or []).append("Skipping retrieval (missing tenant/text)")
        state["similar_problems"] = []
        state["similar_resolutions"] = []
        state["similar_media"] = []
        state["relevant_ccp_chunks"] = []
        state["relevant_dashboard_updates"] = []
        state["relevant_glide_kb_chunks"] = []
        state["retrieval_query_text"] = query_text
        return state

    embedder = EmbedTool(settings)
    vector_db = VectorTool(settings)

    q = embedder.embed_query(query_text)
    state["retrieval_query_text"] = query_text

    # 0) Company profile (tenant-scoped when tenant_id is known)
    try:
        if tenant_id:
            row = vector_db.get_company_profile_by_tenant_row_id(tenant_row_id=tenant_id)
            if row:
                state["company_profile_matches"] = [
                    {
                        "tenant_row_id": row.get("tenant_row_id"),
                        "company_name": row.get("company_name"),
                        "company_description": row.get("company_description"),
                        "distance": 0.0,
                        "mode": "tenant_exact",
                    }
                ]
                state["company_profile_text"] = (
                    f"Company: {row.get('company_name','')}\n"
                    f"Client description: {row.get('company_description','')}"
                ).strip()
            else:
                state["company_profile_matches"] = []
        else:
            company_matches = vector_db.search_company_profiles(query_embedding=q, top_k=1)
            state["company_profile_matches"] = company_matches
            if company_matches:
                best = company_matches[0]
                state["company_profile_text"] = (
                    f"Company: {best.get('company_name')}\nClient description: {best.get('company_description')}"
                ).strip()
    except Exception as e:
        (state.get("logs") or []).append(f"Company profile retrieval failed (non-fatal): {e}")
        state["company_profile_matches"] = []

    project_name = state.get("project_name")
    part_number = state.get("part_number")
    legacy_id = state.get("legacy_id")

    # 0.5) Glide KB chunks (RawMaterial/Processes/Boughtouts MUST be included)
    critical_tables = ["raw_material", "processes", "boughtouts"]

    def _dedup_chunks(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        out = []
        for it in items or []:
            k = (
                (it.get("table_name") or "").strip(),
                (it.get("item_id") or "").strip(),
                int(it.get("chunk_index") or 0),
            )
            if k in seen:
                continue
            seen.add(k)
            out.append(it)
        return out

    critical = vector_db.search_glide_kb_chunks(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=36,
        project_name=project_name,
        part_number=part_number,
        legacy_id=legacy_id,
        table_names=critical_tables,
    )

    general = vector_db.search_glide_kb_chunks(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=40,
        project_name=project_name,
        part_number=part_number,
        legacy_id=legacy_id,
        table_names=None,
    )

    state["relevant_glide_kb_chunks"] = _dedup_chunks((critical or []) + (general or []))

    # 1) Similar PROBLEMS
    problems = vector_db.search_incidents(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=60,
        project_name=project_name,
        part_number=part_number,
        vector_type="PROBLEM",
    )

    # 2) Similar RESOLUTIONS
    resolutions = vector_db.search_incidents(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=60,
        project_name=project_name,
        part_number=part_number,
        vector_type="RESOLUTION",
    )

    # 3) Similar MEDIA (captions)
    media = vector_db.search_incidents(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=60,
        project_name=project_name,
        part_number=part_number,
        vector_type="MEDIA",
    )

    problems = _drop_self(problems, checkin_id)
    resolutions = _drop_self(resolutions, checkin_id)
    media = _drop_self(media, checkin_id)

    # 4) CCP chunks
    ccp = vector_db.search_ccp_chunks(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=60,
        project_name=project_name,
        part_number=part_number,
    )

    # 5) Dashboard updates
    dash = vector_db.search_dashboard_updates(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=20,
        project_name=project_name,
        part_number=part_number,
    )

    state["similar_problems"] = problems
    state["similar_resolutions"] = resolutions
    state["similar_media"] = media
    state["relevant_ccp_chunks"] = ccp
    state["relevant_dashboard_updates"] = dash

    # backward-compat
    state["similar_incidents"] = problems[:10]

    (state.get("logs") or []).append(
        "Retrieved context using composed query (snapshot+attachments): "
        f"problems={len(problems)} resolutions={len(resolutions)} media={len(media)} ccp={len(ccp)} dash={len(dash)}"
    )
    return state