from __future__ import annotations

from typing import Any, Dict

from ...config import Settings
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool


def retrieve_context(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    tenant_id = (state.get("tenant_id") or "").strip()
    text = (state.get("thread_snapshot_text") or "").strip()

    if not tenant_id or not text:
        (state.get("logs") or []).append("Skipping retrieval (missing tenant/text)")
        state["similar_problems"] = []
        state["similar_resolutions"] = []
        state["relevant_ccp_chunks"] = []
        state["relevant_dashboard_updates"] = []
        return state

    embedder = EmbedTool(settings)
    vector_db = VectorTool(settings)

    q = embedder.embed_query(text)

    project_name = state.get("project_name")
    part_number = state.get("part_number")

    # 1) Similar PROBLEMS
    problems = vector_db.search_incidents(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=60,
        project_name=project_name,
        part_number=part_number,
        vector_type="PROBLEM",
    )

    # 2) Similar RESOLUTIONS (closure memory)
    resolutions = vector_db.search_incidents(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=60,
        project_name=project_name,
        part_number=part_number,
        vector_type="RESOLUTION",
    )

    # 3) CCP chunks
    ccp = vector_db.search_ccp_chunks(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=60,
        project_name=project_name,
        part_number=part_number,
    )

    # 4) Dashboard updates (if ingested; else empty)
    dash = vector_db.search_dashboard_updates(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=20,
        project_name=project_name,
        part_number=part_number,
    )

    state["similar_problems"] = problems
    state["similar_resolutions"] = resolutions
    state["relevant_ccp_chunks"] = ccp
    state["relevant_dashboard_updates"] = dash

    # backward-compat keys (if anything still reads them)
    state["similar_incidents"] = problems[:10]

    (state.get("logs") or []).append("Retrieved context buckets: problems + resolutions + ccp + dashboard")
    return state
