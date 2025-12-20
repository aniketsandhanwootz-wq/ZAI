# service/app/pipeline/nodes/retrieve_context.py
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
        state["similar_incidents"] = []
        state["relevant_ccp_chunks"] = []
        return state

    embedder = EmbedTool(settings)
    vector_db = VectorTool(settings)

    q = embedder.embed_query(text)  # ✅ query embedding

    # Broader fetch (Phase-2 quality)
    # Start with 30, we’ll rerank later if needed
    candidates_inc = vector_db.search_incidents(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=30,
        project_name=state.get("project_name"),
        part_number=state.get("part_number"),
    )

    candidates_ccp = vector_db.search_ccp_chunks(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=30,
        project_name=state.get("project_name"),
        part_number=state.get("part_number"),
    )

    # MVP: no rerank yet, just take top 8 each
    state["similar_incidents"] = candidates_inc[:8]
    state["relevant_ccp_chunks"] = candidates_ccp[:8]

    (state.get("logs") or []).append("Retrieved context (incidents + CCP)")
    return state
