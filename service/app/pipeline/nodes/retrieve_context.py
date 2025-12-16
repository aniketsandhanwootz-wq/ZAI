from ..state import GraphState
from ...config import Settings
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool


def retrieve_context(state: GraphState, config):
    settings: Settings = config["settings"]
    if not state.tenant_id or not state.thread_snapshot_text:
        state.logs.append("Skipping retrieval (missing tenant/text)")
        return state

    embedder = EmbedTool(settings)
    vector_db = VectorTool(settings)

    q = embedder.embed_text(state.thread_snapshot_text)

    state.similar_incidents = vector_db.search_incidents(
        tenant_id=state.tenant_id,
        query_embedding=q,
        top_k=5,
        project_name=state.project_name,
        part_number=state.part_number,
    )

    # CCP retrieval can be added next iteration once CCP ingestion exists.
    state.relevant_ccp_chunks = vector_db.search_ccp_chunks(
        tenant_id=state.tenant_id,
        query_embedding=q,
        top_k=5,
        project_name=state.project_name,
        part_number=state.part_number,
    )

    state.logs.append("Retrieved context (similar incidents + CCP chunks)")
    return state
