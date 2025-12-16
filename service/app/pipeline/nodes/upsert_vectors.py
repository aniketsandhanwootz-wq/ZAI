from ..state import GraphState
from ...config import Settings
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool


def upsert_vectors(state: GraphState, config):
    settings: Settings = config["settings"]
    if not state.tenant_id or not state.checkin_id or not state.thread_snapshot_text:
        state.logs.append("Skipping vector upsert (missing tenant/checkin/text)")
        return state

    embedder = EmbedTool(settings)
    vector_db = VectorTool(settings)

    emb = embedder.embed_text(state.thread_snapshot_text)

    vector_db.upsert_incident_vector(
        tenant_id=state.tenant_id,
        checkin_id=state.checkin_id,
        vector_type="PROBLEM",
        embedding=emb,
        project_name=state.project_name,
        part_number=state.part_number,
        legacy_id=state.legacy_id,
        status=(state.checkin_row or {}).get("Status"),
        text=state.thread_snapshot_text,
    )
    state.logs.append("Upserted PROBLEM vector")
    return state
