# service/app/pipeline/nodes/upsert_vectors.py
from __future__ import annotations

from typing import Any, Dict

from ...config import Settings
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool


def upsert_vectors(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    tenant_id = (state.get("tenant_id") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()
    text = (state.get("thread_snapshot_text") or "").strip()

    if not tenant_id or not checkin_id or not text:
        (state.get("logs") or []).append("Skipping vector upsert (missing tenant/checkin/text)")
        return state

    embedder = EmbedTool(settings)
    vector_db = VectorTool(settings)

    emb = embedder.embed_text(text)  # document embedding

    vector_db.upsert_incident_vector(
        tenant_id=tenant_id,
        checkin_id=checkin_id,
        vector_type="PROBLEM",
        embedding=emb,
        project_name=state.get("project_name"),
        part_number=state.get("part_number"),
        legacy_id=state.get("legacy_id"),
        status=state.get("checkin_status") or None,
        text=text,
    )

    (state.get("logs") or []).append("Upserted incident vector (PROBLEM)")
    return state
