from __future__ import annotations

from typing import Any, Dict

from ...config import Settings
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool


def upsert_vectors(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    tenant_id = (state.get("tenant_id") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()
    problem_text = (state.get("thread_snapshot_text") or "").strip()
    closure_notes = (state.get("closure_notes") or "").strip()

    if not tenant_id or not checkin_id or not problem_text:
        (state.get("logs") or []).append("Skipping vector upsert (missing tenant/checkin/text)")
        return state

    embedder = EmbedTool(settings)
    vector_db = VectorTool(settings)

    # --- PROBLEM vector ---
    emb_problem = embedder.embed_text(problem_text)
    vector_db.upsert_incident_vector(
        tenant_id=tenant_id,
        checkin_id=checkin_id,
        vector_type="PROBLEM",
        embedding=emb_problem,
        project_name=state.get("project_name"),
        part_number=state.get("part_number"),
        legacy_id=state.get("legacy_id"),
        status=state.get("checkin_status") or "",
        text=problem_text,
    )
    (state.get("logs") or []).append("Upserted PROBLEM vector")

    # --- RESOLUTION vector ---
    if closure_notes:
        resolution_text = (
            f"{state.get('project_name') or ''} | {state.get('part_number') or ''} | CHECKIN {checkin_id}\n"
            f"RESOLUTION / WHAT WORKED (from conversation):\n{closure_notes}".strip()
        )
        emb_res = embedder.embed_text(resolution_text)
        vector_db.upsert_incident_vector(
            tenant_id=tenant_id,
            checkin_id=checkin_id,
            vector_type="RESOLUTION",
            embedding=emb_res,
            project_name=state.get("project_name"),
            part_number=state.get("part_number"),
            legacy_id=state.get("legacy_id"),
            status=state.get("checkin_status") or "",
            text=resolution_text,
        )
        (state.get("logs") or []).append("Upserted RESOLUTION vector (closure memory)")

    # --- MEDIA vector (captions) ---
    caps = state.get("image_captions") or []
    if isinstance(caps, list) and caps:
        media_text = "MEDIA CAPTIONS:\n" + "\n".join([f"- {str(x).strip()}" for x in caps if str(x).strip()])
        if media_text.strip():
            emb_media = embedder.embed_text(media_text)
            vector_db.upsert_incident_vector(
                tenant_id=tenant_id,
                checkin_id=checkin_id,
                vector_type="MEDIA",
                embedding=emb_media,
                project_name=state.get("project_name"),
                part_number=state.get("part_number"),
                legacy_id=state.get("legacy_id"),
                status=state.get("checkin_status") or "",
                text=media_text,
            )
            (state.get("logs") or []).append("Upserted MEDIA vector (captions)")

    return state