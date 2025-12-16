from __future__ import annotations

from typing import Dict, Any, List

from ...config import Settings
from ...tools.sheets_tool import SheetsTool
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool


def _build_snapshot(project_name: str, part_number: str, status: str, desc: str, convos: List[Dict[str, Any]]) -> str:
    recent = []
    for c in convos[-5:]:
        t = (c.get("Remark") or "").strip()
        if t:
            recent.append(t)
    header = f"Project: {project_name} | Part: {part_number} | Status: {status}"
    convo = "Recent conversation:\n- " + "\n- ".join(recent) if recent else "Recent conversation: (none)"
    return f"{header}\nCheckin description: {desc}\n{convo}".strip()


def ingest_history(settings: Settings, limit: int = 500) -> Dict[str, Any]:
    sheets = SheetsTool(settings)
    embedder = EmbedTool(settings)
    vec = VectorTool(settings)

    checkins = sheets.list_checkins()
    checkins = [c for c in checkins if str(c.get("CheckIN ID", "")).strip()]
    checkins = checkins[:limit]

    done = 0
    skipped = 0

    for c in checkins:
        checkin_id = str(c.get("CheckIN ID", "")).strip()
        project_name = (c.get("Project Name") or "").strip()
        part_number = (c.get("Part Number") or "").strip()
        legacy_id = str(c.get("ID", "")).strip()
        status = (c.get("Status") or "").strip()
        desc = (c.get("Description") or "").strip()

        if not (project_name and part_number and legacy_id):
            skipped += 1
            continue

        proj = sheets.get_project_row(project_name, part_number, legacy_id)
        tenant_id = (proj or {}).get("Company Row id", "")
        if not tenant_id:
            skipped += 1
            continue

        convos = sheets.get_conversations_for_checkin(checkin_id)
        snapshot = _build_snapshot(project_name, part_number, status, desc, convos)

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

        # Optional MVP: if status is PASS/FAIL we treat as "resolved-like" and also store RESOLUTION vector.
        if status.upper() in ("PASS", "FAIL"):
            vec.upsert_incident_vector(
                tenant_id=tenant_id,
                checkin_id=checkin_id,
                vector_type="RESOLUTION",
                embedding=emb,
                project_name=project_name,
                part_number=part_number,
                legacy_id=legacy_id,
                status=status,
                text=f"Resolution snapshot:\n{snapshot}",
            )

        done += 1

    return {"source": "history", "threads_embedded": done, "skipped": skipped, "limit": limit}
