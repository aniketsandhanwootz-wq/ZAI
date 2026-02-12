from __future__ import annotations

from typing import Any, Dict, List

from ...config import Settings
from ..lc_runtime import lc_registry, lc_invoke


def _clean_lines(items: List[Any], *, max_items: int) -> List[str]:
    out: List[str] = []
    for x in items or []:
        s = str(x or "").strip()
        if not s:
            continue
        out.append(s)
        if len(out) >= max_items:
            break
    return out


def upsert_vectors(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    reg = lc_registry(settings, state)

    tenant_id = (state.get("tenant_id") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()
    problem_text = (state.get("thread_snapshot_text") or "").strip()
    closure_notes = (state.get("closure_notes") or "").strip()

    if not tenant_id or not checkin_id or not problem_text:
        state.setdefault("logs", []).append("Skipping vector upsert (missing tenant/checkin/text)")
        return state

    project_name = state.get("project_name")
    part_number = state.get("part_number")
    legacy_id = state.get("legacy_id")
    status = state.get("checkin_status") or ""

    emb_problem = lc_invoke(reg, "embed_text", {"text": problem_text}, state, fatal=True)
    lc_invoke(
        reg,
        "vector_upsert_incident_vector",
        {
            "tenant_id": tenant_id,
            "checkin_id": checkin_id,
            "vector_type": "PROBLEM",
            "embedding": emb_problem,
            "project_name": project_name,
            "part_number": part_number,
            "legacy_id": legacy_id,
            "status": status,
            "text": problem_text,
        },
        state,
        fatal=False,
    )
    state.setdefault("logs", []).append("Upserted PROBLEM vector")

    if closure_notes:
        resolution_text = (
            f"{project_name or ''} | {part_number or ''} | CHECKIN {checkin_id}\n"
            f"RESOLUTION / WHAT WORKED (from conversation):\n{closure_notes}"
        ).strip()
        emb_res = lc_invoke(reg, "embed_text", {"text": resolution_text}, state, fatal=True)
        lc_invoke(
            reg,
            "vector_upsert_incident_vector",
            {
                "tenant_id": tenant_id,
                "checkin_id": checkin_id,
                "vector_type": "RESOLUTION",
                "embedding": emb_res,
                "project_name": project_name,
                "part_number": part_number,
                "legacy_id": legacy_id,
                "status": status,
                "text": resolution_text,
            },
            state,
            fatal=False,
        )
        state.setdefault("logs", []).append("Upserted RESOLUTION vector (closure memory)")

    caps = state.get("image_captions") or []
    cap_lines = _clean_lines(caps, max_items=12)

    if cap_lines:
        media_text = "MEDIA CAPTIONS (from inspection photos):\n" + "\n".join([f"- {c}" for c in cap_lines])
        emb_media = lc_invoke(reg, "embed_text", {"text": media_text}, state, fatal=True)
        lc_invoke(
            reg,
            "vector_upsert_incident_vector",
            {
                "tenant_id": tenant_id,
                "checkin_id": checkin_id,
                "vector_type": "MEDIA",
                "embedding": emb_media,
                "project_name": project_name,
                "part_number": part_number,
                "legacy_id": legacy_id,
                "status": status,
                "text": media_text,
            },
            state,
            fatal=False,
        )
        state.setdefault("logs", []).append(f"Upserted MEDIA vector (captions={len(cap_lines)})")

    return state