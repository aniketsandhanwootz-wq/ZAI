from __future__ import annotations

from typing import Any, Dict, List

from ...config import Settings
from ..lc_runtime import lc_registry, lc_invoke


def _drop_self(rows: List[Dict[str, Any]], self_checkin_id: str) -> List[Dict[str, Any]]:
    if not rows:
        return []
    sid = (self_checkin_id or "").strip()
    if not sid:
        return rows
    return [r for r in rows if str(r.get("checkin_id") or "").strip() != sid]


def retrieve_context(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    reg = lc_registry(settings, state)

    tenant_id = (state.get("tenant_id") or "").strip()
    text = (state.get("thread_snapshot_text") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()

    if not tenant_id or not text:
        state.setdefault("logs", []).append("Skipping retrieval (missing tenant/text)")
        state["similar_problems"] = []
        state["similar_resolutions"] = []
        state["similar_media"] = []
        state["relevant_ccp_chunks"] = []
        state["relevant_dashboard_updates"] = []
        state["relevant_glide_kb_chunks"] = []
        state["company_profile_matches"] = []
        state["company_profile_text"] = ""
        return state

    q = lc_invoke(reg, "embed_query", {"text": text}, state, fatal=True)

    project_name = state.get("project_name")
    part_number = state.get("part_number")
    legacy_id = state.get("legacy_id")

    # Company profile retrieval (future) - keep placeholders until you add wrappers
    state["company_profile_matches"] = []
    state["company_profile_text"] = ""

    # Glide KB chunks
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

    critical = lc_invoke(
        reg,
        "vector_search_glide_kb_chunks",
        {
            "tenant_id": tenant_id,
            "query_embedding": q,
            "top_k": 36,
            "project_name": project_name,
            "part_number": part_number,
            "legacy_id": legacy_id,
            "table_names": critical_tables,
        },
        state,
        default=[],
    ) or []

    general = lc_invoke(
        reg,
        "vector_search_glide_kb_chunks",
        {
            "tenant_id": tenant_id,
            "query_embedding": q,
            "top_k": 40,
            "project_name": project_name,
            "part_number": part_number,
            "legacy_id": legacy_id,
            "table_names": None,
        },
        state,
        default=[],
    ) or []

    state["relevant_glide_kb_chunks"] = _dedup_chunks((critical or []) + (general or []))

    problems = lc_invoke(
        reg,
        "vector_search_incidents",
        {
            "tenant_id": tenant_id,
            "query_embedding": q,
            "top_k": 60,
            "project_name": project_name,
            "part_number": part_number,
            "legacy_id": legacy_id,
            "vector_type": "PROBLEM",
        },
        state,
        default=[],
    ) or []

    resolutions = lc_invoke(
        reg,
        "vector_search_incidents",
        {
            "tenant_id": tenant_id,
            "query_embedding": q,
            "top_k": 60,
            "project_name": project_name,
            "part_number": part_number,
            "legacy_id": legacy_id,
            "vector_type": "RESOLUTION",
        },
        state,
        default=[],
    ) or []

    media = lc_invoke(
        reg,
        "vector_search_incidents",
        {
            "tenant_id": tenant_id,
            "query_embedding": q,
            "top_k": 60,
            "project_name": project_name,
            "part_number": part_number,
            "legacy_id": legacy_id,
            "vector_type": "MEDIA",
        },
        state,
        default=[],
    ) or []

    problems = _drop_self(problems, checkin_id)
    resolutions = _drop_self(resolutions, checkin_id)
    media = _drop_self(media, checkin_id)

    ccp = lc_invoke(
        reg,
        "vector_search_ccp_chunks",
        {
            "tenant_id": tenant_id,
            "query_embedding": q,
            "top_k": 60,
            "project_name": project_name,
            "part_number": part_number,
            "legacy_id": legacy_id,
        },
        state,
        default=[],
    ) or []

    dash = lc_invoke(
        reg,
        "vector_search_dashboard_updates",
        {
            "tenant_id": tenant_id,
            "query_embedding": q,
            "top_k": 20,
            "project_name": project_name,
            "part_number": part_number,
            "legacy_id": legacy_id,
        },
        state,
        default=[],
    ) or []

    state["similar_problems"] = problems
    state["similar_resolutions"] = resolutions
    state["similar_media"] = media
    state["relevant_ccp_chunks"] = ccp
    state["relevant_dashboard_updates"] = dash

    state["similar_incidents"] = problems[:10]

    state.setdefault("logs", []).append(
        f"Retrieved context: problems={len(problems)} resolutions={len(resolutions)} media={len(media)} ccp={len(ccp)} dash={len(dash)}"
    )
    return state