from __future__ import annotations

from typing import Any, Dict, List
import re


def _tokens(text: str) -> set[str]:
    text = (text or "").lower()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    return {t for t in text.split() if len(t) >= 3}


def _overlap_score(query: str, doc: str) -> float:
    qt = _tokens(query)
    dt = _tokens(doc)
    if not qt or not dt:
        return 0.0
    return len(qt & dt) / max(1, len(qt))


def _cosine_sim_from_distance(distance: float) -> float:
    # pgvector cosine distance is typically in [0..2] depending on normalization.
    d = max(0.0, min(2.0, float(distance)))
    return 1.0 - (d / 2.0)


def _rerank_items(query: str, items: List[Dict[str, Any]], text_key: str, kind: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for i, it in enumerate(items or []):
        doc = (it.get(text_key) or "").strip()
        dist = float(it.get("distance", 1.0))
        sim = _cosine_sim_from_distance(dist)
        base_rank = 1.0 / (1 + i)
        overlap = _overlap_score(query, doc)

        bonus = 0.0
        if kind == "resolution":
            bonus += 0.05  # small nudge; not a blunt override

        score = (0.55 * sim) + (0.25 * overlap) + (0.20 * base_rank) + bonus
        it["_rerank_score"] = float(score)
        out.append(it)

    out.sort(key=lambda x: float(x.get("_rerank_score", 0.0)), reverse=True)
    return out


def rerank_context(settings, state: Dict[str, Any]) -> Dict[str, Any]:
    q = (state.get("thread_snapshot_text") or "").strip()

    problems: List[Dict[str, Any]] = state.get("similar_problems") or []
    resolutions: List[Dict[str, Any]] = state.get("similar_resolutions") or []
    ccp: List[Dict[str, Any]] = state.get("relevant_ccp_chunks") or []
    dash: List[Dict[str, Any]] = state.get("relevant_dashboard_updates") or []

    # De-dup by checkin_id for problems/resolutions
    def dedup_by(items: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
        seen = set()
        out = []
        for it in items:
            k = str(it.get(key) or "").strip()
            if not k:
                out.append(it)
                continue
            if k in seen:
                continue
            seen.add(k)
            out.append(it)
        return out

    problems = dedup_by(problems, "checkin_id")
    resolutions = dedup_by(resolutions, "checkin_id")

    problems_r = _rerank_items(q, problems, "summary", "problem")
    resolutions_r = _rerank_items(q, resolutions, "summary", "resolution")
    ccp_r = _rerank_items(q, ccp, "text", "ccp")
    dash_r = _rerank_items(q, dash, "update_message", "dash")

    # Keep top-N per bucket
    problems_r = problems_r[:10]
    resolutions_r = resolutions_r[:6]
    ccp_r = ccp_r[:10]
    dash_r = dash_r[:6]

    state["similar_problems"] = problems_r
    state["similar_resolutions"] = resolutions_r
    state["relevant_ccp_chunks"] = ccp_r
    state["relevant_dashboard_updates"] = dash_r

    # Pack final context with rule:
    # Prefer 2–4 resolutions when available, else rely on CCP.
    packed: List[str] = []

    if resolutions_r:
        packed.append("RESOLUTIONS (what actually closed similar issues):")
        for i, it in enumerate(resolutions_r[:4], start=1):
            packed.append(f"{i}. { (it.get('summary') or '').strip() }")

    if problems_r:
        packed.append("\nSIMILAR PROBLEMS (symptoms + conditions):")
        for i, it in enumerate(problems_r[:6], start=1):
            packed.append(f"{i}. { (it.get('summary') or '').strip() }")

    if ccp_r:
        packed.append("\nCCP GUIDANCE (process rules / known checks):")
        for i, it in enumerate(ccp_r[:6], start=1):
            name = (it.get("ccp_name") or "").strip()
            t = (it.get("text") or "").strip()
            line = f"{i}. {name}: {t}".strip() if name else f"{i}. {t}"
            packed.append(line)

    if dash_r:
        packed.append("\nPROJECT UPDATES (recent constraints / priorities):")
        for i, it in enumerate(dash_r[:4], start=1):
            packed.append(f"{i}. { (it.get('update_message') or '').strip() }")

    state["packed_context"] = "\n".join([x for x in packed if x.strip()]).strip()

    (state.get("logs") or []).append("Reranked + packed context (resolution-first)")
    return state
