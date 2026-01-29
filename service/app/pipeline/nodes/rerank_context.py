# service/app/pipeline/nodes/rerank_context.py
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
            bonus += 0.05

        # Bonus for critical Glide KB tables
        tname = (it.get("table_name") or "").strip().lower()
        if tname in ("raw_material", "processes", "boughtouts"):
            bonus += 0.10
        score = (0.55 * sim) + (0.25 * overlap) + (0.20 * base_rank) + bonus
        it["_rerank_score"] = float(score)
        out.append(it)

    out.sort(key=lambda x: float(x.get("_rerank_score", 0.0)), reverse=True)
    return out


def rerank_context(settings, state: Dict[str, Any]) -> Dict[str, Any]:
    q = (state.get("thread_snapshot_text") or "").strip()

    problems: List[Dict[str, Any]] = state.get("similar_problems") or []
    resolutions: List[Dict[str, Any]] = state.get("similar_resolutions") or []
    media: List[Dict[str, Any]] = state.get("similar_media") or []
    ccp: List[Dict[str, Any]] = state.get("relevant_ccp_chunks") or []
    dash: List[Dict[str, Any]] = state.get("relevant_dashboard_updates") or []
    glide_kb: List[Dict[str, Any]] = state.get("relevant_glide_kb_chunks") or []
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
    media = dedup_by(media, "checkin_id")

    problems_r = _rerank_items(q, problems, "summary", "problem")[:10]
    resolutions_r = _rerank_items(q, resolutions, "summary", "resolution")[:6]
    media_r = _rerank_items(q, media, "summary", "media")[:6]
    ccp_r = _rerank_items(q, ccp, "text", "ccp")[:10]
    dash_r = _rerank_items(q, dash, "update_message", "dash")[:6]
    glide_r = _rerank_items(q, glide_kb, "text", "glide")[:14]
    state["relevant_glide_kb_chunks"] = glide_r

    state["similar_problems"] = problems_r
    state["similar_resolutions"] = resolutions_r
    state["similar_media"] = media_r
    state["relevant_ccp_chunks"] = ccp_r
    state["relevant_dashboard_updates"] = dash_r

    packed: List[str] = []

    if resolutions_r:
        packed.append("RESOLUTIONS (what actually closed similar issues):")
        for i, it in enumerate(resolutions_r[:4], start=1):
            packed.append(f"{i}. {(it.get('summary') or '').strip()}")

    if media_r:
        packed.append("\nSIMILAR MEDIA EVIDENCE (past photo captions that match this issue):")
        for i, it in enumerate(media_r[:4], start=1):
            packed.append(f"{i}. {(it.get('summary') or '').strip()}")

    if glide_r:
        crit = [x for x in glide_r if (x.get("table_name") or "").strip().lower() in ("raw_material","processes","boughtouts")]
        other = [x for x in glide_r if x not in crit]

        if crit:
            packed.append("\nSHOPFLOOR MASTER DATA (RawMaterial/Processes/Boughtouts):")
            for i, it in enumerate(crit[:10], start=1):
                tn = (it.get("table_name") or "").strip()
                title = (it.get("title") or "").strip()
                txt = (it.get("text") or "").strip()
                head = f"{tn}: {title}".strip(": ").strip() if title else tn
                packed.append(f"{i}. {head} — {txt}".strip())

        if other:
            packed.append("\nGLIDE KB (other relevant notes):")
            for i, it in enumerate(other[:6], start=1):
                title = (it.get("title") or "").strip()
                txt = (it.get("text") or "").strip()
                packed.append(f"{i}. {title}: {txt}".strip() if title else f"{i}. {txt}")
    if problems_r:
        packed.append("\nSIMILAR PROBLEMS (symptoms + conditions):")
        for i, it in enumerate(problems_r[:6], start=1):
            packed.append(f"{i}. {(it.get('summary') or '').strip()}")

    if ccp_r:
        packed.append("\nCCP GUIDANCE (process rules / known checks):")
        for i, it in enumerate(ccp_r[:6], start=1):
            name = (it.get("ccp_name") or "").strip()
            t = (it.get("text") or "").strip()
            packed.append(f"{i}. {name}: {t}".strip() if name else f"{i}. {t}")

    if dash_r:
        packed.append("\nPROJECT UPDATES (recent constraints / priorities):")
        for i, it in enumerate(dash_r[:4], start=1):
            packed.append(f"{i}. {(it.get('update_message') or '').strip()}")

    state["packed_context"] = "\n".join([x for x in packed if x.strip()]).strip()
    (state.get("logs") or []).append("Reranked + packed context (resolution-first + media-evidence)")
    return state
