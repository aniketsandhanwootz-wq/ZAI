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


def rerank_context(settings, state: Dict[str, Any]) -> Dict[str, Any]:
    q = (state.get("thread_snapshot_text") or "").strip()

    inc: List[Dict[str, Any]] = state.get("similar_incidents") or []
    ccp: List[Dict[str, Any]] = state.get("relevant_ccp_chunks") or []

    # incidents: already sorted by distance asc. We'll adjust with token overlap.
    reranked_inc = []
    seen_checkin = set()
    for i, item in enumerate(inc):
        cid = (item.get("checkin_id") or "").strip()
        if cid and cid in seen_checkin:
            continue
        seen_checkin.add(cid)

        summary = (item.get("summary") or "").strip()
        base_rank = 1.0 / (1 + i)   # higher for top results
        overlap = _overlap_score(q, summary)
        score = 0.75 * base_rank + 0.25 * overlap
        item["_rerank_score"] = score
        reranked_inc.append(item)

    reranked_inc.sort(key=lambda x: float(x.get("_rerank_score", 0.0)), reverse=True)
    state["similar_incidents"] = reranked_inc[:10]

    # ccp: rerank similarly
    reranked_ccp = []
    seen_ccp = set()
    for i, item in enumerate(ccp):
        ccp_id = (item.get("ccp_id") or "").strip()
        if ccp_id and ccp_id in seen_ccp:
            continue
        seen_ccp.add(ccp_id)

        text = (item.get("text") or "").strip()
        base_rank = 1.0 / (1 + i)
        overlap = _overlap_score(q, text)
        score = 0.70 * base_rank + 0.30 * overlap
        item["_rerank_score"] = score
        reranked_ccp.append(item)

    reranked_ccp.sort(key=lambda x: float(x.get("_rerank_score", 0.0)), reverse=True)
    state["relevant_ccp_chunks"] = reranked_ccp[:10]

    (state.get("logs") or []).append("Reranked context (incidents + CCP)")
    return state
