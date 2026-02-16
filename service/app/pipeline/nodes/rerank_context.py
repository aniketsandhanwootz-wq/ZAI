from __future__ import annotations

from typing import Any, Dict, List, Tuple
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


def _compose_query_text_for_rerank(state: Dict[str, Any]) -> str:
    """
    Match retrieve_context query behavior so rerank aligns with retrieval.
    Prefer retrieval_query_text if present.
    """
    rq = (state.get("retrieval_query_text") or "").strip()
    if rq:
        return rq

    snap = (state.get("thread_snapshot_text") or "").strip()
    att = (state.get("attachment_context") or "").strip()
    if att and len(att) > 4000:
        att = att[:4000] + "\n[TRUNCATED]"
    if att:
        return (snap + "\n\nATTACHMENT EVIDENCE:\n" + att).strip()
    return snap


def _safe_str(x: Any, max_len: int = 420) -> str:
    s = str(x or "").strip()
    if len(s) > max_len:
        return s[:max_len] + "…"
    return s


def _build_evidence_index(state: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str]:
    """
    Build a cite-able evidence index with stable IDs.
    IDs are short tokens intended for:
      - LLM "citations" output
      - Edge-tab references later

    Types:
      A# = Attachment from Files column
      R# = Past Resolution memory
      P# = Similar Problem memory
      M# = Similar Media caption memory
      C# = CCP chunk
      D# = Dashboard update
      G# = Glide KB chunk
    """
    ev: List[Dict[str, Any]] = []
    lines: List[str] = []
    lines.append("EVIDENCE INDEX (cite using evidence_id tokens like A1, R2, C1):")

    # A) Attachments (already summarized by analyze_attachments)
    attachments = state.get("attachments_analyzed") or []
    if isinstance(attachments, list) and attachments:
        a_count = 0
        for it in attachments:
            if not isinstance(it, dict):
                continue
            ok = bool(it.get("ok") is True)
            if not ok:
                continue
            a_count += 1
            eid = f"A{a_count}"
            filename = _safe_str(it.get("filename") or "file")
            doc_type = _safe_str(it.get("doc_type") or "")
            # locator is filename-based for now; later we can map to DB ids/page refs
            locator = f"files::{filename}"
            ev.append(
                {
                    "evidence_id": eid,
                    "type": "attachment",
                    "locator": locator,
                    "filename": filename,
                    "doc_type": doc_type,
                }
            )
            lines.append(f"- {eid} (attachment) {filename} | {doc_type} | locator={locator}")

    # Helper to add vector/context items
    def add_list(prefix: str, typ: str, items: List[Dict[str, Any]], text_key: str, extra: Dict[str, str] | None = None, limit: int = 6):
        n = 0
        for it in (items or [])[:limit]:
            if not isinstance(it, dict):
                continue
            n += 1
            eid = f"{prefix}{n}"
            text = _safe_str(it.get(text_key) or "")
            locator_bits = []
            for k in ("checkin_id", "legacy_id", "ccp_id", "dashboard_row_id", "row_id", "item_id", "chunk_index", "table_name"):
                v = str(it.get(k) or "").strip()
                if v:
                    locator_bits.append(f"{k}={v}")
            locator = typ + "::" + ",".join(locator_bits) if locator_bits else typ
            rec: Dict[str, Any] = {"evidence_id": eid, "type": typ, "locator": locator, "text": text}
            if extra:
                rec.update(extra)
            ev.append(rec)
            lines.append(f"- {eid} ({typ}) locator={locator} :: {text}")

    add_list("R", "resolution", state.get("similar_resolutions") or [], "summary", limit=6)
    add_list("M", "media", state.get("similar_media") or [], "summary", limit=6)
    add_list("P", "problem", state.get("similar_problems") or [], "summary", limit=8)
    add_list("C", "ccp", state.get("relevant_ccp_chunks") or [], "text", limit=8)
    add_list("D", "dashboard", state.get("relevant_dashboard_updates") or [], "update_message", limit=6)
    add_list("G", "glide_kb", state.get("relevant_glide_kb_chunks") or [], "text", limit=10)

    return ev, "\n".join(lines).strip()


def rerank_context(settings, state: Dict[str, Any]) -> Dict[str, Any]:
    q = _compose_query_text_for_rerank(state)

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

    # Build evidence index (stable cite tokens)
    evidence_index, evidence_text = _build_evidence_index(state)
    state["evidence_index"] = evidence_index
    state["evidence_index_text"] = evidence_text

    # Existing packed_context (keep it), but make it clearly source-typed.
    packed: List[str] = []
    packed.append(evidence_text)

    if resolutions_r:
        packed.append("\nRESOLUTIONS (what actually closed similar issues):")
        for i, it in enumerate(resolutions_r[:4], start=1):
            packed.append(f"- R{i}: {_safe_str((it.get('summary') or '').strip(), 520)}")

    if media_r:
        packed.append("\nSIMILAR MEDIA EVIDENCE (past photo captions):")
        for i, it in enumerate(media_r[:4], start=1):
            packed.append(f"- M{i}: {_safe_str((it.get('summary') or '').strip(), 520)}")

    if glide_r:
        crit = [x for x in glide_r if (x.get("table_name") or "").strip().lower() in ("raw_material", "processes", "boughtouts")]
        other = [x for x in glide_r if x not in crit]

        if crit:
            packed.append("\nSHOPFLOOR MASTER DATA (RawMaterial/Processes/Boughtouts):")
            for i, it in enumerate(crit[:10], start=1):
                tn = (it.get("table_name") or "").strip()
                title = (it.get("title") or "").strip()
                txt = (it.get("text") or "").strip()
                head = f"{tn}: {title}".strip(": ").strip() if title else tn
                packed.append(f"- G{i}: {head} — {_safe_str(txt, 520)}")

        if other:
            packed.append("\nGLIDE KB (other relevant notes):")
            for i, it in enumerate(other[:6], start=1):
                title = (it.get("title") or "").strip()
                txt = (it.get("text") or "").strip()
                if title:
                    packed.append(f"- G{i}: {_safe_str(title, 160)} — {_safe_str(txt, 520)}")
                else:
                    packed.append(f"- G{i}: {_safe_str(txt, 520)}")

    if problems_r:
        packed.append("\nSIMILAR PROBLEMS (symptoms + conditions):")
        for i, it in enumerate(problems_r[:6], start=1):
            packed.append(f"- P{i}: {_safe_str((it.get('summary') or '').strip(), 520)}")

    if ccp_r:
        packed.append("\nCCP GUIDANCE (process rules / checks):")
        for i, it in enumerate(ccp_r[:6], start=1):
            name = (it.get("ccp_name") or "").strip()
            t = (it.get("text") or "").strip()
            if name:
                packed.append(f"- C{i}: {_safe_str(name, 160)} — {_safe_str(t, 520)}")
            else:
                packed.append(f"- C{i}: {_safe_str(t, 520)}")

    if dash_r:
        packed.append("\nPROJECT UPDATES (recent constraints / priorities):")
        for i, it in enumerate(dash_r[:4], start=1):
            packed.append(f"- D{i}: {_safe_str((it.get('update_message') or '').strip(), 520)}")

    state["packed_context"] = "\n".join([x for x in packed if x.strip()]).strip()
    (state.get("logs") or []).append("Reranked + built evidence_index (cite tokens A/R/P/M/C/D/G)")
    return state