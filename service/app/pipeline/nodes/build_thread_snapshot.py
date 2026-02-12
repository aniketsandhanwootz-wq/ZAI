# service/app/pipeline/nodes/build_tread_snapshot.py
from __future__ import annotations

from typing import Dict, List
import re

from ...tools.sheets_tool import _norm_value


_CLOSURE_HINTS = (
    "resolved", "fixed", "closed", "rework", "reworked", "replaced", "changed",
    "offset", "tool", "fixture", "grind", "surface grind", "heat treat",
    "stress relieve", "polish", "deburr", "scrap", "accepted", "ok now", "passed"
)

_EVIDENCE_HINTS = ("cmm", "mic", "micrometer", "vernier", "gauge", "inspection", "measured", "reading", "flatness", "runout", "photo")


def _looks_like_closure_line(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    return any(h in t for h in _CLOSURE_HINTS)

def _looks_like_evidence_line(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    return any(h in t for h in _EVIDENCE_HINTS)

def _extract_closure_notes(convos: List[Dict[str, str]]) -> str:
    """
    Heuristic, factual extraction: picks actionable closure-like remarks from recent conversation.
    No guessing/spec invention.
    """
    lines: List[str] = []
    recent = convos[-20:] if convos else []

    for r in reversed(recent):
        remark = _norm_value(r.get("remarks", "")) or _norm_value(r.get("remark", ""))
        st = _norm_value(r.get("status", ""))
        if not remark:
            continue

        is_passish = st.strip().upper() in ("PASS", "OK", "CLOSED", "DONE", "RESOLVED")
        if is_passish or _looks_like_closure_line(remark) or _looks_like_evidence_line(remark):
            tag = f"[{st}] " if st else ""
            lines.append(f"{tag}{remark}".strip())

        if len(lines) >= 6:
            break

    lines.reverse()
    if not lines:
        return ""

    bullets = "\n- " + "\n- ".join(lines)
    return f"Closure notes (from conversation):{bullets}".strip()

def build_thread_snapshot(settings, state: Dict[str, any]) -> Dict[str, any]:
    project = state.get("project_name") or ""
    part = state.get("part_number") or ""
    status = state.get("checkin_status") or ""
    desc = state.get("checkin_description") or ""

    convos: List[Dict[str, any]] = state.get("conversation_rows") or []
    recent_remarks: List[str] = []
    for r in convos[-10:]:
        remark = _norm_value(r.get("remarks", "")) or _norm_value(r.get("remark", ""))
        st = _norm_value(r.get("status", ""))
        if remark:
            recent_remarks.append(f"[{st}] {remark}".strip() if st else remark)

    header = f"Project: {project} | Part: {part} | Status: {status}".strip()
    body = f"Description: {desc}".strip() if desc else "Description: (empty)"
    convo = (
        "Recent conversation:\n- " + "\n- ".join(recent_remarks)
        if recent_remarks else
        "Recent conversation: (none)"
    )

    snapshot = f"{header}\n{body}\n{convo}".strip()
    state["thread_snapshot_text"] = snapshot
    state["closure_notes"] = _extract_closure_notes(convos)

    state.setdefault("logs", []).append("Built thread snapshot + closure_notes")
    return state