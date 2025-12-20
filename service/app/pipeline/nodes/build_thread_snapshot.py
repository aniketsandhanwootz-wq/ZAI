# service/app/pipeline/nodes/build_thread_snapshot.py
from __future__ import annotations

from typing import Any, Dict, List

from ...tools.sheets_tool import _norm_value


def build_thread_snapshot(settings, state: Dict[str, Any]) -> Dict[str, Any]:
    project = state.get("project_name") or ""
    part = state.get("part_number") or ""
    status = state.get("checkin_status") or ""
    desc = state.get("checkin_description") or ""

    convos: List[Dict[str, Any]] = state.get("conversation_rows") or []
    recent_remarks: List[str] = []
    for r in convos[-10:]:
        # Conversation keys are casefolded in SheetsTool
        remark = _norm_value(r.get("remarks", "")) or _norm_value(r.get("remark", ""))
        st = _norm_value(r.get("status", ""))
        if remark:
            recent_remarks.append(f"[{st}] {remark}".strip() if st else remark)

    header = f"Project: {project} | Part: {part} | Status: {status}".strip()
    body = f"Description: {desc}".strip() if desc else "Description: (empty)"
    convo = "Recent conversation:\n- " + "\n- ".join(recent_remarks) if recent_remarks else "Recent conversation: (none)"

    snapshot = f"{header}\n{body}\n{convo}".strip()
    state["thread_snapshot_text"] = snapshot

    (state.get("logs") or []).append("Built thread snapshot")
    return state
