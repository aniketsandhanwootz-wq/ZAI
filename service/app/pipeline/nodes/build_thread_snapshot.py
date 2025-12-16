from ..state import GraphState


def build_thread_snapshot(state: GraphState, config):
    # Build a compact snapshot used for embedding + LLM query.
    desc = (state.checkin_row or {}).get("Description", "") if state.checkin_row else ""
    status = (state.checkin_row or {}).get("Status", "") if state.checkin_row else ""

    recent_remarks = []
    for r in (state.conversation_rows or [])[-5:]:
        txt = (r.get("Remark") or "").strip()
        if txt:
            recent_remarks.append(txt)

    header = f"Project: {state.project_name or ''} | Part: {state.part_number or ''} | Status: {status}"
    body = f"Checkin description: {desc}".strip()
    convo = "Recent conversation:\n- " + "\n- ".join(recent_remarks) if recent_remarks else "Recent conversation: (none)"

    state.thread_snapshot_text = f"{header}\n{body}\n{convo}".strip()
    state.logs.append("Built thread snapshot text")
    return state
