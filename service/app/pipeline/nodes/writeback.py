# service/app/pipeline/nodes/writeback.py
from __future__ import annotations

from typing import Any, Dict

from ...config import Settings
from ...tools.sheets_tool import SheetsTool


def writeback(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    reply = (state.get("ai_reply") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()

    if not reply or not checkin_id:
        (state.get("logs") or []).append("Skipping writeback (missing ai_reply/checkin_id)")
        return state

    sheets = SheetsTool(settings)

    sheets.append_conversation_ai_comment(
        checkin_id=checkin_id,
        remark=reply,
        status=state.get("checkin_status") or "",
        photos="",
    )

    state["writeback_done"] = True
    (state.get("logs") or []).append("Wrote back AI comment to Conversation")
    return state
