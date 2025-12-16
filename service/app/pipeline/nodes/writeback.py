from ..state import GraphState
from ...config import Settings
from ...tools.sheets_tool import SheetsTool


def writeback(state: GraphState, config):
    settings: Settings = config["settings"]
    if not state.ai_reply or not state.checkin_id:
        state.logs.append("Skipping writeback (missing ai_reply/checkin_id)")
        return state

    sheets = SheetsTool(settings)

    # Append AI comment into Conversation tab (recommended)
    sheets.append_conversation_ai_comment(
        checkin_id=state.checkin_id,
        remark=state.ai_reply,
        status=(state.checkin_row or {}).get("Status", ""),
        photos="",  # future: annotated image URL
    )

    state.writeback_done = True
    state.logs.append("Wrote back AI comment to Conversation")
    return state
