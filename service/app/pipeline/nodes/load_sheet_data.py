from ..state import GraphState
from ...config import Settings
from ...tools.sheets_tool import SheetsTool


def load_sheet_data(state: GraphState, config):
    settings: Settings = config["settings"]
    sheets = SheetsTool(settings)

    ev = state.event
    state.event_type = ev.get("event_type", "")

    state.checkin_id = ev.get("checkin_id")
    state.conversation_id = ev.get("conversation_id")
    state.ccp_id = ev.get("ccp_id")
    state.legacy_id = ev.get("legacy_id")

    # Load rows depending on event type
    if state.checkin_id:
        state.checkin_row = sheets.get_checkin_by_id(state.checkin_id)
        if state.checkin_row:
            state.project_name = state.checkin_row.get("Project Name")
            state.part_number = state.checkin_row.get("Part Number")
            state.legacy_id = state.checkin_row.get("ID") or state.legacy_id

    # Project row needed for tenant_id (Company Row id)
    if state.project_name and state.part_number and state.legacy_id:
        state.project_row = sheets.get_project_row(state.project_name, state.part_number, state.legacy_id)
        if state.project_row:
            state.tenant_id = state.project_row.get("Company Row id")

    # Conversation history
    if state.checkin_id:
        state.conversation_rows = sheets.get_conversations_for_checkin(state.checkin_id)

    state.logs.append("Loaded sheet data")
    return state
