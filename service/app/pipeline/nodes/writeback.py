from __future__ import annotations

from typing import Any, Dict

from ...config import Settings
from ...tools.sheets_tool import SheetsTool
from ...integrations.teams_client import TeamsClient


def writeback(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    reply = (state.get("ai_reply") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()

    if not reply or not checkin_id:
        (state.get("logs") or []).append("Skipping writeback (missing ai_reply/checkin_id)")
        return state

    annotated_urls = state.get("annotated_image_urls") or []
    photos_cell = ""
    if isinstance(annotated_urls, list) and annotated_urls:
        # AppSheet cells often accept newline-separated URLs
        photos_cell = "\n".join([str(u).strip() for u in annotated_urls[:3] if str(u).strip()])

        # also add into remark (so user sees it even if Photo column UI hides)
        reply = reply + "\n\nAnnotated images:\n" + "\n".join([f"- {u}" for u in annotated_urls[:3]])

    sheets = SheetsTool(settings)
    sheets.append_conversation_ai_comment(
        checkin_id=checkin_id,
        remark=reply,
        status=state.get("checkin_status") or "",
        photos=photos_cell,
    )

    state["writeback_done"] = True
    (state.get("logs") or []).append("Wrote back AI comment to Conversation")

    # Teams post (only for new checkins)
    if (state.get("event_type") or "") == "CHECKIN_CREATED":
        try:
            client = TeamsClient(getattr(settings, "teams_webhook_url", ""))
            if client.enabled():
                payload = {
                    "type": "checkin_ai_reply",
                    "checkin_id": checkin_id,
                    "project_name": state.get("project_name"),
                    "part_number": state.get("part_number"),
                    "status": state.get("checkin_status"),
                    "ai_reply": state.get("ai_reply"),
                    "annotated_images": annotated_urls[:3],
                }
                client.post_message(payload)
                (state.get("logs") or []).append("Posted summary to Teams")
        except Exception as e:
            (state.get("logs") or []).append(f"Teams post failed: {e}")

    return state