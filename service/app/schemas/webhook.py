from __future__ import annotations

from typing import Optional, Literal, Dict, Any
from pydantic import BaseModel


class WebhookPayload(BaseModel):
    event_type: Literal[
        "CHECKIN_CREATED",
        "CHECKIN_UPDATED",
        "CHECKIN_DELETED",       # NEW — wootzcheckin only (soft-delete)
        "CONVERSATION_ADDED",
        "CONVERSATION_UPDATED",  # NEW — wootzcheckin only (edit)
        "CONVERSATION_DELETED",  # NEW — wootzcheckin only (soft-delete)
        "CCP_CREATED",           # NEW — wootzcheckin only
        "CCP_UPDATED",
        "CCP_DELETED",           # NEW — wootzcheckin only (hard delete)
        "DASHBOARD_UPDATED",
        "PROJECT_UPDATED",   # NEW
        "MANUAL_TRIGGER",
    ]

    # Which system this event originated from — selects the loader node in
    # graph.py (load_wootz_data vs load_sheet_data). Defaults to "sheets" so
    # the existing Apps Script webhook payloads (which never send this field)
    # keep working unchanged.
    source: Literal["sheets", "wootzcheckin"] = "sheets"

    # Used by PROJECT_UPDATED and also helpful across flows
    legacy_id: Optional[str] = None

    checkin_id: Optional[str] = None
    conversation_id: Optional[str] = None
    ccp_id: Optional[str] = None

    # Dashboard Updates unique identity (canonical: Dashboard Update ID)
    dashboard_update_id: Optional[str] = None

    # Backward-compat aliases (if some webhook sender uses these keys)
    dashboard_row_id: Optional[str] = None
    row_id: Optional[str] = None

    meta: Optional[Dict[str, Any]] = None