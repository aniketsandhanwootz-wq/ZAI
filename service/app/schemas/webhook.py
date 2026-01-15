from __future__ import annotations

from typing import Optional, Literal, Dict, Any
from pydantic import BaseModel


class WebhookPayload(BaseModel):
    event_type: Literal[
        "CHECKIN_CREATED",
        "CHECKIN_UPDATED",
        "CONVERSATION_ADDED",
        "CCP_UPDATED",
        "DASHBOARD_UPDATED",
        "PROJECT_UPDATED",   # NEW
        "MANUAL_TRIGGER",
    ]

    # Used by PROJECT_UPDATED and also helpful across flows
    legacy_id: Optional[str] = None

    checkin_id: Optional[str] = None
    conversation_id: Optional[str] = None
    ccp_id: Optional[str] = None

    # Dashboard Updates unique identity (Row ID in the sheet)
    dashboard_update_id: Optional[str] = None

    # Backward-compat aliases (if some webhook sender uses these keys)
    dashboard_row_id: Optional[str] = None
    row_id: Optional[str] = None

    meta: Optional[Dict[str, Any]] = None