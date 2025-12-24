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
        "MANUAL_TRIGGER",
    ]
    checkin_id: Optional[str] = None
    conversation_id: Optional[str] = None
    ccp_id: Optional[str] = None
    legacy_id: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None