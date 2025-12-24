from __future__ import annotations

from typing import Any, Dict
from fastapi import APIRouter, Request

from ..config import Settings
from ..integrations.teams_client import TeamsClient

router = APIRouter(prefix="/integrations/teams", tags=["integrations"])


def _get_settings(request: Request) -> Settings:
    return request.app.state.settings  # type: ignore


@router.post("/test")
def teams_test(request: Request, payload: Dict[str, Any]):
    s = _get_settings(request)
    client = TeamsClient(getattr(s, "teams_webhook_url", ""))
    if not client.enabled():
        return {"ok": False, "error": "TEAMS_WEBHOOK_URL not set"}
    resp = client.post_message(payload)
    return {"ok": True, "response": resp}