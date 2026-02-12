from __future__ import annotations

from typing import Any, Dict
from fastapi import APIRouter, Request

from ..config import Settings
from ..pipeline.lc_runtime import lc_registry, lc_invoke

router = APIRouter(prefix="/integrations/teams", tags=["integrations"])


def _get_settings(request: Request) -> Settings:
    return request.app.state.settings  # type: ignore


@router.post("/test")
def teams_test(request: Request, payload: Dict[str, Any]):
    s = _get_settings(request)
    state: Dict[str, Any] = {"logs": []}
    reg = lc_registry(s, state)

    resp = lc_invoke(
        reg,
        "teams_post_message",
        {"payload": payload or {}, "webhook_url": ""},
        state,
        fatal=False,
        default={"sent": False, "reason": "tool failed"},
    )

    return {"ok": True, "tool_response": resp, "logs": state.get("logs", [])}