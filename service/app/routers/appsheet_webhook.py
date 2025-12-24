from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, Header, HTTPException, Request

from ..config import Settings
from ..queue import enqueue_job
from ..schemas.webhook import WebhookPayload

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


def _get_settings(request: Request) -> Settings:
    return request.app.state.settings  # type: ignore


@router.post("/appsheet")
def appsheet_webhook(
    request: Request,
    payload: WebhookPayload,
    x_appsheet_secret: Optional[str] = Header(default=None),
):
    settings = _get_settings(request)
    if x_appsheet_secret != settings.appsheet_webhook_secret:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    job_id = enqueue_job(settings, payload.model_dump())
    return {"ok": True, "job_id": job_id}