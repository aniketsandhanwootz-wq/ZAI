from __future__ import annotations

from typing import Optional
from fastapi import APIRouter, Header, HTTPException, Request

from ..config import Settings
from ..queue import enqueue_job
from ..schemas.webhook import WebhookPayload

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


def _get_settings(request: Request) -> Settings:
    return request.app.state.settings  # type: ignore


def _require_secret(settings: Settings, provided: Optional[str]) -> None:
    if (provided or "") != (settings.appsheet_webhook_secret or ""):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


def _enqueue(settings: Settings, payload: WebhookPayload) -> dict:
    job_id = enqueue_job(settings, payload.model_dump())
    return {"ok": True, "job_id": job_id}


@router.post("/appsheet")
def appsheet_webhook(
    request: Request,
    payload: WebhookPayload,
    x_appsheet_secret: Optional[str] = Header(default=None),
):
    settings = _get_settings(request)
    _require_secret(settings, x_appsheet_secret)
    return _enqueue(settings, payload)


# ✅ New: Google Sheets trigger endpoint
@router.post("/sheets")
def sheets_webhook(
    request: Request,
    payload: WebhookPayload,
    x_sheets_secret: Optional[str] = Header(default=None),
):
    settings = _get_settings(request)
    _require_secret(settings, x_sheets_secret)
    return _enqueue(settings, payload)
