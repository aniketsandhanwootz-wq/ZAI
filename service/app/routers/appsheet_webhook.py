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
    if (provided or "") != (settings.webhook_secret or ""):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


def _enqueue(settings: Settings, payload: WebhookPayload) -> dict:
    job_id = enqueue_job(settings, payload.model_dump(exclude_none=True))
    return {"ok": True, "job_id": job_id}


@router.post("/sheets")
def sheets_webhook(
    request: Request,
    payload: WebhookPayload,
    x_sheets_secret: Optional[str] = Header(default=None),
):
    """
    Apps Script -> FastAPI entrypoint.
    Send event_type + identifiers; we enqueue and the worker runs graph routing.
    """
    settings = _get_settings(request)
    _require_secret(settings, x_sheets_secret)
    return _enqueue(settings, payload)
