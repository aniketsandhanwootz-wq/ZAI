from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Request
from redis.exceptions import ConnectionError as RedisConnectionError

from ..config import Settings
from ..queue import enqueue_job
from ..schemas.webhook import WebhookPayload

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


def _get_settings(request: Request) -> Settings:
    return request.app.state.settings  # type: ignore


def _require_secret(settings: Settings, provided: Optional[str]) -> None:
    if (provided or "") != (settings.wootzcheckin_webhook_secret or ""):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


def _enqueue(settings: Settings, payload: WebhookPayload) -> dict:
    try:
        job_id = enqueue_job(settings, payload.model_dump(exclude_none=True))
        return {"ok": True, "job_id": job_id}
    except RedisConnectionError as e:
        # wootzcheckin's worker retries on SQS redelivery, same as Apps Script
        raise HTTPException(status_code=503, detail=f"Queue unavailable: {e}")


@router.post("/wootzcheckin")
def wootzcheckin_webhook(
    request: Request,
    payload: WebhookPayload,
    x_wootzcheckin_secret: Optional[str] = Header(default=None),
):
    settings = _get_settings(request)
    _require_secret(settings, x_wootzcheckin_secret)
    return _enqueue(settings, payload)
