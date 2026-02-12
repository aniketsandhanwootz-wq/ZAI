# service/app/routers/appsheet_webhook.py
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Header, HTTPException, Request
from redis.exceptions import ConnectionError as RedisConnectionError

from ..config import Settings
from ..schemas.webhook import WebhookPayload

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


def _get_settings(request: Request) -> Settings:
    return request.app.state.settings  # type: ignore


def _require_secret(settings: Settings, provided: Optional[str]) -> None:
    if (provided or "") != (settings.webhook_secret or ""):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


def _truthy(v: Optional[str]) -> bool:
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _default_queue(settings: Settings) -> str:
    # Prefer first queue from CONSUMER_QUEUES
    q = (settings.consumer_queues or "default").split(",")[0].strip()
    return q or "default"


@router.post("/sheets")
def sheets_webhook(
    request: Request,
    payload: WebhookPayload,
    x_sheets_secret: Optional[str] = Header(default=None),
    sync: Optional[str] = None,  # ?sync=1 for debug
    queue: Optional[str] = None,  # ?queue=high for override
):
    settings = _get_settings(request)
    _require_secret(settings, x_sheets_secret)

    p = payload.model_dump(exclude_none=True)

    # Debug/local path: run inline
    if _truthy(sync):
        from ..pipeline.graph import run_event_graph

        result = run_event_graph(settings, p)
        return {"ok": True, "enqueued": False, "result": result}

    # Production path: enqueue
    qname = (queue or "").strip() or _default_queue(settings)

    try:
        from ..worker_tasks import enqueue_event_task

        job = enqueue_event_task(p, queue_name=qname)
        return {"ok": True, "enqueued": True, "queue": qname, "job": job}
    except RedisConnectionError as e:
        # Apps Script can retry safely
        raise HTTPException(status_code=503, detail=f"Queue unavailable: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to enqueue: {e}")