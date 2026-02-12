# service/app/routers/glide_webhook.py
from __future__ import annotations

import hmac
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request, HTTPException
from redis.exceptions import ConnectionError as RedisConnectionError

from ..config import Settings

logger = logging.getLogger("zai.glide_webhook")

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


def _get_settings(request: Request) -> Settings:
    return request.app.state.settings  # type: ignore


def _consteq(a: str, b: str) -> bool:
    return hmac.compare_digest((a or "").strip(), (b or "").strip())


def _require_secret(request: Request, settings: Settings) -> None:
    """
    Accept secret via:
      - Header: x-webhook-secret
      - Header: authorization: Bearer <secret>
      - Query: ?secret=<secret>
    Reuse WEBHOOK_SECRET.
    """
    expected = (settings.webhook_secret or "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="Server missing WEBHOOK_SECRET")

    got = (request.headers.get("x-webhook-secret") or "").strip()

    if not got:
        auth = (request.headers.get("authorization") or "").strip()
        if auth.lower().startswith("bearer "):
            got = auth.split(" ", 1)[1].strip()

    if not got:
        got = (request.query_params.get("secret") or "").strip()

    if not got or not _consteq(got, expected):
        raise HTTPException(status_code=401, detail="Unauthorized (bad secret)")


def _truthy(v: Optional[str]) -> bool:
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _default_queue(settings: Settings) -> str:
    q = (settings.consumer_queues or "default").split(",")[0].strip()
    return q or "default"


def _as_list(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        out: List[str] = []
        for v in x:
            s = str(v or "").strip()
            if s:
                out.append(s)
        return out
    s = str(x or "").strip()
    return [s] if s else []


def _pick_table_key(body: Dict[str, Any]) -> str:
    for k in ("table_key", "table", "entity", "kb_table", "type"):
        v = str(body.get(k) or "").strip()
        if v:
            return v
    return ""


def _pick_row_ids(body: Dict[str, Any]) -> List[str]:
    for k in ("row_ids", "rowIds", "row_id", "rowId", "id", "$rowID"):
        if k in body:
            ids = _as_list(body.get(k))
            if ids:
                return ids
    return []


def _normalize_table_key(k: str) -> str:
    k = (k or "").strip().lower().replace(" ", "_").replace("-", "_")
    alias = {
        "rawmaterial": "raw_material",
        "raw_materials": "raw_material",
        "process": "processes",
        "proc": "processes",
        "boughtout": "boughtouts",
        "bought_out": "boughtouts",
        "bo": "boughtouts",
        "project": "project",
        "projects": "project",
        "company": "company",
        "companies": "company",
        "company_profile": "company",
        "company_profiles": "company",
    }
    return alias.get(k, k)


@router.post("/glide")
async def glide_webhook(
    request: Request,
    sync: Optional[str] = None,   # ?sync=1 debug
    queue: Optional[str] = None,  # ?queue=high override
) -> Dict[str, Any]:
    settings = _get_settings(request)
    _require_secret(request, settings)

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object")

    table_key = _normalize_table_key(_pick_table_key(body))
    row_ids = _pick_row_ids(body)

    if not table_key:
        raise HTTPException(status_code=400, detail="Missing table/table_key in payload")
    if not row_ids:
        raise HTTPException(status_code=400, detail="Missing row_id/row_ids in payload")

    meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
    event = str(body.get("event") or body.get("action") or body.get("trigger") or "").strip() or "updated"

    task_payload = {
        "source": "glide_webhook",
        "table_key": table_key,
        "row_ids": row_ids,
        "event": event,
        "meta": meta,
    }

    # Debug/local: run inline
    if _truthy(sync):
        from ..worker_tasks import process_glide_webhook_task

        result = process_glide_webhook_task(task_payload)
        return {"ok": True, "enqueued": False, "result": result, "table_key": table_key, "row_ids": row_ids}

    # Production: enqueue
    qname = (queue or "").strip() or _default_queue(settings)

    try:
        from ..worker_tasks import enqueue_glide_webhook_task

        job = enqueue_glide_webhook_task(task_payload, queue_name=qname)
        logger.info("enqueued glide ingest job=%s table=%s rows=%s event=%s queue=%s", job.get("job_id"), table_key, len(row_ids), event, qname)
        return {"ok": True, "enqueued": True, "queue": qname, "job": job, "table_key": table_key, "row_ids": row_ids, "event": event}
    except RedisConnectionError as e:
        raise HTTPException(status_code=503, detail=f"Queue unavailable: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to enqueue: {e}")