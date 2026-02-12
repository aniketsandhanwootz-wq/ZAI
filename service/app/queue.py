# service/app/queue.py
from __future__ import annotations

import os
from typing import Any, Dict, Optional

from rq import Queue, Retry

from .config import Settings
from .redis_conn import get_redis
from .worker_tasks import process_event_task, process_glide_webhook_task


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def _pick_queue_name(settings: Settings, explicit: Optional[str] = None) -> str:
    q = (explicit or "").strip()
    if q:
        return q

    # Use first queue from settings.consumer_queues
    raw = (settings.consumer_queues or "").strip()
    if raw:
        first = raw.split(",")[0].strip()
        if first:
            return first

    return "default"


def _default_retry() -> Retry:
    # 3 retries with increasing delay (seconds)
    # Tune via env if needed.
    max_retries = _env_int("RQ_RETRY_MAX", 3)
    # Example: "10,30,90"
    raw = os.getenv("RQ_RETRY_INTERVALS", "10,30,90")
    intervals = []
    for p in raw.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            intervals.append(int(p))
        except Exception:
            pass
    if not intervals:
        intervals = [10, 30, 90]
    return Retry(max=max_retries, interval=intervals)


def _queue(settings: Settings, name: Optional[str] = None) -> Queue:
    redis_conn = get_redis(settings.redis_url)
    qname = _pick_queue_name(settings, explicit=name)
    return Queue(name=qname, connection=redis_conn)


def enqueue_event_task(
    settings: Settings,
    payload: Dict[str, Any],
    queue_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Enqueue the main event graph processing task.
    Returns a small dict that is safe to log/return from APIs.
    """
    q = _queue(settings, queue_name)

    job_timeout = _env_int("RQ_JOB_TIMEOUT_SECONDS", 600)  # 10 min
    result_ttl = _env_int("RQ_RESULT_TTL_SECONDS", 0)      # keep 0 to avoid Redis growth
    failure_ttl = _env_int("RQ_FAILURE_TTL_SECONDS", 604800)  # 7 days
    ttl = _env_int("RQ_JOB_TTL_SECONDS", 86400)            # queued job must start within 24h

    job = q.enqueue(
        process_event_task,
        payload,
        job_timeout=job_timeout,
        ttl=ttl,
        result_ttl=result_ttl,
        failure_ttl=failure_ttl,
        retry=_default_retry(),
    )
    return {"job_id": job.id, "queue": q.name}


def enqueue_glide_task(
    settings: Settings,
    payload: Dict[str, Any],
    queue_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Enqueue Glide incremental ingestion task.
    """
    q = _queue(settings, queue_name)

    job_timeout = _env_int("RQ_GLIDE_JOB_TIMEOUT_SECONDS", 600)
    result_ttl = _env_int("RQ_RESULT_TTL_SECONDS", 0)
    failure_ttl = _env_int("RQ_FAILURE_TTL_SECONDS", 604800)
    ttl = _env_int("RQ_JOB_TTL_SECONDS", 86400)

    job = q.enqueue(
        process_glide_webhook_task,
        payload,
        job_timeout=job_timeout,
        ttl=ttl,
        result_ttl=result_ttl,
        failure_ttl=failure_ttl,
        retry=_default_retry(),
    )
    return {"job_id": job.id, "queue": q.name}