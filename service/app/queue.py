# service/app/queue.py
from typing import Any, Dict

from rq import Queue

from .config import Settings
from .redis_conn import get_redis
from .worker_tasks import process_event_task, process_glide_webhook_task


def enqueue_job(settings: Settings, payload: Dict[str, Any]) -> str:
    """
    Existing pipeline jobs (Sheet/Appsheet events etc.)
    """
    redis_conn = get_redis(settings.redis_url)
    q = Queue(name="default", connection=redis_conn)
    job = q.enqueue(process_event_task, payload, job_timeout=600)  # 10 min
    return job.id


def enqueue_glide_job(settings: Settings, payload: Dict[str, Any]) -> str:
    """
    Glide incremental ingestion jobs
    """
    redis_conn = get_redis(settings.redis_url)
    q = Queue(name="default", connection=redis_conn)
    job = q.enqueue(process_glide_webhook_task, payload, job_timeout=600)  # 10 min
    return job.id