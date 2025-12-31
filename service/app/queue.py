# service/app/queue.py
from typing import Any, Dict

from rq import Queue

from .config import Settings
from .worker_tasks import process_event_task
from .redis_conn import get_redis


def enqueue_job(settings: Settings, payload: Dict[str, Any]) -> str:
    redis_conn = get_redis(settings.redis_url)
    q = Queue(name="default", connection=redis_conn)

    job = q.enqueue(process_event_task, payload, job_timeout=600)  # 10 min
    return job.id
