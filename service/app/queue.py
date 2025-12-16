from typing import Any, Dict
from rq import Queue
from redis import Redis

from .config import Settings
from .worker_tasks import process_event_task


def _redis_conn(settings: Settings) -> Redis:
    return Redis.from_url(settings.redis_url)


def enqueue_job(settings: Settings, payload: Dict[str, Any]) -> str:
    q = Queue(name="default", connection=_redis_conn(settings))
    job = q.enqueue(process_event_task, payload, job_timeout=600)  # 10 min
    return job.id
