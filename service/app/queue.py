from typing import Any, Dict, Optional
import os
import threading

from redis import Redis, ConnectionPool
from rq import Queue

from .config import Settings
from .worker_tasks import process_event_task

_lock = threading.Lock()
_pool: Optional[ConnectionPool] = None
_pool_url: Optional[str] = None


def _get_redis(settings: Settings) -> Redis:
    """
    Reuse a single ConnectionPool.
    DO NOT create Redis.from_url() per request -> it creates a new pool and can hit maxclients.
    """
    global _pool, _pool_url

    url = settings.redis_url
    max_conns = int(os.getenv("REDIS_MAX_CONNECTIONS", "10"))

    with _lock:
        if _pool is None or _pool_url != url:
            _pool = ConnectionPool.from_url(
                url,
                max_connections=max_conns,
                socket_keepalive=True,
                health_check_interval=30,
            )
            _pool_url = url

    return Redis(connection_pool=_pool)


def enqueue_job(settings: Settings, payload: Dict[str, Any]) -> str:
    redis_conn = _get_redis(settings)
    q = Queue(name="default", connection=redis_conn)
    job = q.enqueue(process_event_task, payload, job_timeout=600)  # 10 min
    return job.id
