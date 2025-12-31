# service/app/redis_conn.py
from __future__ import annotations

import os
import threading
from typing import Optional

from redis import Redis
from redis.connection import ConnectionPool

_lock = threading.Lock()
_pool: Optional[ConnectionPool] = None
_pool_url: Optional[str] = None


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def get_redis(redis_url: str) -> Redis:
    """
    Single shared pool per process.
    Redis-py will raise 'Too many connections' if pool max_connections is exceeded.
    """
    global _pool, _pool_url

    max_conns = _env_int("REDIS_MAX_CONNECTIONS", 30)  # recommend 30 for single service+worker

    with _lock:
        if _pool is None or _pool_url != redis_url:
            _pool = ConnectionPool.from_url(
                redis_url,
                max_connections=max_conns,
                socket_keepalive=True,
                health_check_interval=30,
                socket_connect_timeout=5,
                socket_timeout=10,
                retry_on_timeout=True,
            )
            _pool_url = redis_url

    return Redis(connection_pool=_pool)
