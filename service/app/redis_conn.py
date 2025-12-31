# service/app/redis_conn.py
from __future__ import annotations

import os
from functools import lru_cache

from redis import Redis
from redis.connection import ConnectionPool


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default


@lru_cache(maxsize=8)
def _pool(redis_url: str, max_conns: int) -> ConnectionPool:
    """
    One ConnectionPool per (redis_url, max_conns) per process.
    This is the core fix to stop Redis client leaks / maxclients spikes.
    """
    return ConnectionPool.from_url(
        redis_url,
        max_connections=max_conns,
        socket_keepalive=True,
        health_check_interval=30,
        socket_connect_timeout=5,
        socket_timeout=10,
        retry_on_timeout=True,
    )


@lru_cache(maxsize=8)
def get_redis(redis_url: str, max_conns: int | None = None) -> Redis:
    """
    Shared Redis client using shared pool.
    """
    if max_conns is None:
        max_conns = _env_int("REDIS_MAX_CONNECTIONS", 15)
    return Redis(connection_pool=_pool(redis_url, int(max_conns)))
