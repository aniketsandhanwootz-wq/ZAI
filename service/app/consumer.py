# service/app/consumer.py
import logging
import os
import socket
import threading
import time
from uuid import uuid4

from rq import Queue, Worker
from redis.exceptions import ConnectionError as RedisConnectionError

from .config import Settings
from .redis_conn import get_redis

logger = logging.getLogger("zai.consumer")

_consumer_started = False


def start_consumer_thread(settings: Settings) -> None:
    """
    Runs an RQ worker inside the SAME web service process (MVP cost saver).
    """
    global _consumer_started
    if _consumer_started:
        return
    _consumer_started = True

    t = threading.Thread(target=_run_worker, args=(settings,), daemon=True)
    t.start()
    logger.info("consumer thread started. queues=%s", settings.consumer_queues)


def _run_worker(settings: Settings) -> None:
    queue_names = [q.strip() for q in settings.consumer_queues.split(",") if q.strip()]

    # Reuse shared pool
    redis_conn = get_redis(settings.redis_url)
    queues = [Queue(name, connection=redis_conn) for name in queue_names]

    while True:
        worker = None
        try:
            worker_name = f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"
            worker = Worker(queues, connection=redis_conn, name=worker_name)

            logger.info("worker.work() loop starting… name=%s queues=%s", worker_name, queue_names)
            worker.work(with_scheduler=False, burst=False)

        except RedisConnectionError as e:
            # Redis saturated / maxclients etc.
            logger.exception("Redis connection error; retrying in 5s: %s", e)

            # Close all pooled sockets so we don't keep stale connections around
            try:
                redis_conn.connection_pool.disconnect(inuse_connections=True)
            except Exception:
                pass

            time.sleep(5)

        except Exception as e:
            logger.exception("worker crashed; retrying in 5s: %s", e)
            try:
                if worker is not None:
                    worker.register_death()
            except Exception:
                pass
            time.sleep(5)
