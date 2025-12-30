import logging
import threading
import time
import os
import socket
from uuid import uuid4

from redis import Redis
from rq import Worker, Queue

from .config import Settings

logger = logging.getLogger("zai.consumer")

_consumer_started = False


def start_consumer_thread(settings: Settings) -> None:
    """
    Runs an RQ worker inside the SAME web service process (MVP cost saver).
    Set RUN_CONSUMER=0 if you later deploy a dedicated Render Worker.
    """
    global _consumer_started
    if _consumer_started:
        return
    _consumer_started = True

    t = threading.Thread(target=_run_worker, args=(settings,), daemon=True)
    t.start()
    logger.info("consumer thread started. queues=%s", settings.consumer_queues)


def _run_worker(settings: Settings) -> None:
    redis_conn = Redis.from_url(settings.redis_url)
    queue_names = [q.strip() for q in settings.consumer_queues.split(",") if q.strip()]
    queues = [Queue(name, connection=redis_conn) for name in queue_names]

    while True:
        worker = None
        try:
            # Unique worker name per retry so stale registrations don't collide
            worker_name = f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"
            worker = Worker(queues, connection=redis_conn, name=worker_name)

            logger.info("worker.work() loop starting… name=%s queues=%s", worker_name, queue_names)
            worker.work(with_scheduler=False, burst=False)

        except Exception as e:
            logger.exception("worker crashed; retrying in 5s: %s", e)

            # Best-effort cleanup if worker partially registered
            try:
                if worker is not None:
                    worker.register_death()
            except Exception:
                pass

            time.sleep(5)
