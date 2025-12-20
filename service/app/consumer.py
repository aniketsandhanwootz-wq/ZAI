import logging
import threading
import time

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

    worker = Worker(queues, connection=redis_conn)

    while True:
        try:
            logger.info("worker.work() loop starting…")
            worker.work(with_scheduler=False, burst=False)
        except Exception as e:
            logger.exception("worker crashed; retrying in 5s: %s", e)
            time.sleep(5)
