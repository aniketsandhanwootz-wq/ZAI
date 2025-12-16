import threading
import time
from rq import Worker, Queue
from redis import Redis

from .config import Settings


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


def _run_worker(settings: Settings) -> None:
    redis_conn = Redis.from_url(settings.redis_url)
    queue_names = [q.strip() for q in settings.consumer_queues.split(",") if q.strip()]
    queues = [Queue(name, connection=redis_conn) for name in queue_names]

    worker = Worker(queues, connection=redis_conn)

    # Run forever. If Redis free instance restarts, worker will reconnect.
    while True:
        try:
            worker.work(with_scheduler=False, burst=False)
        except Exception as e:
            # Avoid crash loop; wait and retry
            print(f"[consumer] worker crashed: {e}")
            time.sleep(5)
