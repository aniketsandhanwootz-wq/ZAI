# service/app/consumer.py
from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
from typing import Optional

from .config import Settings

logger = logging.getLogger("zai.consumer")

_started = False
_stop = threading.Event()
_proc: Optional[subprocess.Popen] = None


def start_consumer_thread(settings: Settings) -> None:
    """
    Starts RQ worker as a SEPARATE PROCESS (safe), but still inside same Render Web Service container.
    Keep RUN_CONSUMER=1 for this mode.
    """
    global _started
    if _started:
        return
    _started = True

    t = threading.Thread(target=_monitor_worker_proc, args=(settings,), daemon=True)
    t.start()
    logger.info("consumer monitor started. queues=%s", settings.consumer_queues)


def stop_consumer() -> None:
    """Call on shutdown to stop worker process cleanly."""
    _stop.set()
    _terminate_proc()


def _monitor_worker_proc(settings: Settings) -> None:
    backoff = 2
    while not _stop.is_set():
        try:
            if _proc is None or _proc.poll() is not None:
                _spawn_worker(settings)
                backoff = 2
            time.sleep(1)
        except Exception as e:
            logger.exception("consumer monitor error; retrying in %ss: %s", backoff, e)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


def _spawn_worker(settings: Settings) -> None:
    global _proc

    queues = [q.strip() for q in (settings.consumer_queues or "").split(",") if q.strip()] or ["default"]

    # Run the RQ worker CLI in a separate process.
    # This avoids running Worker() inside a thread and avoids sharing the web pool.
    cmd = ["rq", "worker", *queues, "--url", settings.redis_url]

    env = os.environ.copy()
    env.setdefault("RQ_WORKER_LOG_LEVEL", "INFO")

    logger.info("starting rq worker process: %s", " ".join(cmd))
    _proc = subprocess.Popen(cmd, env=env)

    time.sleep(0.25)
    if _proc.poll() is not None:
        logger.error("rq worker exited immediately with code=%s", _proc.returncode)


def _terminate_proc() -> None:
    global _proc
    if _proc is None:
        return
    try:
        if _proc.poll() is None:
            _proc.terminate()
            try:
                _proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                _proc.kill()
    except Exception:
        pass
    finally:
        _proc = None
