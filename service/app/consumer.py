# service/app/consumer.py
from __future__ import annotations

import logging
import os
import signal
import subprocess
import threading
import time
from typing import Optional

from .config import Settings

logger = logging.getLogger("zai.consumer")

_started = False
_stop = threading.Event()
_proc: Optional[subprocess.Popen] = None


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def start_consumer_thread(settings: Settings) -> None:
    """
    Starts an RQ worker as a separate PROCESS, monitored by a daemon thread.

    IMPORTANT:
    - If you run multiple web processes/instances, each will spawn its own worker.
      For budget + stability, keep WEB_CONCURRENCY=1 and scale instances=1.
    """
    global _started
    if _started:
        return
    _started = True

    t = threading.Thread(target=_monitor_worker_proc, args=(settings,), daemon=True)
    t.start()
    logger.info("consumer monitor started. queues=%s", settings.consumer_queues)


def stop_consumer() -> None:
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

    worker_ttl = _env_int("RQ_WORKER_TTL_SECONDS", 420)  # RQ default-ish safety
    burst = os.getenv("RQ_BURST", "0").strip() in ("1", "true", "TRUE", "yes", "YES")
    worker_name = os.getenv("RQ_WORKER_NAME", "").strip() or f"web-embedded-{os.getpid()}"

    cmd = ["rq", "worker", *queues, "--url", settings.redis_url, "--name", worker_name, "--worker-ttl", str(worker_ttl)]
    if burst:
        cmd.append("--burst")

    env = os.environ.copy()
    env.setdefault("RQ_WORKER_LOG_LEVEL", "INFO")

    logger.info("starting rq worker process: %s", " ".join(cmd))

    # New process group => we can terminate the whole group reliably
    def _preexec():
        os.setsid()

    _proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=None,  # inherit so Render shows logs
        stderr=None,
        preexec_fn=_preexec if hasattr(os, "setsid") else None,
    )

    time.sleep(0.25)
    if _proc.poll() is not None:
        logger.error("rq worker exited immediately with code=%s", _proc.returncode)


def _terminate_proc() -> None:
    global _proc
    if _proc is None:
        return

    try:
        if _proc.poll() is None:
            # Kill the whole process group if possible
            try:
                pgid = os.getpgid(_proc.pid)
                os.killpg(pgid, signal.SIGTERM)
            except Exception:
                _proc.terminate()

            try:
                _proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    pgid = os.getpgid(_proc.pid)
                    os.killpg(pgid, signal.SIGKILL)
                except Exception:
                    _proc.kill()
    except Exception:
        pass
    finally:
        _proc = None