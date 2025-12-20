# service/app/logctx.py
from __future__ import annotations

import logging
from contextvars import ContextVar

request_id_var: ContextVar[str] = ContextVar("request_id", default="-")
run_id_var: ContextVar[str] = ContextVar("run_id", default="-")


class ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get("-")
        record.run_id = run_id_var.get("-")
        return True


def setup_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fmt = "%(asctime)s %(levelname)s %(name)s | rid=%(request_id)s run=%(run_id)s | %(message)s"

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt))
    handler.addFilter(ContextFilter())

    # avoid duplicate handlers on reload
    root.handlers = [handler]
