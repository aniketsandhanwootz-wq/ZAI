from typing import Any, Dict
import logging

from .config import load_settings
from .pipeline.graph import run_event_graph

logger = logging.getLogger("zai.worker")


def process_event_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    This function is executed by RQ worker.
    """
    settings = load_settings()
    logger.info("worker task started. event_type=%s checkin_id=%s convo_id=%s",
                payload.get("event_type"), payload.get("checkin_id"), payload.get("conversation_id"))
    return run_event_graph(settings, payload)
