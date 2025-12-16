from typing import Any, Dict
from .config import load_settings
from .pipeline.graph import run_event_graph


def process_event_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    This function is executed by RQ worker.
    """
    settings = load_settings()
    return run_event_graph(settings, payload)
