# service/app/lc/observability/langsmith.py
from __future__ import annotations

import os
from typing import Any, Dict, List

def _has_langsmith() -> bool:
    # Either the newer or older env patterns might be present depending on setup
    return bool(
        os.getenv("LANGSMITH_API_KEY")
        or os.getenv("LANGCHAIN_API_KEY")
        or os.getenv("LANGCHAIN_TRACING_V2")
        or os.getenv("LANGSMITH_TRACING")
    )

def get_callbacks(run_id: str, extra: Dict[str, Any] | None = None) -> List[Any]:
    """
    Returns callbacks that LangGraph/LangChain can use for tracing.
    Safe no-op if LangSmith isn't configured.
    """
    if not _has_langsmith():
        return []

    # Newer stack: langsmith + langchain callbacks
    try:
        from langchain.callbacks.tracers.langchain import LangChainTracer  # type: ignore
        tracer = LangChainTracer(project_name=os.getenv("LANGSMITH_PROJECT") or os.getenv("LANGCHAIN_PROJECT") or "zai")
        # Attach metadata
        tracer.tags = ["zai", "langgraph"]
        tracer.metadata = {"run_id": run_id, **(extra or {})}
        return [tracer]
    except Exception:
        # If callback import mismatches versions, do not break the pipeline.
        return []