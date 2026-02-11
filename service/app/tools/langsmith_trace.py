# service/app/tools/langsmith_trace.py
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Optional, TypeVar

T = TypeVar("T")

def _truthy(v: str) -> bool:
    return (v or "").strip().lower() in ("1", "true", "yes", "y", "on")

def enabled() -> bool:
    # Support either naming style
    # LangSmith typical: LANGSMITH_API_KEY + LANGSMITH_TRACING=true
    # LangChain typical: LANGCHAIN_API_KEY + LANGCHAIN_TRACING_V2=true
    ls_key = os.getenv("LANGSMITH_API_KEY") or os.getenv("LANGCHAIN_API_KEY") or ""
    if not ls_key.strip():
        return False

    if _truthy(os.getenv("LANGSMITH_TRACING", "")):
        return True

    if _truthy(os.getenv("LANGCHAIN_TRACING_V2", "")):
        return True

    return False

def _project_name() -> str:
    return (
        os.getenv("LANGCHAIN_PROJECT")
        or os.getenv("LANGSMITH_PROJECT")
        or os.getenv("LANGSMITH_PROJECT_NAME")
        or "zai"
    )

def _tags_env() -> list[str]:
    raw = os.getenv("LANGSMITH_TAGS") or os.getenv("LANGCHAIN_TAGS") or ""
    tags = [t.strip() for t in raw.split(",") if t.strip()]
    return tags

def _safe_len(x: Any) -> int:
    try:
        return len(x)  # type: ignore[arg-type]
    except Exception:
        return 0

def traceable_wrap(fn: Callable[..., T], *, name: str, run_type: str) -> Callable[..., T]:
    """
    Wrap any function into a LangSmith span, if tracing is enabled.
    If tracing is disabled OR langsmith isn't installed, returns original fn.
    """
    if not enabled():
        return fn

    try:
        # Preferred minimal API: decorator
        from langsmith import traceable  # type: ignore
    except Exception:
        return fn

    def _wrapped(*args: Any, **kwargs: Any) -> T:
        return fn(*args, **kwargs)

    # Give it a readable name in the UI
    try:
        _wrapped.__name__ = name.replace("/", "_").replace(" ", "_").replace(":", "_")
    except Exception:
        pass

    return traceable(run_type=run_type)(_wrapped)  # type: ignore[return-value]

@contextmanager
def tracing_context(metadata: Optional[Dict[str, Any]] = None) -> Iterator[None]:
    """
    Optional project/tags context.
    Safe no-op if langsmith isn't installed or env isn't enabled.
    """
    if not enabled():
        yield
        return

    try:
        import langsmith as ls  # type: ignore
    except Exception:
        yield
        return

    # Some langsmith versions expose tracing_context; if missing, we still no-op safely.
    ctx = getattr(ls, "tracing_context", None)
    if not ctx:
        yield
        return

    project = _project_name()
    tags = _tags_env()
    md = dict(metadata or {})
    if tags:
        md.setdefault("tags", tags)

    with ctx(project=project, metadata=md):
        yield

def mk_http_meta(*, url: str, payload: Any = None, timeout_s: float | int | None = None) -> Dict[str, Any]:
    # Keep it small + safe (don’t log images / huge prompts)
    return {
        "url": str(url),
        "timeout_s": float(timeout_s) if timeout_s is not None else None,
        "payload_bytes": _safe_len(payload) if isinstance(payload, (bytes, bytearray)) else None,
        "payload_type": type(payload).__name__,
    }