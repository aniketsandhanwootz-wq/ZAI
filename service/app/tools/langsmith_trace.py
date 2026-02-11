# service/app/tools/langsmith_trace.py
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Optional, TypeVar

T = TypeVar("T")

def _is_settings_like(x: Any) -> bool:
    return hasattr(x, "__class__") and x.__class__.__name__ == "Settings"


def _scrub_settings(x: Any) -> Dict[str, Any]:
    """
    Prevent huge/secret Settings from being sent to LangSmith.
    Keep only non-sensitive debugging fields.
    """
    try:
        return {
            "llm_provider": getattr(x, "llm_provider", None),
            "llm_model": getattr(x, "llm_model", None),
            "embedding_provider": getattr(x, "embedding_provider", None),
            "embedding_model": getattr(x, "embedding_model", None),
            "vision_provider": getattr(x, "vision_provider", None),
            "vision_model": getattr(x, "vision_model", None),
            "run_consumer": getattr(x, "run_consumer", None),
            "consumer_queues": getattr(x, "consumer_queues", None),
            "run_migrations": getattr(x, "run_migrations", None),
        }
    except Exception:
        return {"settings": "unavailable"}


def _scrub_state(x: Any) -> Any:
    """
    Keep state small; avoid uploading images/base64/huge payloads.
    """
    if not isinstance(x, dict):
        return x
    keep_keys = {
        "run_id",
        "event_type",
        "primary_id",
        "idempotency_primary_id",
        "tenant_id",
        "checkin_id",
        "conversation_id",
        "legacy_id",
        "project_name",
        "part_number",
        "checkin_status",
        "writeback_done",
        "logs",
    }
    out: Dict[str, Any] = {}
    for k in keep_keys:
        if k in x:
            out[k] = x.get(k)
    # payload is often huge; keep only a minimal view
    payload = x.get("payload")
    if isinstance(payload, dict):
        out["payload"] = {
            "event_type": payload.get("event_type"),
            "checkin_id": payload.get("checkin_id"),
            "conversation_id": payload.get("conversation_id"),
            "legacy_id": payload.get("legacy_id"),
            "ccp_id": payload.get("ccp_id"),
            "row_id": payload.get("row_id") or payload.get("dashboard_row_id") or payload.get("dashboard_update_id"),
            "meta": payload.get("meta") if isinstance(payload.get("meta"), dict) else None,
        }
    return out


def _safe_process_inputs(args: Any, kwargs: Any) -> Dict[str, Any]:
    """
    LangSmith 'traceable' supports process_inputs in many versions.
    We normalize args/kwargs into a safe dict.
    """
    safe_args: list[Any] = []
    for a in list(args or []):
        if _is_settings_like(a):
            safe_args.append(_scrub_settings(a))
        elif isinstance(a, dict) and ("payload" in a or "event_type" in a or "run_id" in a):
            safe_args.append(_scrub_state(a))
        else:
            safe_args.append(a)
    # kwargs can be large too; keep as-is but scrub obvious state/settings
    safe_kwargs: Dict[str, Any] = {}
    for k, v in (kwargs or {}).items():
        if _is_settings_like(v):
            safe_kwargs[k] = _scrub_settings(v)
        elif isinstance(v, dict) and ("payload" in v or "event_type" in v or "run_id" in v):
            safe_kwargs[k] = _scrub_state(v)
        else:
            safe_kwargs[k] = v
    return {"args": safe_args, "kwargs": safe_kwargs}


def _safe_process_outputs(out: Any) -> Any:
    # Keep outputs small and JSON-ish
    if isinstance(out, dict):
        # drop large blobs if any
        banned = {"images", "image_bytes", "pdf_bytes", "raw_response", "raw"}
        return {k: v for k, v in out.items() if k not in banned}
    return out


def flush_traces() -> None:
    """
    Force flush in long-running workers/web containers so runs don't remain 'running' in UI.
    Safe no-op if not supported.
    """
    if not enabled():
        return
    try:
        import langsmith as ls  # type: ignore
        Client = getattr(ls, "Client", None)
        if Client:
            Client().flush()
    except Exception:
        pass

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
    Sanitizes args/outputs so runs always finalize (prevents spinner / "running" forever).
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

    # Some langsmith versions support process_inputs/process_outputs. Use if available, else fall back.
    try:
        return traceable(
            run_type=run_type,
            name=name,
            process_inputs=lambda args, kwargs: _safe_process_inputs(args, kwargs),
            process_outputs=_safe_process_outputs,
        )(_wrapped)  # type: ignore[return-value]
    except TypeError:
        # Older versions: no process_inputs/process_outputs
        return traceable(run_type=run_type, name=name)(_wrapped)  # type: ignore[return-value]
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