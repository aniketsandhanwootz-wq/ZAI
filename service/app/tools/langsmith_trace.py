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
    if isinstance(out, dict):
        banned = {"images", "image_bytes", "pdf_bytes", "raw_response", "raw", "content_b64", "image_b64"}
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
    return [t.strip() for t in raw.split(",") if t.strip()]


def traceable_wrap(fn: Callable[..., T], *, name: str, run_type: str) -> Callable[..., T]:
    """
    Wrap any function into a LangSmith span, if tracing is enabled.
    Sanitizes args/outputs so runs always finalize (prevents spinner / "running" forever).
    If tracing is disabled OR langsmith isn't installed, returns original fn.
    """
    if not enabled():
        return fn

    try:
        from langsmith import traceable  # type: ignore
    except Exception:
        return fn

    def _wrapped(*args: Any, **kwargs: Any) -> T:
        return fn(*args, **kwargs)

    try:
        _wrapped.__name__ = name.replace("/", "_").replace(" ", "_").replace(":", "_")
    except Exception:
        pass

    try:
        return traceable(
            run_type=run_type,
            name=name,
            process_inputs=lambda args, kwargs: _safe_process_inputs(args, kwargs),
            process_outputs=_safe_process_outputs,
        )(_wrapped)  # type: ignore[return-value]
    except TypeError:
        return traceable(run_type=run_type, name=name)(_wrapped)  # type: ignore[return-value]


@contextmanager
def tracing_context(metadata: Optional[Dict[str, Any]] = None) -> Iterator[None]:
    if not enabled():
        yield
        return

    try:
        import langsmith as ls  # type: ignore
    except Exception:
        yield
        return

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

def mk_http_meta(
    *,
    url: str = "",
    method: str = "POST",
    status_code: Optional[int] = None,
    provider: str = "",
    model: str = "",
    timeout_s: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build SAFE http metadata for tracing/logging.

    Rules:
    - Never include headers, tokens, or request/response bodies.
    - Strip querystring to avoid leaking API keys (e.g., ?key=...).
    """
    safe_url = (url or "").strip()
    if "?" in safe_url:
        safe_url = safe_url.split("?", 1)[0]

    out: Dict[str, Any] = {
        "http": {
            "method": (method or "POST").upper(),
            "url": safe_url,
        }
    }

    if status_code is not None:
        out["http"]["status_code"] = int(status_code)

    if timeout_s is not None:
        out["http"]["timeout_s"] = int(timeout_s)

    if provider:
        out["provider"] = str(provider)

    if model:
        out["model"] = str(model)

    if isinstance(extra, dict) and extra:
        # Ensure extra is JSON-ish and not huge/sensitive
        cleaned: Dict[str, Any] = {}
        for k, v in extra.items():
            if v is None:
                continue
            if isinstance(v, (str, int, float, bool)):
                cleaned[str(k)] = v
            else:
                cleaned[str(k)] = str(v)
        if cleaned:
            out["extra"] = cleaned

    return out
