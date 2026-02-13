# service/app/tools/langsmith_trace.py
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Optional, TypeVar

T = TypeVar("T")

def _truncate_str(s: str, max_len: int = 2000) -> str:
    s = s or ""
    if len(s) <= max_len:
        return s
    return s[:max_len] + "…[TRUNCATED]"

def _is_langchain_tool(x: Any) -> bool:
    # Avoid importing langchain in all environments; use duck-typing
    try:
        return x.__class__.__name__ in ("StructuredTool", "Tool") and hasattr(x, "name")
    except Exception:
        return False

def _tool_brief(x: Any) -> Dict[str, Any]:
    try:
        return {
            "tool": True,
            "name": getattr(x, "name", None),
            "description": _truncate_str(str(getattr(x, "description", "") or ""), 500),
        }
    except Exception:
        return {"tool": True, "name": "unknown"}

def _jsonable(x: Any, *, depth: int = 0, max_depth: int = 4, max_list: int = 50) -> Any:
    """
    Convert arbitrary objects into JSON-serializable shapes.
    This prevents LangSmith from trying model_dump_json on unsupported objects.
    """
    if x is None or isinstance(x, (bool, int, float)):
        return x

    if isinstance(x, str):
        return _truncate_str(x, 4000)

    if isinstance(x, bytes):
        return {"bytes": True, "len": len(x)}

    # Pydantic v2 BaseModel or compatible
    if hasattr(x, "model_dump") and callable(getattr(x, "model_dump")):
        try:
            return _jsonable(x.model_dump(), depth=depth + 1, max_depth=max_depth, max_list=max_list)
        except Exception:
            return {"pydantic": True, "type": x.__class__.__name__}

    # LangChain tools / StructuredTool
    if _is_langchain_tool(x):
        return _tool_brief(x)

    if depth >= max_depth:
        # stop recursion, but keep type info
        return {"type": x.__class__.__name__}

    if isinstance(x, dict):
        out: Dict[str, Any] = {}
        # keep only a reasonable number of keys to avoid huge trace payloads
        for i, (k, v) in enumerate(x.items()):
            if i >= 200:
                out["…"] = "TRUNCATED_KEYS"
                break
            out[str(k)] = _jsonable(v, depth=depth + 1, max_depth=max_depth, max_list=max_list)
        return out

    if isinstance(x, (list, tuple, set)):
        xs = list(x)
        if len(xs) > max_list:
            xs = xs[:max_list] + ["…TRUNCATED_LIST"]
        return [_jsonable(v, depth=depth + 1, max_depth=max_depth, max_list=max_list) for v in xs]

    # Fallback: string form
    try:
        return _truncate_str(str(x), 2000)
    except Exception:
        return {"type": x.__class__.__name__}
    
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
    Normalize args/kwargs into a SAFE, JSON-serializable dict.
    """
    safe_args: list[Any] = []
    for a in list(args or []):
        if _is_settings_like(a):
            safe_args.append(_jsonable(_scrub_settings(a)))
        elif isinstance(a, dict) and ("payload" in a or "event_type" in a or "run_id" in a):
            safe_args.append(_jsonable(_scrub_state(a)))
        else:
            safe_args.append(_jsonable(a))

    safe_kwargs: Dict[str, Any] = {}
    for k, v in (kwargs or {}).items():
        if _is_settings_like(v):
            safe_kwargs[str(k)] = _jsonable(_scrub_settings(v))
        elif isinstance(v, dict) and ("payload" in v or "event_type" in v or "run_id" in v):
            safe_kwargs[str(k)] = _jsonable(_scrub_state(v))
        else:
            safe_kwargs[str(k)] = _jsonable(v)

    return {"args": safe_args, "kwargs": safe_kwargs}

def _safe_process_outputs(out: Any) -> Any:
    """
    Ensure outputs are JSON-serializable and don't leak blobs.
    """
    o = _jsonable(out)

    if isinstance(o, dict):
        banned = {"images", "image_bytes", "pdf_bytes", "raw_response", "raw", "content_b64", "image_b64"}
        return {k: v for k, v in o.items() if k not in banned}

    return o



def _truthy(v: str) -> bool:
    return (v or "").strip().lower() in ("1", "true", "yes", "y", "on")


def enabled() -> bool:
    # API key is required
    ls_key = os.getenv("LANGSMITH_API_KEY") or os.getenv("LANGCHAIN_API_KEY") or ""
    if not ls_key.strip():
        return False

    # Prefer LangSmith flag, but accept LangChain v2 flag as well
    # Docs commonly show: LANGSMITH_TRACING=true
    if _truthy(os.getenv("LANGSMITH_TRACING", "")):
        return True

    # Some setups still use this (LangChain v2 tracing flag)
    if _truthy(os.getenv("LANGCHAIN_TRACING_V2", "")):
        return True

    # Back-compat: some users set LANGSMITH_TRACING_V2 or LANGCHAIN_TRACING
    if _truthy(os.getenv("LANGSMITH_TRACING_V2", "")):
        return True
    if _truthy(os.getenv("LANGCHAIN_TRACING", "")):
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

    Compatibility goal:
    - Some LangSmith versions call process_inputs(inputs_dict)
    - Others call process_inputs(args, kwargs)
    We support both without throwing TypeError.
    """
    if not enabled():
        return fn

    # langsmith moved/aliased traceable across versions; support both.
    try:
        from langsmith import traceable  # type: ignore
    except Exception:
        try:
            from langsmith.run_helpers import traceable  # type: ignore
        except Exception:
            return fn

    def _wrapped(*args: Any, **kwargs: Any) -> T:
        return fn(*args, **kwargs)

    try:
        _wrapped.__name__ = name.replace("/", "_").replace(" ", "_").replace(":", "_")
    except Exception:
        pass

    def _process_inputs_compat(*p: Any, **k: Any) -> Dict[str, Any]:
        # Variant A: process_inputs(inputs_dict)
        if len(p) == 1 and not k and isinstance(p[0], dict):
            inputs = p[0]
            a = inputs.get("args", [])
            kw = inputs.get("kwargs", {})
            return _safe_process_inputs(a, kw)

        # Variant B: process_inputs(args, kwargs)
        if len(p) == 2 and not k:
            return _safe_process_inputs(p[0], p[1])

        # Variant C: process_inputs(*args, **kwargs) (rare)
        return _safe_process_inputs(p, k)

    try:
        return traceable(
            run_type=run_type,
            name=name,
            process_inputs=_process_inputs_compat,
            process_outputs=_safe_process_outputs,
        )(_wrapped)  # type: ignore[return-value]
    except TypeError:
        # Older traceable without process_inputs/process_outputs support
        return traceable(run_type=run_type, name=name)(_wrapped)  # type: ignore[return-value]

@contextmanager
def tracing_context(metadata: Optional[Dict[str, Any]] = None) -> Iterator[None]:
    if not enabled():
        yield
        return

    project = _project_name()
    tags = _tags_env()
    md = dict(metadata or {})
    if tags:
        md.setdefault("tags", tags)

    # Support both:
    # - from langsmith import tracing_context
    # - import langsmith as ls; ls.tracing_context
    try:
        from langsmith import tracing_context as _ctx  # type: ignore
    except Exception:
        try:
            import langsmith as ls  # type: ignore
            _ctx = getattr(ls, "tracing_context", None)
        except Exception:
            _ctx = None

    if not _ctx:
        yield
        return

    with _ctx(project=project, metadata=md):
        yield

def flush_traces() -> None:
    """
    Force flush in long-running workers/web containers so runs don't remain 'running' in UI.
    Safe no-op if not supported.

    Different LangSmith versions expose flush in different places; try the common ones.
    """
    if not enabled():
        return

    # 1) Newer-style flush utility (if present)
    try:
        import langsmith as ls  # type: ignore
        utils = getattr(ls, "utils", None)
        tracing = getattr(utils, "tracing", None) if utils else None
        flush_fn = getattr(tracing, "flush", None) if tracing else None
        if callable(flush_fn):
            flush_fn()
            return
    except Exception:
        pass

    # 2) Client().flush() (older)
    try:
        import langsmith as ls  # type: ignore
        Client = getattr(ls, "Client", None)
        if Client:
            c = Client()
            flush = getattr(c, "flush", None)
            if callable(flush):
                flush()
                return
    except Exception:
        pass

    # 3) Nothing available -> no-op
    return

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
