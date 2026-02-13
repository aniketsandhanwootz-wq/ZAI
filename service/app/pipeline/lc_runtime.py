from __future__ import annotations

from typing import Any, Dict, Optional, Union

def _is_envelope(x: Any) -> bool:
    return isinstance(x, dict) and "ok" in x and ("result" in x or "error" in x)

def _unwrap_result(x: Any) -> Any:
    """
    Recursively unwrap nested ToolResponse-style envelopes:
      {"ok": True, "result": <maybe another envelope>, ...}
    This prevents shape drift when a tool already returns an envelope.
    """
    cur = x
    # unwrap up to a small depth to avoid accidental loops
    for _ in range(5):
        if _is_envelope(cur) and cur.get("ok") is True:
            cur = cur.get("result")
            continue
        break
    return cur
def _ensure_logs(state: Dict[str, Any]) -> list:
    if "logs" not in state or not isinstance(state.get("logs"), list):
        state["logs"] = []
    return state["logs"]


def lc_registry(settings, state: Dict[str, Any]):
    """
    Build ToolRegistry once per pipeline run and cache in state.
    """
    reg = state.get("tool_registry")
    if reg is not None:
        return reg
    from ..tools.langchain_tools import get_tool_registry  # local import to avoid cycles
    reg = get_tool_registry(settings)
    state["tool_registry"] = reg
    return reg


def lc_invoke(
    tools_or_registry: Union[Dict[str, Any], Any],
    tool_name: str,
    args: Dict[str, Any],
    state: Dict[str, Any],
    *,
    fatal: bool = False,
    default: Any = None,
) -> Any:
    """
    Invoke tool by name and unwrap the stable envelope:
      { ok: bool, result: ..., error: {code,message,details} }

    Supports:
      - ToolRegistry (preferred)
      - dict of StructuredTool (legacy)
    """
    logs = _ensure_logs(state)

    # Helpful tracing metadata (SAFE + small)
    run_id = state.get("run_id") or state.get("idempotency_primary_id") or ""
    tenant_id = state.get("tenant_id") or ""
    primary_id = state.get("primary_id") or state.get("checkin_id") or state.get("legacy_id") or ""

    # Wrap each tool call in a context so spans nest consistently
    try:
        from ..tools.langsmith_trace import tracing_context, flush_traces
    except Exception:
        tracing_context = None  # type: ignore
        flush_traces = None  # type: ignore

    meta = {
        "zai": {
            "tool_name": tool_name,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "primary_id": primary_id,
            "event_type": state.get("event_type") or (state.get("payload") or {}).get("event_type"),
        }
    }

    def _with_ctx(fn):
        if tracing_context:
            with tracing_context(meta):
                return fn()
        return fn()

    # Preferred: ToolRegistry
    if hasattr(tools_or_registry, "invoke") and callable(getattr(tools_or_registry, "invoke")):
        try:
            resp = _with_ctx(lambda: tools_or_registry.invoke(tool_name, args or {}))
        except Exception as e:
            msg = f"lc_invoke: registry crashed for '{tool_name}': {e}"
            logs.append(msg)
            if fatal:
                raise
            return default

        # Opportunistic flush helps prevent "running" traces on workers
        try:
            if flush_traces:
                flush_traces()
        except Exception:
            pass

        if isinstance(resp, dict) and resp.get("ok") is True:
            return _unwrap_result(resp.get("result"))

        err = (resp or {}).get("error") if isinstance(resp, dict) else {}
        code = (err or {}).get("code") or "UNKNOWN"
        message = (err or {}).get("message") or "tool failed"
        logs.append(f"Tool failed: {tool_name} code={code} message={message}")
        if fatal:
            raise RuntimeError(f"{tool_name} failed: {code} {message}")
        return default

    # Legacy: dict[name]->StructuredTool (kept only for backwards compatibility)
    tools = tools_or_registry if isinstance(tools_or_registry, dict) else {}
    t = tools.get(tool_name)
    if not t:
        msg = f"lc_invoke: missing tool '{tool_name}'"
        logs.append(msg)
        if fatal:
            raise RuntimeError(msg)
        return default

    try:
        resp = _with_ctx(lambda: t.invoke(args or {}))
    except Exception as e:
        msg = f"lc_invoke: tool '{tool_name}' invoke crashed: {e}"
        logs.append(msg)
        if fatal:
            raise
        return default

    try:
        if flush_traces:
            flush_traces()
    except Exception:
        pass

    if not isinstance(resp, dict):
        msg = f"lc_invoke: tool '{tool_name}' returned non-dict"
        logs.append(msg)
        if fatal:
            raise RuntimeError(msg)
        return default

    if resp.get("ok") is True:
        return _unwrap_result(resp.get("result"))

    err = resp.get("error") or {}
    code = (err.get("code") or "UNKNOWN").strip()
    message = (err.get("message") or "tool failed").strip()
    logs.append(f"Tool failed: {tool_name} code={code} message={message}")
    if fatal:
        raise RuntimeError(f"{tool_name} failed: {code} {message}")
    return default