# service/app/pipeline/graph.py
from __future__ import annotations

import importlib
import logging
import time
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional

from langgraph.graph import StateGraph, END

from ..config import Settings
from ..logctx import run_id_var
from .ingest.run_log import RunLog
from ..tools.langsmith_trace import traceable_wrap, tracing_context

logger = logging.getLogger("zai.graph")

State = Dict[str, Any]
NodeFn = Callable[[Settings, State], State]


def _resolve_node(module_rel: str, candidates: List[str]) -> NodeFn:
    mod = importlib.import_module(module_rel, package=__package__)
    for name in candidates:
        fn = getattr(mod, name, None)
        if callable(fn):
            return fn  # type: ignore
    raise ImportError(
        f"Could not find a callable in {module_rel}. Tried: {candidates}. "
        f"Available: {[x for x in dir(mod) if not x.startswith('_')]}"
    )


# ---- Node function resolution (your existing nodes, unchanged) ----
load_sheet_data = _resolve_node(".nodes.load_sheet_data", ["load_sheet_data_node", "load_sheet_data", "run", "node"])
build_thread_snapshot = _resolve_node(".nodes.build_thread_snapshot", ["build_thread_snapshot_node", "build_thread_snapshot", "run", "node"])
analyze_media = _resolve_node(".nodes.analyze_media", ["analyze_media", "run", "node"])
analyze_attachments = _resolve_node(".nodes.analyze_attachments", ["analyze_attachments", "run", "node"])
upsert_vectors = _resolve_node(".nodes.upsert_vectors", ["upsert_vectors_node", "upsert_vectors", "run", "node"])
retrieve_context = _resolve_node(".nodes.retrieve_context", ["retrieve_context_node", "retrieve_context", "run", "node"])
rerank_context = _resolve_node(".nodes.rerank_context", ["rerank_context", "run", "node"])
generate_ai_reply = _resolve_node(".nodes.generate_ai_reply", ["generate_ai_reply_node", "generate_ai_reply", "run", "node"])
annotate_media = _resolve_node(".nodes.annotate_media", ["annotate_media", "run", "node"])
writeback = _resolve_node(".nodes.writeback", ["writeback_node", "writeback", "run", "node"])
generate_assembly_todo = _resolve_node(".nodes.generate_assembly_todo", ["generate_assembly_todo", "run", "node"])


_ALLOWED_EVENT_TYPES = {
    "CHECKIN_CREATED",
    "CHECKIN_UPDATED",
    "CONVERSATION_ADDED",
    "CCP_UPDATED",
    "DASHBOARD_UPDATED",
    "PROJECT_UPDATED",
    "MANUAL_TRIGGER",
}


def _tenant_from_payload(payload: Dict[str, Any]) -> str:
    rmeta = payload.get("meta") or {}
    return str(rmeta.get("tenant_id") or "")


def _meta(payload: Dict[str, Any]) -> Dict[str, Any]:
    m = payload.get("meta") or {}
    return m if isinstance(m, dict) else {}


def _truthy(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return x != 0
    if isinstance(x, str):
        return x.strip().lower() in ("1", "true", "yes", "y", "on")
    return bool(x)


def _primary_id_for_event(payload: Dict[str, Any], event_type: str) -> str:
    event_type = (event_type or "").strip()

    checkin_id = str(payload.get("checkin_id") or "").strip()
    conversation_id = str(payload.get("conversation_id") or "").strip()
    ccp_id = str(payload.get("ccp_id") or "").strip()

    dashboard_row_id = str(
        payload.get("dashboard_update_id")
        or payload.get("dashboard_row_id")
        or payload.get("row_id")
        or ""
    ).strip()

    legacy_id = str(payload.get("legacy_id") or "").strip()

    if event_type == "PROJECT_UPDATED":
        return legacy_id or "UNKNOWN_PROJECT"

    if event_type in ("CHECKIN_CREATED", "CHECKIN_UPDATED"):
        return checkin_id or "UNKNOWN_CHECKIN"

    if event_type == "CONVERSATION_ADDED":
        return conversation_id or "UNKNOWN_CONVO"

    if event_type == "CCP_UPDATED":
        return ccp_id or "UNKNOWN_CCP"

    if event_type == "DASHBOARD_UPDATED":
        return dashboard_row_id or legacy_id or "UNKNOWN_DASH"

    m = _meta(payload)
    meta_primary = str(m.get("primary_id") or "").strip()
    return meta_primary or checkin_id or conversation_id or ccp_id or dashboard_row_id or legacy_id or "UNKNOWN"


def _scoped_primary_id_for_run(payload: Dict[str, Any], *, primary_id: str) -> str:
    """
    Checkpointing decision:
      - We do NOT use LangGraph checkpointing in MVP.
      - We rely on RunLog idempotency keys + RQ retry semantics.
    This scope avoids collisions between normal runs vs backfill modes.
    """
    m = _meta(payload)
    ingest_only = _truthy(m.get("ingest_only") or m.get("skip_reply") or m.get("skip_ai_reply"))
    media_only = _truthy(m.get("media_only"))

    if ingest_only and media_only:
        return f"{primary_id}::MEDIA_V1"
    if ingest_only:
        return f"{primary_id}::INGEST_V1"
    return primary_id


def _ensure_logs(state: State) -> List[str]:
    logs = state.get("logs")
    if not isinstance(logs, list):
        logs = []
        state["logs"] = logs
    return logs


def _timed_node(settings: Settings, state: State, name: str, fn: NodeFn) -> State:
    t0 = time.time()
    logger.info("node:start %s", name)

    traced = traceable_wrap(fn, name=f"zai.node.{name}", run_type="tool")
    out = traced(settings, state)

    dt = (time.time() - t0) * 1000
    logger.info("node:end %s ms=%.1f", name, dt)
    return out


def _assembly_todo_nonfatal(settings: Settings, state: State) -> State:
    try:
        return _timed_node(settings, state, "generate_assembly_todo", generate_assembly_todo)
    except Exception as e:
        _ensure_logs(state).append(f"assembly_todo: non-fatal failure: {e}")
        return state


def _route_after_analyzers(state: State) -> str:
    """
    Decide the next step after:
      load_sheet_data -> generate_assembly_todo -> build_thread_snapshot -> analyze_media -> analyze_attachments
    """
    payload = state.get("payload") or {}
    m = _meta(payload)
    event_type = str(state.get("event_type") or payload.get("event_type") or "").strip()

    force_reply = _truthy(m.get("force_reply"))
    ingest_only_default = event_type in ("CHECKIN_UPDATED", "CONVERSATION_ADDED")
    ingest_only = _truthy(m.get("ingest_only") or m.get("skip_reply") or m.get("skip_ai_reply")) or ingest_only_default
    if event_type == "CHECKIN_CREATED" and force_reply:
        ingest_only = False

    if ingest_only or event_type != "CHECKIN_CREATED":
        return "upsert_vectors"

    return "retrieve_context"


def _route_after_upsert(state: State) -> str:
    payload = state.get("payload") or {}
    event_type = str(state.get("event_type") or payload.get("event_type") or "").strip()
    m = _meta(payload)

    force_reply = _truthy(m.get("force_reply"))
    ingest_only_default = event_type in ("CHECKIN_UPDATED", "CONVERSATION_ADDED")
    ingest_only = _truthy(m.get("ingest_only") or m.get("skip_reply") or m.get("skip_ai_reply")) or ingest_only_default
    if event_type == "CHECKIN_CREATED" and force_reply:
        ingest_only = False

    if ingest_only or event_type != "CHECKIN_CREATED":
        return "END"
    return "writeback"


@lru_cache(maxsize=1)
def _build_langgraph_app() -> Any:
    """
    Build/compile once per process.
    Dispatch layer calls bound node closures stored in state["__lg_bound_nodes__"].
    """
    def _dispatch(node_name: str) -> Callable[[State], State]:
        def _f(s: State) -> State:
            bound = s.get("__lg_bound_nodes__") or {}
            fn = bound.get(node_name)
            if not callable(fn):
                _ensure_logs(s).append(f"LangGraph dispatch missing node '{node_name}'")
                return s
            return fn(s)
        return _f

    g: StateGraph = StateGraph(dict)

    # Nodes are dispatch wrappers
    g.add_node("load_sheet_data", _dispatch("load_sheet_data"))
    g.add_node("generate_assembly_todo", _dispatch("generate_assembly_todo"))
    g.add_node("build_thread_snapshot", _dispatch("build_thread_snapshot"))
    g.add_node("analyze_media", _dispatch("analyze_media"))
    g.add_node("analyze_attachments", _dispatch("analyze_attachments"))
    g.add_node("upsert_vectors", _dispatch("upsert_vectors"))
    g.add_node("retrieve_context", _dispatch("retrieve_context"))
    g.add_node("rerank_context", _dispatch("rerank_context"))
    g.add_node("generate_ai_reply", _dispatch("generate_ai_reply"))
    g.add_node("annotate_media", _dispatch("annotate_media"))
    g.add_node("writeback", _dispatch("writeback"))

    # Edges
    g.set_entry_point("load_sheet_data")
    g.add_edge("load_sheet_data", "generate_assembly_todo")
    g.add_edge("generate_assembly_todo", "build_thread_snapshot")
    g.add_edge("build_thread_snapshot", "analyze_media")
    g.add_edge("analyze_media", "analyze_attachments")

    g.add_conditional_edges(
        "analyze_attachments",
        _route_after_analyzers,
        {"upsert_vectors": "upsert_vectors", "retrieve_context": "retrieve_context"},
    )

    g.add_edge("retrieve_context", "rerank_context")
    g.add_edge("rerank_context", "generate_ai_reply")
    g.add_edge("generate_ai_reply", "annotate_media")
    g.add_edge("annotate_media", "upsert_vectors")

    g.add_conditional_edges(
        "upsert_vectors",
        _route_after_upsert,
        {"END": END, "writeback": "writeback"},
    )
    g.add_edge("writeback", END)

    return g.compile()


def _bind_nodes(settings: Settings) -> Dict[str, Callable[[State], State]]:
    """
    Bind Settings once per invocation. Each bound callable has signature (state)->state.
    """
    def _mk(name: str, fn: NodeFn) -> Callable[[State], State]:
        def _f(s: State) -> State:
            return _timed_node(settings, s, name, fn)
        return _f

    def _mk_assembly() -> Callable[[State], State]:
        def _f(s: State) -> State:
            return _assembly_todo_nonfatal(settings, s)
        return _f

    return {
        "load_sheet_data": _mk("load_sheet_data", load_sheet_data),
        "generate_assembly_todo": _mk_assembly(),
        "build_thread_snapshot": _mk("build_thread_snapshot", build_thread_snapshot),
        "analyze_media": _mk("analyze_media", analyze_media),
        "analyze_attachments": _mk("analyze_attachments", analyze_attachments),
        "upsert_vectors": _mk("upsert_vectors", upsert_vectors),
        "retrieve_context": _mk("retrieve_context", retrieve_context),
        "rerank_context": _mk("rerank_context", rerank_context),
        "generate_ai_reply": _mk("generate_ai_reply", generate_ai_reply),
        "annotate_media": _mk("annotate_media", annotate_media),
        "writeback": _mk("writeback", writeback),
    }


def run_event_graph(settings: Settings, payload: Dict[str, Any]) -> Dict[str, Any]:
    event_type = str(payload.get("event_type") or "UNKNOWN").strip()
    primary_id = _primary_id_for_event(payload, event_type)

    runlog = RunLog(settings)
    tenant_id_hint = (_tenant_from_payload(payload) or "UNKNOWN").strip()

    primary_id_scoped = _scoped_primary_id_for_run(payload, primary_id=primary_id)
    run_id = runlog.start(tenant_id_hint, event_type, primary_id_scoped)
    token = run_id_var.set(run_id)

    state: State = {
        "payload": payload,
        "run_id": run_id,
        "event_type": event_type,
        "primary_id": primary_id,
        "idempotency_primary_id": primary_id_scoped,
        "logs": [],
    }

    trace_meta = {
        "run_id": run_id,
        "event_type": event_type,
        "primary_id": primary_id,
        "idempotency_primary_id": primary_id_scoped,
        "tenant_hint": tenant_id_hint,
    }

    try:
        with tracing_context(trace_meta):
            if event_type not in _ALLOWED_EVENT_TYPES:
                msg = f"Skipping pipeline for event_type='{event_type}' (allowed={sorted(_ALLOWED_EVENT_TYPES)})"
                _ensure_logs(state).append(msg)
                logger.info(msg)
                runlog.success(run_id)
                return {
                    "run_id": run_id,
                    "ok": True,
                    "skipped": True,
                    "primary_id": primary_id,
                    "event_type": event_type,
                    "logs": state.get("logs"),
                }

            # ---- Keep incremental ingest fast-paths (not part of checkin LangGraph yet) ----
            if event_type == "CCP_UPDATED":
                ccp_id = str(payload.get("ccp_id") or "").strip()
                if not ccp_id:
                    runlog.success(run_id)
                    return {"run_id": run_id, "ok": True, "skipped": True, "reason": "missing ccp_id", "logs": state.get("logs")}

                from .ingest.ccp_ingest import ingest_ccp_one
                out = ingest_ccp_one(settings, ccp_id=ccp_id)

                # best-effort refresh assembly checklist
                state = _assembly_todo_nonfatal(settings, state)

                runlog.success(run_id)
                return {"run_id": run_id, "ok": True, "event_type": event_type, "ccp_id": ccp_id, "result": out, "logs": state.get("logs")}

            if event_type == "DASHBOARD_UPDATED":
                dashboard_row_id = str(
                    payload.get("dashboard_update_id")
                    or payload.get("dashboard_row_id")
                    or payload.get("row_id")
                    or ""
                ).strip()

                if dashboard_row_id:
                    from .ingest.dashboard_ingest import ingest_dashboard_one_by_row_id
                    out = ingest_dashboard_one_by_row_id(settings, dashboard_row_id=dashboard_row_id)
                    state = _assembly_todo_nonfatal(settings, state)

                    runlog.success(run_id)
                    return {
                        "run_id": run_id,
                        "ok": True,
                        "event_type": event_type,
                        "dashboard_row_id": dashboard_row_id,
                        "result": out,
                        "assembly_todo_written": state.get("assembly_todo_written"),
                        "logs": state.get("logs"),
                    }

                legacy_id = str(payload.get("legacy_id") or "").strip()
                if not legacy_id:
                    runlog.success(run_id)
                    return {"run_id": run_id, "ok": True, "skipped": True, "reason": "missing dashboard_row_id and legacy_id", "logs": state.get("logs")}

                from .ingest.dashboard_ingest import ingest_dashboard_one
                out = ingest_dashboard_one(settings, legacy_id=legacy_id)
                state = _assembly_todo_nonfatal(settings, state)

                runlog.success(run_id)
                return {
                    "run_id": run_id,
                    "ok": True,
                    "event_type": event_type,
                    "legacy_id": legacy_id,
                    "result": out,
                    "assembly_todo_written": state.get("assembly_todo_written"),
                    "logs": state.get("logs"),
                }

            if event_type == "PROJECT_UPDATED":
                state = _assembly_todo_nonfatal(settings, state)
                runlog.success(run_id)
                return {
                    "run_id": run_id,
                    "ok": True,
                    "event_type": event_type,
                    "legacy_id": str(payload.get("legacy_id") or "").strip(),
                    "assembly_todo_written": state.get("assembly_todo_written"),
                    "logs": state.get("logs"),
                }

            # ---- LangGraph orchestration for checkin/conversation flow ----
            state["__lg_bound_nodes__"] = _bind_nodes(settings)

            app = _build_langgraph_app()
            final_state: State = app.invoke(state)

            tenant_id = str(final_state.get("tenant_id") or "").strip()
            if tenant_id and tenant_id != tenant_id_hint:
                runlog.update_tenant(run_id, tenant_id)

            runlog.success(run_id)

            return {
                "run_id": run_id,
                "ok": True,
                "primary_id": primary_id,
                "event_type": event_type,
                "ai_reply": final_state.get("ai_reply"),
                "writeback_done": final_state.get("writeback_done"),
                "logs": final_state.get("logs"),
            }

    except Exception as e:
        runlog.error(run_id, str(e))
        logger.exception("ERROR: %s", e)
        return {
            "run_id": run_id,
            "ok": False,
            "error": str(e),
            "primary_id": primary_id,
            "event_type": event_type,
            "logs": state.get("logs"),
        }

    finally:
        try:
            from ..tools.langsmith_trace import flush_traces
            flush_traces()
        except Exception:
            pass

        run_id_var.reset(token)