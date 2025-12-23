from __future__ import annotations

import importlib
import logging
import time
from typing import Any, Callable, Dict, List

from ..config import Settings
from .ingest.run_log import RunLog
from ..logctx import run_id_var

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


load_sheet_data = _resolve_node(".nodes.load_sheet_data", ["load_sheet_data_node", "load_sheet_data", "run", "node"])
build_thread_snapshot = _resolve_node(".nodes.build_thread_snapshot", ["build_thread_snapshot_node", "build_thread_snapshot", "run", "node"])
upsert_vectors = _resolve_node(".nodes.upsert_vectors", ["upsert_vectors_node", "upsert_vectors", "run", "node"])
retrieve_context = _resolve_node(".nodes.retrieve_context", ["retrieve_context_node", "retrieve_context", "run", "node"])
rerank_context = _resolve_node(".nodes.rerank_context", ["rerank_context", "run", "node"])
generate_ai_reply = _resolve_node(".nodes.generate_ai_reply", ["generate_ai_reply_node", "generate_ai_reply", "run", "node"])
writeback = _resolve_node(".nodes.writeback", ["writeback_node", "writeback", "run", "node"])


def _tenant_from_payload(payload: Dict[str, Any]) -> str:
    rmeta = payload.get("meta") or {}
    return str(rmeta.get("tenant_id") or "")


def run_event_graph(settings: Settings, payload: Dict[str, Any]) -> Dict[str, Any]:
    event_type = str(payload.get("event_type") or "UNKNOWN")
    primary_id = str(
        payload.get("checkin_id")
        or payload.get("conversation_id")
        or payload.get("ccp_id")
        or payload.get("legacy_id")
        or "UNKNOWN"
    )

    runlog = RunLog(settings)
    tenant_id_hint = (_tenant_from_payload(payload) or "UNKNOWN").strip()
    run_id = runlog.start(tenant_id_hint, event_type, primary_id)

    token = run_id_var.set(run_id)

    state: State = {
        "payload": payload,
        "run_id": run_id,
        "event_type": event_type,
        "primary_id": primary_id,
        "logs": [],
    }

    def _timed(name: str, fn: NodeFn) -> State:
        t0 = time.time()
        logger.info("node:start %s", name)
        out = fn(settings, state)
        dt = (time.time() - t0) * 1000
        logger.info("node:end %s ms=%.1f", name, dt)
        return out

    try:
        state = _timed("load_sheet_data", load_sheet_data)

        tenant_id = str(state.get("tenant_id") or "").strip()
        if tenant_id and tenant_id != tenant_id_hint:
            runlog.update_tenant(run_id, tenant_id)

        state = _timed("build_thread_snapshot", build_thread_snapshot)
        state = _timed("upsert_vectors", upsert_vectors)
        state = _timed("retrieve_context", retrieve_context)
        state = _timed("rerank_context", rerank_context)
        state = _timed("generate_ai_reply", generate_ai_reply)
        state = _timed("writeback", writeback)

        runlog.success(run_id)
        logger.info("SUCCESS primary_id=%s", primary_id)

        return {
            "run_id": run_id,
            "ok": True,
            "primary_id": primary_id,
            "ai_reply": state.get("ai_reply"),
            "writeback_done": state.get("writeback_done"),
            "logs": state.get("logs"),
        }

    except Exception as e:
        runlog.error(run_id, str(e))
        logger.exception("ERROR: %s", e)
        return {
            "run_id": run_id,
            "ok": False,
            "error": str(e),
            "primary_id": primary_id,
            "logs": state.get("logs"),
        }

    finally:
        run_id_var.reset(token)
