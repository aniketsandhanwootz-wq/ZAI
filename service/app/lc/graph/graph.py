# service/app/lc/graph/graph.py
from __future__ import annotations

import importlib
import logging
import time
from typing import Any, Callable, Dict, List, Literal, cast

from langgraph.graph import StateGraph, END

from ...config import Settings
from ...logctx import run_id_var
from ...pipeline.ingest.run_log import RunLog
from .state import LCState
from ..observability.langsmith import get_callbacks

logger = logging.getLogger("zai.lc.graph")

Node = Callable[[LCState], LCState]

_ALLOWED_EVENT_TYPES = {
    "CHECKIN_CREATED",
    "CHECKIN_UPDATED",
    "CONVERSATION_ADDED",
    "CCP_UPDATED",
    "DASHBOARD_UPDATED",
    "PROJECT_UPDATED",
    "MANUAL_TRIGGER",
}

# ---------- helpers (ported 1:1) ----------

def _truthy(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return x != 0
    if isinstance(x, str):
        return x.strip().lower() in ("1", "true", "yes", "y", "on")
    return bool(x)

def _meta(payload: Dict[str, Any]) -> Dict[str, Any]:
    m = payload.get("meta") or {}
    return m if isinstance(m, dict) else {}

def _tenant_from_payload(payload: Dict[str, Any]) -> str:
    rmeta = payload.get("meta") or {}
    return str(rmeta.get("tenant_id") or "")

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
    m = _meta(payload)
    ingest_only = _truthy(m.get("ingest_only") or m.get("skip_reply") or m.get("skip_ai_reply"))
    media_only = _truthy(m.get("media_only"))
    if ingest_only and media_only:
        return f"{primary_id}::MEDIA_V1"
    if ingest_only:
        return f"{primary_id}::INGEST_V1"
    return primary_id

def _clean_lines(items: List[Any], *, max_items: int) -> List[str]:
    out: List[str] = []
    for x in items or []:
        s = str(x or "").strip()
        if not s:
            continue
        out.append(s)
        if len(out) >= max_items:
            break
    return out

# ---------- Legacy node resolver/wrapper ----------
def _resolve_legacy_node(module_rel: str, candidates: List[str]) -> Callable[[Settings, Dict[str, Any]], Dict[str, Any]]:
    mod = importlib.import_module(module_rel, package="service.app.pipeline")
    for name in candidates:
        fn = getattr(mod, name, None)
        if callable(fn):
            return fn  # type: ignore
    raise ImportError(f"Cannot find callable in {module_rel} tried={candidates}")

def _wrap_legacy(settings: Settings, legacy_fn: Callable[[Settings, Dict[str, Any]], Dict[str, Any]], name: str) -> Node:
    def node(state: LCState) -> LCState:
        t0 = time.time()
        logger.info("node:start %s", name)
        # legacy expects Dict[str,Any]
        out = legacy_fn(settings, cast(Dict[str, Any], state))
        dt = (time.time() - t0) * 1000
        logger.info("node:end %s ms=%.1f", name, dt)
        return cast(LCState, out)
    return node

# ---------- Build graph ----------
def build_graph(settings: Settings, runlog: RunLog) -> Any:
    # Resolve legacy nodes (we will replace these with LC-native later)
    load_sheet_data = _wrap_legacy(settings, _resolve_legacy_node("service.app.pipeline.nodes.load_sheet_data", ["load_sheet_data_node", "load_sheet_data", "run", "node"]), "load_sheet_data")
    build_thread_snapshot = _wrap_legacy(settings, _resolve_legacy_node("service.app.pipeline.nodes.build_thread_snapshot", ["build_thread_snapshot_node", "build_thread_snapshot", "run", "node"]), "build_thread_snapshot")
    analyze_media = _wrap_legacy(settings, _resolve_legacy_node("service.app.pipeline.nodes.analyze_media", ["analyze_media", "run", "node"]), "analyze_media")
    analyze_attachments = _wrap_legacy(settings, _resolve_legacy_node("service.app.pipeline.nodes.analyze_attachments", ["analyze_attachments", "run", "node"]), "analyze_attachments")
    upsert_vectors = _wrap_legacy(settings, _resolve_legacy_node("service.app.pipeline.nodes.upsert_vectors", ["upsert_vectors_node", "upsert_vectors", "run", "node"]), "upsert_vectors")
    retrieve_context = _wrap_legacy(settings, _resolve_legacy_node("service.app.pipeline.nodes.retrieve_context", ["retrieve_context_node", "retrieve_context", "run", "node"]), "retrieve_context")
    rerank_context = _wrap_legacy(settings, _resolve_legacy_node("service.app.pipeline.nodes.rerank_context", ["rerank_context", "run", "node"]), "rerank_context")
    generate_ai_reply = _wrap_legacy(settings, _resolve_legacy_node("service.app.pipeline.nodes.generate_ai_reply", ["generate_ai_reply_node", "generate_ai_reply", "run", "node"]), "generate_ai_reply")
    annotate_media = _wrap_legacy(settings, _resolve_legacy_node("service.app.pipeline.nodes.annotate_media", ["annotate_media", "run", "node"]), "annotate_media")
    writeback = _wrap_legacy(settings, _resolve_legacy_node("service.app.pipeline.nodes.writeback", ["writeback_node", "writeback", "run", "node"]), "writeback")
    generate_assembly_todo = _wrap_legacy(settings, _resolve_legacy_node("service.app.pipeline.nodes.generate_assembly_todo", ["generate_assembly_todo", "run", "node"]), "generate_assembly_todo")

    # Router/init nodes
    def init_state(state: LCState) -> LCState:
        payload = state["payload"]
        event_type = str(payload.get("event_type") or "UNKNOWN").strip()
        primary_id = _primary_id_for_event(payload, event_type)
        state["event_type"] = event_type
        state["primary_id"] = primary_id
        state["idempotency_primary_id"] = _scoped_primary_id_for_run(payload, primary_id=primary_id)
        state.setdefault("logs", [])
        # Compute flags once
        m = _meta(payload)
        force_reply = _truthy(m.get("force_reply"))
        ingest_only_default = event_type in ("CHECKIN_UPDATED", "CONVERSATION_ADDED")
        ingest_only = _truthy(m.get("ingest_only") or m.get("skip_reply") or m.get("skip_ai_reply")) or ingest_only_default
        if event_type == "CHECKIN_CREATED" and force_reply:
            ingest_only = False
        media_only = _truthy(m.get("media_only"))
        state["force_reply"] = force_reply
        state["ingest_only"] = ingest_only
        state["media_only"] = media_only
        return state

    def skip_node(state: LCState) -> LCState:
        msg = f"Skipping pipeline for event_type='{state.get('event_type')}' (allowed={sorted(_ALLOWED_EVENT_TYPES)})"
        state.setdefault("logs", []).append(msg)
        return state

    # CCP/Dashboard ingest nodes are “side effects” like legacy
    def ccp_updated_node(state: LCState) -> LCState:
        payload = state["payload"]
        ccp_id = str(payload.get("ccp_id") or "").strip()
        if not ccp_id:
            state.setdefault("logs", []).append("CCP_UPDATED: missing ccp_id; skipped")
            return state
        from service.app.pipeline.ingest.ccp_ingest import ingest_ccp_one
        ingest_ccp_one(settings, ccp_id=ccp_id)
        # refresh assembly todo best-effort
        try:
            state = generate_assembly_todo(state)
        except Exception as e:
            state.setdefault("logs", []).append(f"assembly_todo: non-fatal after CCP_UPDATED: {e}")
        return state

    def dashboard_updated_node(state: LCState) -> LCState:
        payload = state["payload"]
        dashboard_row_id = str(
            payload.get("dashboard_update_id")
            or payload.get("dashboard_row_id")
            or payload.get("row_id")
            or ""
        ).strip()

        if dashboard_row_id:
            from service.app.pipeline.ingest.dashboard_ingest import ingest_dashboard_one_by_row_id
            ingest_dashboard_one_by_row_id(settings, dashboard_row_id=dashboard_row_id)
        else:
            legacy_id = str(payload.get("legacy_id") or "").strip()
            if not legacy_id:
                state.setdefault("logs", []).append("DASHBOARD_UPDATED: missing row_id and legacy_id; skipped")
                return state
            from service.app.pipeline.ingest.dashboard_ingest import ingest_dashboard_one
            ingest_dashboard_one(settings, legacy_id=legacy_id)

        try:
            state = generate_assembly_todo(state)
        except Exception as e:
            state.setdefault("logs", []).append(f"assembly_todo: non-fatal after DASHBOARD_UPDATED: {e}")
        return state

    def project_updated_node(state: LCState) -> LCState:
        try:
            return generate_assembly_todo(state)
        except Exception as e:
            state.setdefault("logs", []).append(f"PROJECT_UPDATED: assembly_todo non-fatal: {e}")
            return state

    def update_tenant_node(state: LCState) -> LCState:
        # legacy updates tenant after load_sheet_data
        tenant_id = str(state.get("tenant_id") or "").strip()
        hint = (_tenant_from_payload(state["payload"]) or "UNKNOWN").strip()
        if tenant_id and tenant_id != hint:
            try:
                runlog.update_tenant(state["run_id"], tenant_id)
            except Exception as e:
                state.setdefault("logs", []).append(f"runlog.update_tenant non-fatal: {e}")
        return state

    def ingest_media_only_node(state: LCState) -> LCState:
        caps = state.get("image_captions") or []
        cap_lines = _clean_lines(caps, max_items=12)
        if not cap_lines:
            state.setdefault("logs", []).append("ingest_only(media_only): no captions found; skipping MEDIA vector")
            state["media_upserted"] = False  # type: ignore
            return state

        tenant_id = str(state.get("tenant_id") or "").strip()
        checkin_id = str(state.get("checkin_id") or "").strip()
        if not tenant_id or not checkin_id:
            state.setdefault("logs", []).append("ingest_only(media_only): missing tenant/checkin; cannot upsert")
            state["media_upserted"] = False  # type: ignore
            return state

        from service.app.tools.embed_tool import EmbedTool
        from service.app.tools.vector_tool import VectorTool

        media_text = "MEDIA CAPTIONS (from inspection photos/docs):\n" + "\n".join([f"- {c}" for c in cap_lines])
        emb = EmbedTool(settings).embed_text(media_text)
        VectorTool(settings).upsert_incident_vector(
            tenant_id=tenant_id,
            checkin_id=checkin_id,
            vector_type="MEDIA",
            embedding=emb,
            project_name=state.get("project_name"),
            part_number=state.get("part_number"),
            legacy_id=state.get("legacy_id"),
            status=state.get("checkin_status") or "",
            text=media_text,
        )
        state.setdefault("logs", []).append(f"ingest_only(media_only): upserted MEDIA vector (captions={len(cap_lines)})")
        state["media_upserted"] = True  # type: ignore
        return state

    # ---- routing conditions ----
    def route_event(state: LCState) -> str:
        et = str(state.get("event_type") or "UNKNOWN").strip()
        if et not in _ALLOWED_EVENT_TYPES:
            return "skip"
        if et == "CCP_UPDATED":
            return "ccp"
        if et == "DASHBOARD_UPDATED":
            return "dash"
        if et == "PROJECT_UPDATED":
            return "project"
        return "checkin_flow"

    def route_checkin_flow(state: LCState) -> str:
        # after analyze_attachments: decide ingest_only/media_only vs normal
        if bool(state.get("ingest_only")):
            if bool(state.get("media_only")):
                return "ingest_media_only"
            return "ingest_vectors_only"
        # normal pipeline safety: only reply/writeback for CHECKIN_CREATED
        if str(state.get("event_type") or "") != "CHECKIN_CREATED":
            return "non_created_refresh"
        return "created_full"

    # ---- build graph ----
    g = StateGraph(LCState)
    g.add_node("init_state", init_state)
    g.add_node("skip", skip_node)

    g.add_node("ccp_updated", ccp_updated_node)
    g.add_node("dashboard_updated", dashboard_updated_node)
    g.add_node("project_updated", project_updated_node)

    g.add_node("load_sheet_data", load_sheet_data)
    g.add_node("update_tenant", update_tenant_node)
    g.add_node("generate_assembly_todo", generate_assembly_todo)
    g.add_node("build_thread_snapshot", build_thread_snapshot)
    g.add_node("analyze_media", analyze_media)
    g.add_node("analyze_attachments", analyze_attachments)

    g.add_node("ingest_media_only", ingest_media_only_node)
    g.add_node("upsert_vectors", upsert_vectors)

    g.add_node("retrieve_context", retrieve_context)
    g.add_node("rerank_context", rerank_context)
    g.add_node("generate_ai_reply", generate_ai_reply)
    g.add_node("annotate_media", annotate_media)
    g.add_node("writeback", writeback)

    # edges
    g.set_entry_point("init_state")
    g.add_conditional_edges("init_state", route_event, {
        "skip": "skip",
        "ccp": "ccp_updated",
        "dash": "dashboard_updated",
        "project": "project_updated",
        "checkin_flow": "load_sheet_data",
    })

    g.add_edge("skip", END)
    g.add_edge("ccp_updated", END)
    g.add_edge("dashboard_updated", END)
    g.add_edge("project_updated", END)

    # checkin flow edges
    g.add_edge("load_sheet_data", "update_tenant")
    g.add_edge("update_tenant", "generate_assembly_todo")
    g.add_edge("generate_assembly_todo", "build_thread_snapshot")
    g.add_edge("build_thread_snapshot", "analyze_media")
    g.add_edge("analyze_media", "analyze_attachments")

    g.add_conditional_edges("analyze_attachments", route_checkin_flow, {
        "ingest_media_only": "ingest_media_only",
        "ingest_vectors_only": "upsert_vectors",
        "non_created_refresh": "upsert_vectors",
        "created_full": "retrieve_context",
    })

    g.add_edge("ingest_media_only", END)
    g.add_edge("upsert_vectors", END)

    g.add_edge("retrieve_context", "rerank_context")
    g.add_edge("rerank_context", "generate_ai_reply")
    g.add_edge("generate_ai_reply", "annotate_media")
    g.add_edge("annotate_media", "upsert_vectors")
    g.add_edge("upsert_vectors", "writeback")
    g.add_edge("writeback", END)

    return g.compile()

# ---------- Public entry (LangGraph equivalent of run_event_graph) ----------
def run_event_graph_lc(settings: Settings, payload: Dict[str, Any]) -> Dict[str, Any]:
    event_type = str(payload.get("event_type") or "UNKNOWN").strip()
    primary_id = _primary_id_for_event(payload, event_type)

    runlog = RunLog(settings)
    tenant_id_hint = (_tenant_from_payload(payload) or "UNKNOWN").strip()

    primary_id_scoped = _scoped_primary_id_for_run(payload, primary_id=primary_id)
    run_id = runlog.start(tenant_id_hint, event_type, primary_id_scoped)
    token = run_id_var.set(run_id)

    state: LCState = {
        "payload": payload,
        "run_id": run_id,
        "event_type": event_type,
        "primary_id": primary_id,
        "idempotency_primary_id": primary_id_scoped,
        "logs": [],
    }

    app = build_graph(settings, runlog)
    callbacks = get_callbacks(run_id, extra={"event_type": event_type, "primary_id": primary_id_scoped})

    try:
        out = app.invoke(state, config={"callbacks": callbacks})
        runlog.success(run_id)
        return {
            "run_id": run_id,
            "ok": True,
            "primary_id": primary_id,
            "event_type": event_type,
            "ai_reply": out.get("ai_reply"),
            "writeback_done": out.get("writeback_done"),
            "assembly_todo_written": out.get("assembly_todo_written"),
            "logs": out.get("logs"),
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
        run_id_var.reset(token)