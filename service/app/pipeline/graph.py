# service/app/pipeline/graph.py
# This module defines the data pipeline graph for processing events such as checkin creation,
# conversation additions, CCP updates, and dashboard updates. It resolves and orchestrates
# various processing nodes to handle data ingestion, context retrieval, AI reply generation,
# media analysis, vector upsertion, and writeback to the source system. The graph is
# designed to be modular and extensible, allowing for easy addition of new processing nodes
# and workflows as needed.
# It also includes logic for handling idempotency, event type filtering, and specialized processing
# for different event types.
# The main function `run_event_graph` executes the pipeline based on the event type and payload,
# returning the results and logs of the processing.
# It leverages a RunLog for tracking execution and supports various modes such as
# ingest-only and media-only processing.
# The module is structured to facilitate maintenance and future enhancements.
# Happy coding!


from __future__ import annotations

import importlib
import logging
import time
from typing import Any, Callable, Dict, List

from ..config import Settings
from ..logctx import run_id_var
from .ingest.run_log import RunLog

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


# Checkin pipeline nodes
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


_ALLOWED_EVENT_TYPES = {
    "CHECKIN_CREATED",
    "CHECKIN_UPDATED",
    "CONVERSATION_ADDED",
    "CCP_UPDATED",
    "DASHBOARD_UPDATED",
    "PROJECT_UPDATED",   # NEW (cron/status->mfg trigger)
    "MANUAL_TRIGGER",
}

def _primary_id_for_event(payload: Dict[str, Any], event_type: str) -> str:
    """
    Idempotency primary_id MUST be the entity's own unique id:
      - CHECKIN_*            -> checkin_id
      - CONVERSATION_ADDED   -> conversation_id (NOT checkin_id)
      - CCP_UPDATED          -> ccp_id
      - DASHBOARD_UPDATED    -> dashboard_update_id / row_id (NOT legacy_id)
      - MANUAL_TRIGGER       -> meta.primary_id if provided else fallback
    """
    event_type = (event_type or "").strip()

    checkin_id = str(payload.get("checkin_id") or "").strip()
    conversation_id = str(payload.get("conversation_id") or "").strip()
    ccp_id = str(payload.get("ccp_id") or "").strip()

    # Dashboard update unique id: prefer explicit payload, else row_id variants
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
        # THIS is the key fix
        return conversation_id or "UNKNOWN_CONVO"

    if event_type == "CCP_UPDATED":
        return ccp_id or "UNKNOWN_CCP"

    if event_type == "DASHBOARD_UPDATED":
        # prefer the actual Row ID trigger
        return dashboard_row_id or legacy_id or "UNKNOWN_DASH"

    # MANUAL / fallback
    m = _meta(payload)
    meta_primary = str(m.get("primary_id") or "").strip()
    return meta_primary or checkin_id or conversation_id or ccp_id or dashboard_row_id or legacy_id or "UNKNOWN"

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

def _scoped_primary_id_for_run(payload: Dict[str, Any], *, event_type: str, primary_id: str) -> str:
    """
    Make idempotency key include "mode" so backfills don't collide with earlier runs.

    Examples:
      - normal webhook: primary_id stays same
      - media-only ingest backfill: "<id>::MEDIA_V1"
      - ingest-only (non-media): "<id>::INGEST_V1"
    """
    m = _meta(payload)

    ingest_only = _truthy(m.get("ingest_only") or m.get("skip_reply") or m.get("skip_ai_reply"))
    media_only = _truthy(m.get("media_only"))

    if ingest_only and media_only:
        return f"{primary_id}::MEDIA_V1"
    if ingest_only:
        return f"{primary_id}::INGEST_V1"

    # Keep default behavior for normal events
    return primary_id

def run_event_graph(settings: Settings, payload: Dict[str, Any]) -> Dict[str, Any]:
    event_type = str(payload.get("event_type") or "UNKNOWN").strip()
    primary_id = _primary_id_for_event(payload, event_type)

    runlog = RunLog(settings)
    tenant_id_hint = (_tenant_from_payload(payload) or "UNKNOWN").strip()

    primary_id_scoped = _scoped_primary_id_for_run(payload, event_type=event_type, primary_id=primary_id)
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

    def _timed(name: str, fn: NodeFn) -> State:
        t0 = time.time()
        logger.info("node:start %s", name)
        out = fn(settings, state)
        dt = (time.time() - t0) * 1000
        logger.info("node:end %s ms=%.1f", name, dt)
        return out

    try:
        if event_type not in _ALLOWED_EVENT_TYPES:
            msg = f"Skipping pipeline for event_type='{event_type}' (allowed={sorted(_ALLOWED_EVENT_TYPES)})"
            (state.get("logs") or []).append(msg)
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

        m = _meta(payload)

        # -------------------------
        # CCP incremental ingestion
        # -------------------------
        if event_type == "CCP_UPDATED":
            ccp_id = str(payload.get("ccp_id") or "").strip()
            if not ccp_id:
                runlog.success(run_id)
                return {"run_id": run_id, "ok": True, "skipped": True, "reason": "missing ccp_id", "logs": state.get("logs")}

            from .ingest.ccp_ingest import ingest_ccp_one

            out = ingest_ccp_one(settings, ccp_id=ccp_id)
            # Also refresh assembly checklist if project is already in MFG
            try:
                state["payload"] = payload
                state = _timed("generate_assembly_todo", generate_assembly_todo)
            except Exception as _e:
                (state.get("logs") or []).append(f"assembly_todo: non-fatal after CCP_UPDATED: {_e}")
            runlog.success(run_id)
            return {"run_id": run_id, "ok": True, "event_type": event_type, "ccp_id": ccp_id, "result": out, "logs": state.get("logs")}

        # ------------------------------
        # Dashboard incremental ingestion
        # ------------------------------
        if event_type == "DASHBOARD_UPDATED":
            dashboard_row_id = str(
                payload.get("dashboard_update_id")
                or payload.get("dashboard_row_id")
                or payload.get("row_id")
                or ""
            ).strip()

            # Prefer Row ID ingestion (correct trigger)
            if dashboard_row_id:
                from .ingest.dashboard_ingest import ingest_dashboard_one_by_row_id

                out = ingest_dashboard_one_by_row_id(settings, dashboard_row_id=dashboard_row_id)

                # Also refresh assembly checklist if project is already in MFG
                try:
                    state["payload"] = payload
                    state = _timed("generate_assembly_todo", generate_assembly_todo)
                except Exception as _e:
                    (state.get("logs") or []).append(f"assembly_todo: non-fatal after DASHBOARD_UPDATED(row_id): {_e}")

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

            # Fallback to legacy_id ingestion (older payloads)
            legacy_id = str(payload.get("legacy_id") or "").strip()
            if not legacy_id:
                runlog.success(run_id)
                return {"run_id": run_id, "ok": True, "skipped": True, "reason": "missing dashboard_row_id and legacy_id", "logs": state.get("logs")}

            from .ingest.dashboard_ingest import ingest_dashboard_one

            out = ingest_dashboard_one(settings, legacy_id=legacy_id)

            # Also refresh assembly checklist if project is already in MFG
            try:
                state["payload"] = payload
                state = _timed("generate_assembly_todo", generate_assembly_todo)
            except Exception as _e:
                (state.get("logs") or []).append(f"assembly_todo: non-fatal after DASHBOARD_UPDATED(legacy_id): {_e}")

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
        # -------------------------
        # Project status trigger (cron -> mfg)
        # -------------------------
        if event_type == "PROJECT_UPDATED":
            # payload should contain legacy_id (Project.ID)
            state = _timed("generate_assembly_todo", generate_assembly_todo)
            runlog.success(run_id)
            return {
                "run_id": run_id,
                "ok": True,
                "event_type": event_type,
                "legacy_id": str(payload.get("legacy_id") or "").strip(),
                "assembly_todo_written": state.get("assembly_todo_written"),
                "logs": state.get("logs"),
            }
        # -------------------------
        # Checkin / Conversation flow
        # -------------------------
        # Reply/writeback ONLY for CHECKIN_CREATED (your requirement)
        force_reply = _truthy(m.get("force_reply"))
        ingest_only_default = event_type in ("CHECKIN_UPDATED", "CONVERSATION_ADDED")
        ingest_only = _truthy(m.get("ingest_only") or m.get("skip_reply") or m.get("skip_ai_reply")) or ingest_only_default
        if event_type == "CHECKIN_CREATED" and force_reply:
            ingest_only = False

        media_only = _truthy(m.get("media_only"))

        state = _timed("load_sheet_data", load_sheet_data)

        # Always try to refresh assembly checklist after any relevant event.
        # Node itself will skip unless Project.Status_assembly == 'mfg'.
        try:
            state = _timed("generate_assembly_todo", generate_assembly_todo)
        except Exception as _e:
            (state.get("logs") or []).append(f"assembly_todo: non-fatal failure: {_e}")

        tenant_id = str(state.get("tenant_id") or "").strip()
        if tenant_id and tenant_id != tenant_id_hint:
            runlog.update_tenant(run_id, tenant_id)

        state = _timed("build_thread_snapshot", build_thread_snapshot)
        state = _timed("analyze_media", analyze_media)
        # NEW: ingest + analyze "Files" attachments (idempotent)
        state = _timed("analyze_attachments", analyze_attachments)

        # ingest-only modes
        if ingest_only:
            if media_only:
                caps = state.get("image_captions") or []
                cap_lines = _clean_lines(caps, max_items=12)
                if not cap_lines:
                    (state.get("logs") or []).append("ingest_only(media_only): no captions found; skipping MEDIA vector")
                    runlog.success(run_id)
                    return {
                        "run_id": run_id,
                        "ok": True,
                        "primary_id": primary_id,
                        "event_type": event_type,
                        "ingest_only": True,
                        "media_only": True,
                        "media_upserted": False,
                        "logs": state.get("logs"),
                    }

                from ..tools.embed_tool import EmbedTool
                from ..tools.vector_tool import VectorTool

                checkin_id = str(state.get("checkin_id") or "").strip()
                if not tenant_id or not checkin_id:
                    (state.get("logs") or []).append("ingest_only(media_only): missing tenant/checkin; cannot upsert")
                    runlog.success(run_id)
                    return {
                        "run_id": run_id,
                        "ok": True,
                        "primary_id": primary_id,
                        "event_type": event_type,
                        "ingest_only": True,
                        "media_only": True,
                        "media_upserted": False,
                        "logs": state.get("logs"),
                    }

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
                (state.get("logs") or []).append(f"ingest_only(media_only): upserted MEDIA vector (captions={len(cap_lines)})")

                runlog.success(run_id)
                return {
                    "run_id": run_id,
                    "ok": True,
                    "primary_id": primary_id,
                    "event_type": event_type,
                    "ingest_only": True,
                    "media_only": True,
                    "media_upserted": True,
                    "logs": state.get("logs"),
                }

            state = _timed("upsert_vectors", upsert_vectors)
            runlog.success(run_id)
            logger.info("SUCCESS(ingest_only) primary_id=%s", primary_id)
            return {
                "run_id": run_id,
                "ok": True,
                "primary_id": primary_id,
                "event_type": event_type,
                "ingest_only": True,
                "logs": state.get("logs"),
            }

        # normal pipeline (reply/writeback happens only for CHECKIN_CREATED)
        if event_type != "CHECKIN_CREATED":
            # safety: even if caller didn't set ingest_only, we won't reply for other events
            state = _timed("upsert_vectors", upsert_vectors)
            runlog.success(run_id)
            return {
                "run_id": run_id,
                "ok": True,
                "primary_id": primary_id,
                "event_type": event_type,
                "note": "Non-created event: vectors refreshed, no reply/writeback.",
                "logs": state.get("logs"),
            }

        state = _timed("retrieve_context", retrieve_context)
        state = _timed("rerank_context", rerank_context)
        state = _timed("generate_ai_reply", generate_ai_reply)
        state = _timed("annotate_media", annotate_media)
        state = _timed("upsert_vectors", upsert_vectors)
        state = _timed("writeback", writeback)

        runlog.success(run_id)
        logger.info("SUCCESS primary_id=%s", primary_id)

        return {
            "run_id": run_id,
            "ok": True,
            "primary_id": primary_id,
            "event_type": event_type,
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
            "event_type": event_type,
            "logs": state.get("logs"),
        }

    finally:
        run_id_var.reset(token)
