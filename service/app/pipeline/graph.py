# service/app/pipeline/graph.py
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


load_sheet_data = _resolve_node(".nodes.load_sheet_data", ["load_sheet_data_node", "load_sheet_data", "run", "node"])
build_thread_snapshot = _resolve_node(".nodes.build_thread_snapshot", ["build_thread_snapshot_node", "build_thread_snapshot", "run", "node"])
analyze_media = _resolve_node(".nodes.analyze_media", ["analyze_media", "run", "node"])
upsert_vectors = _resolve_node(".nodes.upsert_vectors", ["upsert_vectors_node", "upsert_vectors", "run", "node"])
retrieve_context = _resolve_node(".nodes.retrieve_context", ["retrieve_context_node", "retrieve_context", "run", "node"])
rerank_context = _resolve_node(".nodes.rerank_context", ["rerank_context", "run", "node"])
generate_ai_reply = _resolve_node(".nodes.generate_ai_reply", ["generate_ai_reply_node", "generate_ai_reply", "run", "node"])
writeback = _resolve_node(".nodes.writeback", ["writeback_node", "writeback", "run", "node"])


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


# ✅ only run pipeline for checkin created
_ALLOWED_EVENT_TYPES = {"CHECKIN_CREATED"}


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


def run_event_graph(settings: Settings, payload: Dict[str, Any]) -> Dict[str, Any]:
    event_type = str(payload.get("event_type") or "UNKNOWN").strip()
    primary_id = str(
        payload.get("checkin_id")
        or payload.get("conversation_id")
        or payload.get("ccp_id")
        or payload.get("legacy_id")
        or "UNKNOWN"
    ).strip()

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
        # ✅ HARD GATE
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
        ingest_only = _truthy(m.get("ingest_only") or m.get("skip_reply") or m.get("skip_ai_reply"))
        media_only = _truthy(m.get("media_only"))

        state = _timed("load_sheet_data", load_sheet_data)

        tenant_id = str(state.get("tenant_id") or "").strip()
        if tenant_id and tenant_id != tenant_id_hint:
            runlog.update_tenant(run_id, tenant_id)

        state = _timed("build_thread_snapshot", build_thread_snapshot)
        state = _timed("analyze_media", analyze_media)

        # ✅ ingest-only modes
        if ingest_only:
            # media-only: caption artifacts already stored in analyze_media
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

            # full ingest-only: store vectors but do NOT do retrieval / reply / writeback
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

        # normal pipeline
        state = _timed("retrieve_context", retrieve_context)
        state = _timed("rerank_context", rerank_context)
        state = _timed("generate_ai_reply", generate_ai_reply)

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
