# service/app/pipeline/cqts_graph.py
"""
CQTS (Cost/Quality/Timeline/Scope) classification pipeline — daily batch,
separate from the real-time, webhook-driven graph.py. One checkin at a time:

    load_context           -> GET /internal/checkins/:id/cqts-context
    retrieve_and_rerank    -> reuse retrieve_context.py/rerank_context.py as-is,
                               against ZAI's own incident_vectors (unchanged)
    classify                -> structured Gemini call via langchain-google-genai
    validate_and_writeback  -> POST /internal/checkins/:id/cqts-classification
    upsert_vectors           -> reuse upsert_vectors.py as-is (PROBLEM/RESOLUTION)

See the CQTS plan (Part 3) for the full design rationale. LangSmith tracing is
deliberately not wired into this graph — not needed yet, per the plan.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from ..config import Settings
from ..tools.wootzcheckin_client import WootzCheckinClient
from .nodes.retrieve_context import retrieve_context
from .nodes.rerank_context import rerank_context
from .nodes.upsert_vectors import upsert_vectors

logger = logging.getLogger("zai.cqts_graph")

State = Dict[str, Any]
NodeFn = Any


def _timed(name: str, fn, settings: Settings, state: State) -> State:
    t0 = time.time()
    logger.info("cqts_node:start %s", name)
    out = fn(settings, state)
    dt = (time.time() - t0) * 1000
    logger.info("cqts_node:end %s ms=%.1f", name, dt)
    return out


# ── Structured output schema ────────────────────────────────────────────────

Severity = str  # "critical" | "moderate" | "watch" | "none" — validated below


class CqtsBucketOut(BaseModel):
    severity: Severity = Field(description="One of: critical, moderate, watch, none")
    title: str = Field(description="Short label, <= 8 words")
    rootCause: str
    recommendedAction: str


class CqtsClassificationOut(BaseModel):
    cost: Optional[CqtsBucketOut] = None
    quality: Optional[CqtsBucketOut] = None
    timeline: Optional[CqtsBucketOut] = None
    scope: Optional[CqtsBucketOut] = None


_VALID_SEVERITIES = {"critical", "moderate", "watch", "none"}


# ── Nodes ────────────────────────────────────────────────────────────────────

def load_context(settings: Settings, state: State) -> State:
    checkin_id = state["checkin_id"]
    client = WootzCheckinClient(settings)
    ctx = client.get_checkin_context(checkin_id)

    checkin = ctx.get("checkin") or {}
    project = ctx.get("project") or {}
    conversations = ctx.get("conversations") or []
    dashboard_updates = ctx.get("dashboardUpdates") or []

    # tenant_id/legacy_id feed retrieve_context.py's company-profile lookup
    # and Glide KB filtering — see the CQTS plan's identifier-mapping section
    # for why these specific wootzcheckin fields map to ZAI's own concepts.
    state["tenant_id"] = project.get("companyRowId") or ""
    state["legacy_id"] = project.get("legacyId") or ""
    state["project_name"] = project.get("projectName") or ""
    state["part_number"] = project.get("partNumber") or ""
    state["checkin_status"] = checkin.get("status") or ""
    state["checkin_description"] = checkin.get("description") or ""
    state["closure_notes"] = checkin.get("resolutionComments") or ""
    state["checkin_images"] = checkin.get("image") or []

    # Compose a chronological thread snapshot — this is both the retrieval
    # query text (retrieve_context._compose_query_text prefers it) and the
    # text embedded as this checkin's own PROBLEM vector (upsert_vectors).
    lines: List[str] = [f"CHECK-IN [{checkin.get('status', '')}]: {checkin.get('description', '')}"]
    for cv in conversations:
        lines.append(
            f"[{cv.get('timestamp', '')}] {cv.get('addedBy', '')}"
            f" ({cv.get('status') or 'comment'}): {cv.get('message', '')}"
        )
    state["thread_snapshot_text"] = "\n".join(lines).strip()

    ccp_lines = [
        f"- {ccp.get('ccpName', '')}: {ccp.get('description') or ''}".strip()
        for ccp in (project.get("ccps") or [])
    ]
    dashboard_lines = [
        f"- [{du.get('added_at', '')}] {du.get('added_by', '')}: {du.get('update_message', '')}"
        for du in dashboard_updates
    ]

    state["cqts_project_context_text"] = "\n".join(
        [
            f"Project: {project.get('projectName', '')} | Part: {project.get('partNumber', '')}",
            f"Assembly status: {project.get('statusAssembly', '')} | Dispatch date: {project.get('dispatchDate') or '(not set)'}",
            "CCPs:" if ccp_lines else "",
            *ccp_lines,
            "Recent dashboard updates:" if dashboard_lines else "",
            *dashboard_lines,
        ]
    ).strip()

    state["cqts_conversation_thread_text"] = "\n".join(
        f"[{cv.get('timestamp', '')}] {cv.get('addedBy', '')} ({cv.get('status') or 'comment'}): {cv.get('message', '')}"
        for cv in conversations
    ).strip() or "(no conversation yet)"

    state.setdefault("logs", []).append(f"load_context: loaded checkin={checkin_id}")
    return state


def retrieve_and_rerank(settings: Settings, state: State) -> State:
    state = retrieve_context(settings, state)
    state = rerank_context(settings, state)
    return state


def _format_vector_memory(state: State) -> str:
    problems = (state.get("similar_problems") or [])[:6]
    resolutions = (state.get("similar_resolutions") or [])[:6]

    lines: List[str] = []
    if problems:
        lines.append("Similar past problems:")
        for p in problems:
            text = (p.get("text") or "").strip()
            if text:
                lines.append(f"- {text[:400]}")
    if resolutions:
        lines.append("Similar past resolutions:")
        for r in resolutions:
            text = (r.get("text") or "").strip()
            if text:
                lines.append(f"- {text[:400]}")

    return "\n".join(lines).strip() or "(no similar history found)"


def _render_prompt(state: State) -> str:
    from pathlib import Path

    # cqts_graph.py lives at service/app/pipeline/ (one level shallower than
    # nodes/*.py), so this is parents[3] to reach the ZAI repo root, not [4].
    template_path = Path(__file__).resolve().parents[3] / "packages" / "prompts" / "cqts_classification.md"
    template = template_path.read_text(encoding="utf-8")

    return (
        template.replace("{checkin_context}", state.get("checkin_description", ""))
        .replace("{project_context}", state.get("cqts_project_context_text", ""))
        .replace("{conversation_thread}", state.get("cqts_conversation_thread_text", ""))
        .replace("{vector_memory}", _format_vector_memory(state))
    )


def classify(settings: Settings, state: State) -> State:
    # Imported lazily so the rest of this module (and the daily script) can
    # still be imported/tested even if langchain-google-genai isn't installed
    # in a given environment.
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_core.messages import HumanMessage

    prompt = _render_prompt(state)

    llm = ChatGoogleGenerativeAI(
        model=settings.llm_model or "gemini-2.5-flash",
        google_api_key=settings.llm_api_key,
        temperature=0.2,
    )
    structured_llm = llm.with_structured_output(CqtsClassificationOut)

    content: List[Dict[str, Any]] = [{"type": "text", "text": prompt}]
    for url in (state.get("checkin_images") or [])[:4]:
        content.append({"type": "image_url", "image_url": {"url": url}})

    result: CqtsClassificationOut = structured_llm.invoke([HumanMessage(content=content)])

    state["cqts_result"] = result
    state.setdefault("logs", []).append("classify: structured Gemini call completed")
    return state


def validate_and_writeback(settings: Settings, state: State) -> State:
    result: CqtsClassificationOut = state["cqts_result"]

    payload: Dict[str, Any] = {}
    for bucket_key in ("cost", "quality", "timeline", "scope"):
        bucket: Optional[CqtsBucketOut] = getattr(result, bucket_key)
        if bucket is None:
            continue
        if bucket.severity not in _VALID_SEVERITIES:
            raise ValueError(f"classify returned invalid severity {bucket.severity!r} for bucket {bucket_key!r}")
        if bucket.severity == "none":
            continue
        payload[bucket_key] = {
            "severity": bucket.severity,
            "title": bucket.title,
            "rootCause": bucket.rootCause,
            "recommendedAction": bucket.recommendedAction,
        }

    state["cqts_payload"] = payload

    if state.get("dry_run"):
        logger.info("[DRY RUN] checkin_id=%s classification=%s", state["checkin_id"], payload)
        state.setdefault("logs", []).append(f"validate_and_writeback: [DRY RUN] would write buckets={list(payload.keys())}")
        return state

    client = WootzCheckinClient(settings)
    client.post_classification(state["checkin_id"], payload)
    state.setdefault("logs", []).append(f"validate_and_writeback: wrote buckets={list(payload.keys())}")
    return state


def run_cqts_classification_for_checkin(
    settings: Settings, checkin_id: str, *, dry_run: bool = False
) -> Dict[str, Any]:
    state: State = {"checkin_id": checkin_id, "logs": [], "dry_run": dry_run}
    try:
        state = _timed("load_context", load_context, settings, state)
        state = _timed("retrieve_and_rerank", retrieve_and_rerank, settings, state)
        state = _timed("classify", classify, settings, state)
        state = _timed("validate_and_writeback", validate_and_writeback, settings, state)
        if not dry_run:
            # Vector upsert reflects this checkin's own PROBLEM/RESOLUTION
            # memory going forward — skipped in dry-run so nothing about
            # ZAI's own retrieval index changes during a trial run.
            state = _timed("upsert_vectors", upsert_vectors, settings, state)

        return {
            "ok": True,
            "checkin_id": checkin_id,
            "buckets": list((state.get("cqts_payload") or {}).keys()),
            "dry_run": dry_run,
            "logs": state.get("logs"),
        }
    except Exception as e:
        logger.exception("CQTS classification failed for checkin_id=%s", checkin_id)
        return {
            "ok": False,
            "checkin_id": checkin_id,
            "error": str(e),
            "error_type": type(e).__name__,
            "logs": state.get("logs"),
        }
