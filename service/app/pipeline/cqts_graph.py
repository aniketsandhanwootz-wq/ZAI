# service/app/pipeline/cqts_graph.py
"""
CQTS (Cost/Quality/Timeline/Scope) classification pipeline — daily batch,
separate from the real-time, webhook-driven graph.py. One checkin at a time:

    load_context           -> GET /internal/checkins/:id/cqts-context
    investigate             -> agent loop: given load_context's output, decide
                               what's still missing, call up to 4 tools
                               (search_problems/search_resolutions/
                               search_ccp_chunks/search_glide_kb) against
                               ZAI's own incident_vectors/ccp_vectors/
                               glide_kb_vectors, capped at 5 total tool calls,
                               then finish_investigation()
    classify                -> structured Gemini call via langchain-google-genai
    validate_and_writeback  -> POST /internal/checkins/:id/cqts-classification

`investigate` replaces the old `retrieve_and_rerank` (which unconditionally
ran every lookup via retrieve_context.py/rerank_context.py, no LLM-driven
decision about what's actually needed, and only fed problems/resolutions
into the final prompt — CCP guidance, Glide KB hits, and media captions were
fetched and then silently discarded before classification; see git history
for that version). retrieve_context.py/rerank_context.py are left in place,
unused by this pipeline now, in case their retrieval/reranking logic is
useful elsewhere later.

No vector-upsert step here, deliberately: graph.py's real-time, webhook-driven
ingestion already calls upsert_vectors on every CHECKIN_CREATED/UPDATED/
CONVERSATION_ADDED event — the exact same activity that makes a checkin show
up in needs-cqts-classification in the first place. By the time this batch
reaches a checkin, its PROBLEM/RESOLUTION vectors are already fresh, upserted
earlier and more reliably than a once-a-day batch could redo. Re-embedding
here would just be duplicate work against the same (tenant_id, checkin_id,
vector_type) row. The one-time backfill_cqts_vectors.py script is the only
place this pipeline touches incident_vectors, covering history real-time
ingestion never saw.

See the CQTS plan (Part 3) for the full design rationale. LangSmith tracing is
deliberately not wired into this graph — not needed yet, per the plan.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Optional

from .nodes.rerank_context import _cosine_sim_from_distance, _overlap_score

from pydantic import BaseModel, Field

from ..config import Settings
from ..tools.wootzcheckin_client import WootzCheckinClient

logger = logging.getLogger("zai.cqts_graph")

MAX_INVESTIGATION_TOOL_CALLS = 5

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
# Single-bucket classification: a check-in gets at most ONE primary CQTS
# category. Secondary effects on other categories are folded into
# recommendedAction's text rather than producing a second bucket entry — see
# the CQTS plan for why (avoids a checkin fanning out into 3-4 near-duplicate
# entries when it really has one dominant issue).

Severity = str  # "critical" | "moderate" | "watch" — validated below
Bucket = str  # "cost" | "quality" | "timeline" | "scope" — validated below

_VALID_BUCKETS = {"cost", "quality", "timeline", "scope"}
_VALID_SEVERITIES = {"critical", "moderate", "watch"}


class CqtsClassificationOut(BaseModel):
    bucket: Optional[Bucket] = Field(
        default=None, description="One of: cost, quality, timeline, scope. Unset if nothing applies."
    )
    severity: Optional[Severity] = Field(default=None, description="One of: critical, moderate, watch")
    title: Optional[str] = Field(default=None, description="Short label, <= 8 words")
    rootCause: Optional[str] = None
    recommendedAction: Optional[str] = Field(
        default=None, description="Concrete next step, 60 words maximum"
    )


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
    state["checkin_files"] = checkin.get("files") or []

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
    file_names = [
        (f.split("/")[-1].split("?")[0] if isinstance(f, str) else "")
        for f in (state.get("checkin_files") or [])
    ]
    file_names = [f for f in file_names if f]

    state["cqts_project_context_text"] = "\n".join(
        [
            f"Project: {project.get('projectName', '')} | Part: {project.get('partNumber', '')}",
            f"Assembly status: {project.get('statusAssembly', '')} | Dispatch date: {project.get('dispatchDate') or '(not set)'}",
            "CCPs:" if ccp_lines else "",
            *ccp_lines,
            "Recent dashboard updates:" if dashboard_lines else "",
            *dashboard_lines,
            # Lets the agent know upfront that attachments exist, before it
            # decides whether to search for them (see search_problems, which
            # now also covers ATTACHMENT-type vectors).
            f"Attached files on this check-in: {', '.join(file_names)}" if file_names else "",
        ]
    ).strip()

    state["cqts_conversation_thread_text"] = "\n".join(
        f"[{cv.get('timestamp', '')}] {cv.get('addedBy', '')} ({cv.get('status') or 'comment'}): {cv.get('message', '')}"
        for cv in conversations
    ).strip() or "(no conversation yet)"

    state.setdefault("logs", []).append(f"load_context: loaded checkin={checkin_id}")
    return state


def _fmt_incident_rows(rows: List[Dict[str, Any]]) -> str:
    # search_incidents() returns "summary" (summary_text), not "text" — the
    # old _format_vector_memory read the wrong key here and silently
    # rendered nothing for problems/resolutions; fixed in this rewrite.
    lines = []
    for r in rows[:8]:
        summary = (r.get("summary") or "").strip()
        if not summary:
            continue
        tag = f"[{r.get('status')}] " if r.get("status") else ""
        lines.append(f"- {tag}{summary[:400]}")
    return "\n".join(lines) if lines else "(no matches)"


def _fmt_ccp_rows(rows: List[Dict[str, Any]]) -> str:
    lines = []
    for r in rows[:8]:
        text = (r.get("text") or "").strip()
        if not text:
            continue
        name = r.get("ccp_name") or ""
        lines.append(f"- [{name}] {text[:400]}")
    return "\n".join(lines) if lines else "(no matches)"


def _fmt_glide_kb_rows(rows: List[Dict[str, Any]]) -> str:
    lines = []
    for r in rows[:8]:
        text = (r.get("text") or "").strip()
        if not text:
            continue
        title = r.get("title") or r.get("table_name") or ""
        lines.append(f"- [{title}] {text[:400]}")
    return "\n".join(lines) if lines else "(no matches)"


_CRITICAL_GLIDE_TABLES = ("raw_material", "processes", "boughtouts")


def _dedupe_key(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())[:120]


def _rerank_and_dedupe(
    query: str, rows: List[Dict[str, Any]], text_key: str, kind: str, *, keep: int = 8
) -> List[Dict[str, Any]]:
    """
    Same heuristic scoring rerank_context.py already used for the real-time
    reply flow — reused here rather than reimplemented — plus a light
    dedup pass: earlier live testing showed the plain top-K search
    surfacing several near-identical rows (same underlying issue logged as
    separate checkins), drowning out distinct evidence.
    """
    scored: List[tuple[float, Dict[str, Any]]] = []
    for i, r in enumerate(rows):
        doc = (r.get(text_key) or "").strip()
        sim = _cosine_sim_from_distance(r.get("distance", 1.0))
        overlap = _overlap_score(query, doc)
        base_rank = 1.0 / (1 + i)
        bonus = 0.05 if kind == "resolution" else 0.0
        if (r.get("table_name") or "").strip().lower() in _CRITICAL_GLIDE_TABLES:
            bonus += 0.10
        score = (0.55 * sim) + (0.25 * overlap) + (0.20 * base_rank) + bonus
        scored.append((score, r))
    scored.sort(key=lambda x: x[0], reverse=True)

    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for _score, r in scored:
        key = _dedupe_key(r.get(text_key) or "")
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        out.append(r)
        if len(out) >= keep:
            break
    return out


def _make_investigation_tools(settings: Settings, state: State, transcript: List[str]):
    """
    The flowchart's 4 search tools, each a thin wrapper around VectorTool —
    embeds the model's own query text and searches the corresponding table.
    Every call appends a rendered block to `transcript` (the "Observation:
    tool result appended to running context" step), which becomes the
    {investigation_findings} the classification prompt actually sees.
    """
    from langchain_core.tools import tool
    from ..tools.embed_tool import EmbedTool
    from ..tools.vector_tool import VectorTool

    embedder = EmbedTool(settings)
    vec = VectorTool(settings)
    tenant_id = (state.get("tenant_id") or "").strip()
    project_name = state.get("project_name") or None
    part_number = state.get("part_number") or None
    legacy_id = state.get("legacy_id") or None
    self_checkin_id = (state.get("checkin_id") or "").strip()

    @tool
    def search_problems(query: str) -> str:
        """Search past QUALITY/COST/TIMELINE/SCOPE PROBLEMS from other check-ins — including analyzed PDF/file attachments (inspection reports, test certs) and photo captions — by semantic similarity to `query`. Use this to find precedent for what's happening on this check-in."""
        q = embedder.embed_query(query)
        rows: List[Dict[str, Any]] = []
        for vt in ("PROBLEM", "ATTACHMENT", "MEDIA"):
            rows.extend(vec.search_incidents(tenant_id=tenant_id, query_embedding=q, top_k=20, project_name=project_name, part_number=part_number, vector_type=vt))
        rows = [r for r in rows if str(r.get("checkin_id") or "") != self_checkin_id]
        rows = _rerank_and_dedupe(query, rows, "summary", "problem")
        out = _fmt_incident_rows(rows)
        transcript.append(f"search_problems(\"{query}\") ->\n{out}")
        return out

    @tool
    def search_resolutions(query: str) -> str:
        """Search past RESOLUTIONS (what actually fixed similar problems before), by semantic similarity to `query`. Use this to ground a recommendedAction in real precedent."""
        q = embedder.embed_query(query)
        rows = vec.search_incidents(tenant_id=tenant_id, query_embedding=q, top_k=20, project_name=project_name, part_number=part_number, vector_type="RESOLUTION")
        rows = [r for r in rows if str(r.get("checkin_id") or "") != self_checkin_id]
        rows = _rerank_and_dedupe(query, rows, "summary", "resolution")
        out = _fmt_incident_rows(rows)
        transcript.append(f"search_resolutions(\"{query}\") ->\n{out}")
        return out

    @tool
    def search_ccp_chunks(query: str) -> str:
        """Search this assembly's CCP (Critical Control Point) documentation — specs, tolerances, inspection guidance — by semantic similarity to `query`. Use this when the check-in references a spec/tolerance/inspection point you need grounding for."""
        q = embedder.embed_query(query)
        rows = vec.search_ccp_chunks(tenant_id=tenant_id, query_embedding=q, top_k=20, project_name=project_name, part_number=part_number, legacy_id=legacy_id)
        rows = _rerank_and_dedupe(query, rows, "text", "ccp")
        out = _fmt_ccp_rows(rows)
        transcript.append(f"search_ccp_chunks(\"{query}\") ->\n{out}")
        return out

    @tool
    def search_glide_kb(query: str) -> str:
        """Search shopfloor knowledge base (raw material specs, processes, bought-out parts, supplier info) by semantic similarity to `query`. Use this for material/process/supplier facts not present in the check-in itself."""
        q = embedder.embed_query(query)
        rows = vec.search_glide_kb_chunks(tenant_id=tenant_id, query_embedding=q, top_k=20, project_name=project_name, part_number=part_number)
        rows = _rerank_and_dedupe(query, rows, "text", "glide")
        out = _fmt_glide_kb_rows(rows)
        transcript.append(f"search_glide_kb(\"{query}\") ->\n{out}")
        return out

    @tool
    def finish_investigation() -> str:
        """Call this once you've gathered enough evidence to classify the check-in confidently. Stops the investigation and proceeds to classification."""
        return "Investigation complete."

    return [search_problems, search_resolutions, search_ccp_chunks, search_glide_kb, finish_investigation]


def _load_prompt(filename: str) -> str:
    from pathlib import Path

    # cqts_graph.py lives at service/app/pipeline/ (one level shallower than
    # nodes/*.py), so this is parents[3] to reach the ZAI repo root, not [4].
    prompt_path = Path(__file__).resolve().parents[3] / "packages" / "prompts" / filename
    return prompt_path.read_text(encoding="utf-8")


def investigate(settings: Settings, state: State) -> State:
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage

    transcript: List[str] = []
    tools = _make_investigation_tools(settings, state, transcript)
    tool_by_name = {t.name: t for t in tools}

    llm = ChatGoogleGenerativeAI(
        model=settings.llm_model or "gemini-2.5-flash",
        google_api_key=settings.llm_api_key,
        temperature=0.1,
    )
    llm_with_tools = llm.bind_tools(tools)

    context_block = (
        f"CHECK-IN:\n{state.get('checkin_description', '')}\n\n"
        f"PROJECT / ASSEMBLY CONTEXT:\n{state.get('cqts_project_context_text', '')}\n\n"
        f"CONVERSATION THREAD:\n{state.get('cqts_conversation_thread_text', '')}"
    )
    messages: List[Any] = [
        SystemMessage(content=_load_prompt("cqts_investigate.md")),
        HumanMessage(content=context_block),
    ]

    calls_made = 0
    tool_names_used: List[str] = []
    while calls_made < MAX_INVESTIGATION_TOOL_CALLS:
        response = llm_with_tools.invoke(messages)
        messages.append(response)

        tool_calls = getattr(response, "tool_calls", None) or []
        if not tool_calls:
            # Model responded with no tool call at all — treats as done.
            break

        stop = False
        for tc in tool_calls:
            if calls_made >= MAX_INVESTIGATION_TOOL_CALLS:
                break
            name = tc.get("name")
            calls_made += 1

            if name == "finish_investigation":
                messages.append(ToolMessage(content="Investigation complete.", tool_call_id=tc.get("id", "")))
                stop = True
                break

            fn = tool_by_name.get(name)
            if not fn:
                messages.append(ToolMessage(content=f"Unknown tool: {name}", tool_call_id=tc.get("id", "")))
                continue

            try:
                result = fn.invoke(tc.get("args") or {})
            except Exception as e:
                result = f"(tool error: {e})"
            tool_names_used.append(name)
            messages.append(ToolMessage(content=str(result), tool_call_id=tc.get("id", "")))

        if stop:
            break

    state["investigation_findings"] = "\n\n".join(transcript).strip() or "(no additional evidence gathered)"
    state["investigation_tool_calls"] = calls_made
    state.setdefault("logs", []).append(
        f"investigate: {calls_made} tool call(s) ({', '.join(tool_names_used) or 'none'}), "
        f"{'cap hit — forced finish' if calls_made >= MAX_INVESTIGATION_TOOL_CALLS else 'model finished voluntarily'}"
    )
    return state


def _render_prompt(state: State) -> str:
    template = _load_prompt("cqts_classification.md")

    return (
        template.replace("{checkin_context}", state.get("checkin_description", ""))
        .replace("{project_context}", state.get("cqts_project_context_text", ""))
        .replace("{conversation_thread}", state.get("cqts_conversation_thread_text", ""))
        .replace("{investigation_findings}", state.get("investigation_findings", "(no additional evidence gathered)"))
    )


# checkin.image[] is a mixed media array (can hold video URLs too — see
# CheckinMediaContent.tsx's isVideoUrl/isImageUrl split on the wootzcheckin
# side) — filter before handing URLs to Gemini as image_url content, same
# extension check analyze_media.py's _IMG_EXT_RX already uses for the
# real-time ingestion path.
_IMAGE_EXT_RX = re.compile(r"\.(png|jpe?g|webp|bmp|tiff?)(\?.*)?$", re.IGNORECASE)


def _looks_like_image_url(url: str) -> bool:
    return bool(_IMAGE_EXT_RX.search((url or "").strip()))


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
    image_urls = [u for u in (state.get("checkin_images") or []) if _looks_like_image_url(u)]
    for url in image_urls[:4]:
        content.append({"type": "image_url", "image_url": {"url": url}})

    result: CqtsClassificationOut = structured_llm.invoke([HumanMessage(content=content)])

    state["cqts_result"] = result
    state.setdefault("logs", []).append("classify: structured Gemini call completed")
    return state


_RECOMMENDED_ACTION_MAX_WORDS = 60


def _cap_words(text: str, max_words: int) -> Optional[str]:
    """None in, None out — a recommendedAction is only sent when the model
    actually gave one (see cqts_classification.md: 'leave unset unless
    extremely necessary'). wootzcheckin's schema treats null as "no action
    needed," not a missing field."""
    text = (text or "").strip()
    if not text:
        return None
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]) + "…"


def validate_and_writeback(settings: Settings, state: State) -> State:
    result: CqtsClassificationOut = state["cqts_result"]

    payload: Dict[str, Any] = {}
    if result.bucket:
        if result.bucket not in _VALID_BUCKETS:
            raise ValueError(f"classify returned invalid bucket {result.bucket!r}")
        if result.severity not in _VALID_SEVERITIES:
            raise ValueError(f"classify returned invalid severity {result.severity!r}")
        payload[result.bucket] = {
            "severity": result.severity,
            "title": result.title or "",
            "rootCause": result.rootCause or "",
            "recommendedAction": _cap_words(result.recommendedAction or "", _RECOMMENDED_ACTION_MAX_WORDS),
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
        state = _timed("investigate", investigate, settings, state)
        state = _timed("classify", classify, settings, state)
        state = _timed("validate_and_writeback", validate_and_writeback, settings, state)

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
