from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import re
from datetime import datetime, date
import json
import hashlib
import secrets
import string
from ...tools.zai_cues_log_tool import ZaiCuesLogTool, ZaiCuesLogRow
from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.llm_tool import LLMTool
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool
from .rerank_context import rerank_context
from ...integrations.appsheet_client import AppSheetClient

ZAI_CUES_TEMPERATURE = 0.4


# -------------------------
# Prompt loader (ONLY zai_cues_10.md)
# -------------------------

def _find_repo_root(start: Path) -> Path:
    p = start
    for _ in range(10):
        if (p / "packages" / "prompts" / "zai_cues_10.md").exists():
            return p
        if p.parent == p:
            break
        p = p.parent
    return start


def _load_prompt_file(name: str) -> str:
    here = Path(__file__).resolve()
    root = _find_repo_root(here.parent.parent.parent.parent)  # nodes -> pipeline -> app -> service -> repo
    path = root / "packages" / "prompts" / name
    return path.read_text(encoding="utf-8")


def _load_zai_cues_prompt() -> str:
    return _load_prompt_file("zai_cues_10.md")

def _load_zai_context_prompt() -> str:
    return _load_prompt_file("zai_context.md")
# -------------------------
# Helpers
# -------------------------

_ALPHANUM = string.ascii_letters + string.digits


def _now_timestamp_str() -> str:
    # Match sheet style like: 01/07/26 12:49 PM
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.now(ZoneInfo("Asia/Kolkata"))
    except Exception:
        dt = datetime.now()
    return dt.strftime("%m/%d/%y %I:%M %p")


def _slot_cue_id(*, tenant_id: str, legacy_id: str, slot: int) -> str:
    """
    Stable slot IDs: always 10 rows per legacy_id.
    Every trigger updates same 10 rows (no accumulation).
    """
    base = f"{tenant_id}|{legacy_id}|ZAI_CUE_SLOT_V1|{int(slot)}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]


def _clamp_10_words(line: str) -> str:
    w = [x for x in re.split(r"\s+", (line or "").strip()) if x]
    w = w[:10]
    return " ".join(w).strip()


def _parse_date_loose(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None

    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%m-%d-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
    ):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass

    try:
        if "T" in s:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        pass

    return None


_STAGE_KEYWORDS_EARLY = re.compile(r"\b(raw|rm|sheet|plate|laser|cut|cutting|bend|bending|burr|scratch)\b", re.I)
_STAGE_KEYWORDS_MID = re.compile(r"\b(fabric|weld|welding|fitup|fixture|jig|grind|grinding|distort|spatter|undercut)\b", re.I)
_STAGE_KEYWORDS_LATE = re.compile(r"\b(paint|powder|coat|coating|finish|assembly|dispatch|packing|mask|thread|torque)\b", re.I)


def _infer_stage(*, dispatch_date_str: str, recent_text_blob: str) -> str:
    dd = _parse_date_loose(dispatch_date_str)
    if dd:
        today = datetime.now().date()
        days_left = (dd - today).days
        if days_left >= 14:
            return f"Early Stage (time remaining ~{days_left}d)"
        if 4 <= days_left < 14:
            return f"Mid Stage (time remaining ~{days_left}d)"
        return f"Late Stage (time remaining ~{days_left}d)"

    blob = (recent_text_blob or "").strip()
    if _STAGE_KEYWORDS_LATE.search(blob):
        return "Late Stage (from recent activity)"
    if _STAGE_KEYWORDS_MID.search(blob):
        return "Mid Stage (from recent activity)"
    if _STAGE_KEYWORDS_EARLY.search(blob):
        return "Early Stage (from recent activity)"

    return "Stage unknown (insufficient signals)"


def _compact_lines(lines: List[str], max_lines: int) -> str:
    out: List[str] = []
    for ln in lines:
        ln = (ln or "").strip()
        if not ln:
            continue
        out.append(ln)
        if len(out) >= max_lines:
            break
    return "\n".join(out).strip()


def _fmt_recent_activity(*, related_checkins: List[Dict[str, Any]], sheets: SheetsTool) -> str:
    if not related_checkins:
        return "(no recent checkins found)"

    k_ci_status = _key(sheets.map.col("checkin", "status"))
    k_ci_desc = _key(sheets.map.col("checkin", "description"))
    k_ci_id = _key(sheets.map.col("checkin", "checkin_id"))

    lines: List[str] = []
    for c in related_checkins[-6:]:
        cid = _norm_value((c or {}).get(k_ci_id, ""))
        st = _norm_value((c or {}).get(k_ci_status, ""))
        desc = _norm_value((c or {}).get(k_ci_desc, ""))
        s = f"{st}: {desc}".strip(": ").strip()
        if cid:
            s = f"{s} (checkin_id={cid})".strip()
        if s:
            lines.append("- " + s)

    return _compact_lines(lines, 12) or "(no usable recent activity)"


def _fmt_process_material(*, project_name: str, part_number: str, company_profile_text: str) -> str:
    lines: List[str] = []
    if part_number:
        lines.append(f"Part: {part_number}")
    if project_name:
        lines.append(f"Project: {project_name}")
    if company_profile_text:
        cp = company_profile_text.replace("\n", " ").strip()
        if len(cp) > 180:
            cp = cp[:177] + "..."
        lines.append(f"Client context: {cp}")
    return " | ".join(lines).strip() if lines else "(unknown)"


def _parse_json_loose(s: str) -> dict:
    s = (s or "").strip()
    if not s:
        return {}
    if s.startswith("```"):
        s = s.strip().strip("`").strip()
        if s.lower().startswith("json"):
            s = s[4:].strip()
    try:
        return json.loads(s)
    except Exception:
        return {}

def _generate_context_notes_for_cues(
    *,
    llm: LLMTool,
    cues10: List[str],
    stage: str,
    packed_context: str,
    process_material: str,
    recent_activity: str,
    previous_chips: str,
) -> Dict[int, str]:
    """
    Uses packages/prompts/zai_context.md
    Returns {index: "TYPE: header\\nexplanation"} for only the ~30% selected.
    """
    if not cues10:
        return {}

    tmpl = _load_zai_context_prompt()

    cues_list = "\n".join([f"{i}|{(cues10[i] or '').strip()}" for i in range(len(cues10))]).strip()

    # Map prompt placeholders to what we actually have today.
    prompt = (
        tmpl.replace("{stage}", stage or "N/A")
        .replace("{vector_risks}", packed_context or "N/A")           # best available proxy
        .replace("{process_material}", process_material or "N/A")
        .replace("{recent_activity}", recent_activity or "N/A")
        .replace("{previous_chips}", previous_chips or "N/A")
    )

    # Inject cues list (zai_context.md expects CUES_LIST conceptually; we hard-add it)
    prompt = (
        prompt
        + "\n\nCUES_LIST:\n"
        + cues_list
        + "\n\n(Important: Output only selected blocks in the specified format.)\n"
    )

    raw = llm.generate_text(prompt, temperature=ZAI_CUES_TEMPERATURE)
    text = str(raw or "").strip()
    if not text:
        return {}

    out: Dict[int, str] = {}

    # Parse blocks like: `index|TYPE: Header\nExplanation`
    # We accept multiple blocks separated by blank lines.
    blocks = re.split(r"\n\s*\n+", text)
    for b in blocks:
        b = (b or "").strip()
        if not b:
            continue
        lines = [ln.strip() for ln in b.splitlines() if ln.strip()]
        if not lines:
            continue

        first = lines[0]
        # Expect: "<idx>|<TYPE>: <Header>"
        m = re.match(r"^\s*(\d+)\s*\|\s*([a-zA-Z_]+)\s*:\s*(.+)\s*$", first)
        if not m:
            continue

        idx = int(m.group(1))
        typ = m.group(2).strip()
        header = m.group(3).strip()

        if idx < 0 or idx >= len(cues10):
            continue

        # Remaining lines are explanation (max ~90 chars per your prompt; we keep as-is but clamp total)
        expl = " ".join(lines[1:]).strip()
        note_lines: List[str] = []
        note_lines.append(f"{typ}: {header}".strip())
        if expl:
            note_lines.append(expl)

        note = "\n".join(note_lines).strip()
        if len(note) > 240:
            note = note[:237] + "..."

        out[idx] = note

    return out

def _split_lines_fallback(text: str, max_items: int = 10) -> List[str]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    out: List[str] = []
    for ln in lines:
        ln = re.sub(r"^\-\s*\[\s*\]\s*", "", ln).strip()
        ln = re.sub(r"^\-\s*", "", ln).strip()
        ln = re.sub(r"^\•\s*", "", ln).strip()
        ln = re.sub(r"^\d+[\).]\s*", "", ln).strip()
        if ln:
            out.append(ln)
        if len(out) >= max_items:
            break
    return out[:max_items]


def _generate_10_cues_from_context(
    *,
    llm: LLMTool,
    stage: str,
    packed_context: str,
    process_material: str,
    recent_activity: str,
    previous_chips: str,
    company_context: str = "",
    snapshot: str = "",
    closure_notes: str = "",
    attachment_context: str = "",
) -> List[str]:
    """
    Uses packages/prompts/zai_cues_10.md
    HARD REQ: returns exactly 10 strings (<=10 words each), deduped.
    """
    tmpl = _load_zai_cues_prompt()

    prompt = (
        tmpl.replace("{stage}", stage or "N/A")
        .replace("{packed_context}", packed_context or "N/A")
        .replace("{process_material}", process_material or "N/A")
        .replace("{recent_activity}", recent_activity or "N/A")
        .replace("{previous_chips}", previous_chips or "N/A")
        # extra placeholders (safe even if not present in file)
        .replace("{company_context}", company_context or "N/A")
        .replace("{snapshot}", snapshot or "N/A")
        .replace("{closure_notes}", closure_notes or "N/A")
        .replace("{attachment_context}", attachment_context or "N/A")
    )

    raw = llm.generate_text(prompt, temperature=ZAI_CUES_TEMPERATURE)

    cues: List[str] = []
    obj = _parse_json_loose(raw)
    if isinstance(obj, dict) and isinstance(obj.get("cues"), list):
        cues = [str(x) for x in obj.get("cues") or []]
    else:
        cues = _split_lines_fallback(raw, max_items=10)

    out: List[str] = []
    seen = set()
    for c in cues:
        line = _clamp_10_words(str(c))
        k = re.sub(r"\s+", " ", line).strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(line)
        if len(out) >= 10:
            break

    fallback = [
        "Surface pe scratch? glove se wipe karke dekho",
        "Edges pe burr? finger run once quickly",
        "Hole alignment pin se check kar lo",
        "Weld spatter remove; paint me issue hota",
        "Critical dim vernier se confirm kar lo",
        "Threads clean? bolt run once smooth jaa raha",
        "Backside dents? flip karke ek baar dekh lo",
        "Mating fit trial once; tight toh nahi",
        "Packing se pehle final visual scan kar lo",
        "Label/marking correct? dispatch pe confusion hota",
    ]
    i = 0
    while len(out) < 10 and i < len(fallback):
        cand = _clamp_10_words(fallback[i])
        i += 1
        k = cand.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(cand)

    return out[:10]


def _project_chips_from_10(cues10: List[str]) -> str:
    cues10 = [c for c in (cues10 or []) if (c or "").strip()]
    if not cues10:
        return ""
    picked = cues10[:3]
    if len(cues10) >= 4:
        picked = cues10[:4]
    return "\n".join([_clamp_10_words(x) for x in picked]).strip()


def _is_mfg(status: str) -> bool:
    return (status or "").strip().lower() == "mfg"

def _log_cues_event(
    *,
    settings: Settings,
    state: Dict[str, Any],
    legacy_id: str,
    tenant_id: str,
    status_assembly: str,
    skipped: bool,
    skip_reason: str,
    cues10: Optional[List[str]] = None,
    chips: str = "",
    rerank_used: bool = False,
) -> None:
    """
    Best-effort logging to separate ZAI_CUES_LOG spreadsheet.
    Never raises.
    """
    try:
        tool = ZaiCuesLogTool(settings)
        if not tool.enabled():
            return

        event_type = str(state.get("event_type") or (state.get("payload") or {}).get("event_type") or "").strip()
        run_id = str(state.get("run_id") or "").strip()
        primary_id = str(state.get("primary_id") or "").strip()
        idem = str(state.get("idempotency_primary_id") or "").strip()

        cues10_json = ""
        if cues10 is not None:
            # keep it simple + stable
            cues10_json = json.dumps([str(x) for x in (cues10 or [])], ensure_ascii=False)

        row = ZaiCuesLogRow(
            timestamp_ist=_now_timestamp_str(),
            event_type=event_type,
            run_id=run_id,
            primary_id=primary_id,
            idempotency_primary_id=idem,
            tenant_id=str(tenant_id or "").strip(),
            legacy_id=str(legacy_id or "").strip(),
            status_assembly=str(status_assembly or "").strip(),
            skipped=bool(skipped),
            skip_reason=str(skip_reason or "").strip(),
            rerank_used=bool(rerank_used),
            cues10_json=cues10_json,
            chips=str(chips or "").strip(),
        )
        tool.append_row(row)
    except Exception as e:
        (state.get("logs") or []).append(f"zai_cues_log: non-fatal failure: {e}")
# -------------------------
# Main node
# -------------------------

def generate_assembly_todo(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    payload = state.get("payload") or {}
    legacy_id = _norm_value(payload.get("legacy_id") or state.get("legacy_id") or "")
    if not legacy_id:
        msg = "assembly_todo: missing legacy_id; skipped"
        (state.get("logs") or []).append(msg)
        state["assembly_todo_skipped"] = True
        _log_cues_event(
            settings=settings,
            state=state,
            legacy_id="",
            tenant_id="",
            status_assembly="",
            skipped=True,
            skip_reason=msg,
            rerank_used=False,
        )
        return state

    sheets = SheetsTool(settings)

    pr = sheets.get_project_by_legacy_id(legacy_id)
    if not pr:
        msg = f"assembly_todo: project row not found for legacy_id={legacy_id}; skipped"
        (state.get("logs") or []).append(msg)
        state["assembly_todo_skipped"] = True
        _log_cues_event(
            settings=settings,
            state=state,
            legacy_id=legacy_id,
            tenant_id="",
            status_assembly="",
            skipped=True,
            skip_reason=msg,
            rerank_used=False,
        )
        return state

    k_status = _key(sheets.map.col("project", "status_assembly"))
    status_val = _norm_value(pr.get(k_status, ""))
    if not _is_mfg(status_val):
        msg = f"assembly_todo: status_assembly='{status_val}' != 'mfg'; skipped"
        (state.get("logs") or []).append(msg)
        state["assembly_todo_skipped"] = True
        _log_cues_event(
            settings=settings,
            state=state,
            legacy_id=legacy_id,
            tenant_id="",
            status_assembly=status_val,
            skipped=True,
            skip_reason=msg,
            rerank_used=False,
        )
        return state

    k_pname = _key(sheets.map.col("project", "project_name"))
    k_part = _key(sheets.map.col("project", "part_number"))
    k_tenant = _key(sheets.map.col("project", "company_row_id"))
    k_prev = _key(sheets.map.col("project", "ai_critcal_point"))

    try:
        k_dispatch = _key(sheets.map.col("project", "dispatch_date"))
    except Exception:
        k_dispatch = ""

    project_name = _norm_value(pr.get(k_pname, ""))
    part_number = _norm_value(pr.get(k_part, ""))
    tenant_id = _norm_value(pr.get(k_tenant, ""))
    previous_chips = _norm_value(pr.get(k_prev, ""))
    dispatch_date_str = _norm_value(pr.get(k_dispatch, "")) if k_dispatch else ""

    if not tenant_id:
        msg = f"assembly_todo: missing tenant_id(company_row_id) for legacy_id={legacy_id}; skipped"
        (state.get("logs") or []).append(msg)
        state["assembly_todo_skipped"] = True
        _log_cues_event(
            settings=settings,
            state=state,
            legacy_id=legacy_id,
            tenant_id="",
            status_assembly=status_val,
            skipped=True,
            skip_reason=msg,
            rerank_used=False,
        )
        return state

    query_text = (
        f"Micro inspection cues to avoid blind spots during manufacturing.\n"
        f"PROJECT_NAME: {project_name or '(unknown)'}\n"
        f"PART_NUMBER: {part_number or '(unknown)'}\n"
        f"LEGACY_ID: {legacy_id}\n"
        f"Use past failures, CCP requirements, resolutions that worked, and recent updates.\n"
        f"Output should be 10 cues, shopfloor style."
    ).strip()

    embedder = EmbedTool(settings)
    vector_db = VectorTool(settings)

    try:
        q = embedder.embed_query(query_text)
    except Exception as e:
        msg = f"assembly_todo: embed_query failed: {e}"
        (state.get("logs") or []).append(msg)
        state["assembly_todo_skipped"] = True
        _log_cues_event(
            settings=settings,
            state=state,
            legacy_id=legacy_id,
            tenant_id=tenant_id,
            status_assembly=status_val,
            skipped=True,
            skip_reason=msg,
            rerank_used=False,
        )
        return state

    problems = vector_db.search_incidents(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=80,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=legacy_id,
        vector_type="PROBLEM",
    )
    resolutions = vector_db.search_incidents(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=80,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=legacy_id,
        vector_type="RESOLUTION",
    )
    media = vector_db.search_incidents(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=60,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=legacy_id,
        vector_type="MEDIA",
    )
    ccp = vector_db.search_ccp_chunks(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=60,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=legacy_id,
    )
    dash = vector_db.search_dashboard_updates(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=30,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=legacy_id,
    )

    company_profile_text = ""
    try:
        row = vector_db.get_company_profile_by_tenant_row_id(tenant_row_id=tenant_id)
        if row:
            company_profile_text = (
                f"Company: {row.get('company_name','')}\n"
                f"Client description: {row.get('company_description','')}"
            ).strip()
    except Exception as e:
        (state.get("logs") or []).append(f"assembly_todo: company profile retrieval failed (non-fatal): {e}")

    tmp_state = {
        "thread_snapshot_text": query_text,
        "similar_problems": problems,
        "similar_resolutions": resolutions,
        "similar_media": media,
        "relevant_ccp_chunks": ccp,
        "relevant_dashboard_updates": dash,
        "relevant_glide_kb_chunks": [],
        "logs": [],
    }
    tmp_state = rerank_context(settings, tmp_state)
    packed_context = _norm_value(tmp_state.get("packed_context", ""))

    k_ci_legacy = _key(sheets.map.col("checkin", "legacy_id"))
    all_checkins = sheets.list_checkins()
    related_checkins = [
        c for c in (all_checkins or [])
        if _key(_norm_value((c or {}).get(k_ci_legacy, ""))) == _key(legacy_id)
    ]
    related_checkins = related_checkins[-10:]
    recent_activity = _fmt_recent_activity(related_checkins=related_checkins, sheets=sheets)

    recent_blob = f"{dispatch_date_str}\n{recent_activity}\n{packed_context}"
    stage = _infer_stage(dispatch_date_str=dispatch_date_str, recent_text_blob=recent_blob)

    process_material = _fmt_process_material(
        project_name=project_name,
        part_number=part_number,
        company_profile_text=company_profile_text,
    )

    llm = LLMTool(settings)

    cues10 = _generate_10_cues_from_context(
        llm=llm,
        stage=stage,
        packed_context=packed_context,
        process_material=process_material,
        recent_activity=recent_activity,
        previous_chips=previous_chips,
        company_context=company_profile_text,
        snapshot=query_text,
        closure_notes="",
        attachment_context="",
    )
    # Context notes (only for ~30% cues that are non-obvious / risky)
    context_map = _generate_context_notes_for_cues(
        llm=llm,
        cues10=cues10,
        stage=stage,
        packed_context=packed_context,
        process_material=process_material,
        recent_activity=recent_activity,
        previous_chips=previous_chips,
    )
    state["assembly_todo_context_map"] = context_map
    chips = _project_chips_from_10(cues10)

    col_out = sheets.map.col("project", "ai_critcal_point")
    ok = sheets.update_project_cell_by_legacy_id(legacy_id, column_name=col_out, value=chips)

    state["assembly_todo_written"] = bool(ok)
    state["assembly_todo_skipped"] = False
    state["assembly_todo_legacy_id"] = legacy_id
    state["assembly_todo_chips"] = chips
    state["assembly_todo_cues10"] = cues10

    # --- ZAI Cues Log (best-effort) ---
    _log_cues_event(
        settings=settings,
        state=state,
        legacy_id=legacy_id,
        tenant_id=tenant_id,
        status_assembly=status_val,
        skipped=False,
        skip_reason="",
        cues10=cues10,
        chips=chips,
        rerank_used=True,
    )
    if ok:
        (state.get("logs") or []).append(f"assembly_todo: wrote chips to Project.ai_critcal_point legacy_id={legacy_id}")
    else:
        (state.get("logs") or []).append(f"assembly_todo: FAILED writeback to Project.ai_critcal_point legacy_id={legacy_id}")

    # Sync same 10 cues into AppSheet (replace/update same 10 slots)
    try:
        client = AppSheetClient(settings)
        if client.enabled() and cues10:
            generated_at = _now_timestamp_str()
            cue_items = []
            for i in range(1, 11):
                idx = i - 1
                cue_items.append(
                    {
                        "cue_id": _slot_cue_id(tenant_id=tenant_id, legacy_id=legacy_id, slot=i),
                        "cue": cues10[idx],
                        "context": (context_map.get(idx) or "").strip(),
                    }
                )
            client.upsert_cues_rows(
                legacy_id=legacy_id,
                cue_items=cue_items,
                generated_at=generated_at,
            )
            (state.get("logs") or []).append(f"appsheet_cues: upserted=10 legacy_id={legacy_id}")
    except Exception as e:
        (state.get("logs") or []).append(f"appsheet_cues: non-fatal failure: {e}")

    return state
