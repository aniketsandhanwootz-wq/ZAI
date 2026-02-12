# service/app/pipeline/nodes/generate_assembly_todo.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import re
from datetime import datetime, date
import json
import hashlib
import string

from ...config import Settings
from ...tools.sheets_tool import _key, _norm_value
from .rerank_context import rerank_context

from ..lc_runtime import lc_registry, lc_invoke


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
    root = _find_repo_root(here.parent.parent.parent.parent)
    path = root / "packages" / "prompts" / name
    return path.read_text(encoding="utf-8")

def _load_zai_cues_prompt() -> str:
    return _load_prompt_file("zai_cues_10.md")


# -------------------------
# Helpers
# -------------------------

_ALPHANUM = string.ascii_letters + string.digits

def _now_timestamp_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.now(ZoneInfo("Asia/Kolkata"))
    except Exception:
        dt = datetime.now()
    return dt.strftime("%m/%d/%y %I:%M %p")

def _slot_cue_id(*, tenant_id: str, legacy_id: str, slot: int) -> str:
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
        "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m-%d-%Y",
        "%d/%m/%Y", "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S", "%m-%d-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S",
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

def _fmt_recent_activity(*, related_checkins: List[Dict[str, Any]], k_ci_status: str, k_ci_desc: str, k_ci_id: str) -> str:
    if not related_checkins:
        return "(no recent checkins found)"
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
    llm_text_fn,
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
    tmpl = _load_zai_cues_prompt()
    prompt = (
        tmpl.replace("{stage}", stage or "N/A")
        .replace("{packed_context}", packed_context or "N/A")
        .replace("{process_material}", process_material or "N/A")
        .replace("{recent_activity}", recent_activity or "N/A")
        .replace("{previous_chips}", previous_chips or "N/A")
        .replace("{company_context}", company_context or "N/A")
        .replace("{snapshot}", snapshot or "N/A")
        .replace("{closure_notes}", closure_notes or "N/A")
        .replace("{attachment_context}", attachment_context or "N/A")
    )

    raw = str(llm_text_fn(prompt) or "").strip()

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
    picked = cues10[:4] if len(cues10) >= 4 else cues10[:3]
    return "\n".join([_clamp_10_words(x) for x in picked]).strip()

def _is_mfg(status: str) -> bool:
    return (status or "").strip().lower() == "mfg"


def generate_assembly_todo(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    payload = state.get("payload") or {}
    legacy_id = _norm_value(payload.get("legacy_id") or state.get("legacy_id") or "")
    if not legacy_id:
        state.setdefault("logs", []).append("assembly_todo: missing legacy_id; skipped")
        state["assembly_todo_skipped"] = True
        return state

    reg = lc_registry(settings, state)

    pr = lc_invoke(reg, "sheets_get_project_by_legacy_id", {"legacy_id": legacy_id}, state, default=None)
    if not isinstance(pr, dict) or not pr:
        state.setdefault("logs", []).append(f"assembly_todo: project row not found for legacy_id={legacy_id}; skipped")
        state["assembly_todo_skipped"] = True
        return state

    col_status = lc_invoke(reg, "sheets_map_col", {"table": "project", "field": "status_assembly"}, state, default="")
    status_val = _norm_value(pr.get(_key(col_status), "")) if col_status else ""
    if not _is_mfg(status_val):
        state.setdefault("logs", []).append(f"assembly_todo: status_assembly='{status_val}' != 'mfg'; skipped")
        state["assembly_todo_skipped"] = True
        return state

    col_pname = lc_invoke(reg, "sheets_map_col", {"table": "project", "field": "project_name"}, state, default="")
    col_part = lc_invoke(reg, "sheets_map_col", {"table": "project", "field": "part_number"}, state, default="")
    col_tenant = lc_invoke(reg, "sheets_map_col", {"table": "project", "field": "company_row_id"}, state, default="")
    col_prev = lc_invoke(reg, "sheets_map_col", {"table": "project", "field": "ai_critcal_point"}, state, default="")
    col_dispatch = lc_invoke(reg, "sheets_map_col", {"table": "project", "field": "dispatch_date"}, state, default="")

    project_name = _norm_value(pr.get(_key(col_pname), "")) if col_pname else ""
    part_number = _norm_value(pr.get(_key(col_part), "")) if col_part else ""
    tenant_id = _norm_value(pr.get(_key(col_tenant), "")) if col_tenant else ""
    previous_chips = _norm_value(pr.get(_key(col_prev), "")) if col_prev else ""
    dispatch_date_str = _norm_value(pr.get(_key(col_dispatch), "")) if col_dispatch else ""

    if not tenant_id:
        state.setdefault("logs", []).append(f"assembly_todo: missing tenant_id(company_row_id) for legacy_id={legacy_id}; skipped")
        state["assembly_todo_skipped"] = True
        return state

    query_text = (
        f"Micro inspection cues to avoid blind spots during manufacturing.\n"
        f"PROJECT_NAME: {project_name or '(unknown)'}\n"
        f"PART_NUMBER: {part_number or '(unknown)'}\n"
        f"LEGACY_ID: {legacy_id}\n"
        f"Use past failures, CCP requirements, resolutions that worked, and recent updates.\n"
        f"Output should be 10 cues, shopfloor style."
    ).strip()

    q = lc_invoke(reg, "embed_query", {"text": query_text}, state, fatal=True)

    problems = lc_invoke(
        reg, "vector_search_incidents",
        {"tenant_id": tenant_id, "query_embedding": q, "top_k": 80, "project_name": project_name or None, "part_number": part_number or None, "legacy_id": legacy_id, "vector_type": "PROBLEM"},
        state, default=[]
    ) or []
    resolutions = lc_invoke(
        reg, "vector_search_incidents",
        {"tenant_id": tenant_id, "query_embedding": q, "top_k": 80, "project_name": project_name or None, "part_number": part_number or None, "legacy_id": legacy_id, "vector_type": "RESOLUTION"},
        state, default=[]
    ) or []
    media = lc_invoke(
        reg, "vector_search_incidents",
        {"tenant_id": tenant_id, "query_embedding": q, "top_k": 60, "project_name": project_name or None, "part_number": part_number or None, "legacy_id": legacy_id, "vector_type": "MEDIA"},
        state, default=[]
    ) or []
    ccp = lc_invoke(
        reg, "vector_search_ccp_chunks",
        {"tenant_id": tenant_id, "query_embedding": q, "top_k": 60, "project_name": project_name or None, "part_number": part_number or None, "legacy_id": legacy_id},
        state, default=[]
    ) or []
    dash = lc_invoke(
        reg, "vector_search_dashboard_updates",
        {"tenant_id": tenant_id, "query_embedding": q, "top_k": 30, "project_name": project_name or None, "part_number": part_number or None, "legacy_id": legacy_id},
        state, default=[]
    ) or []

    company_profile_text = ""
    row = lc_invoke(
        reg,
        "vector_get_company_profile_by_tenant_row_id",
        {"tenant_row_id": tenant_id},
        state,
        default=None,
    )
    if isinstance(row, dict) and row:
        company_profile_text = (
            f"Company: {row.get('company_name','')}\n"
            f"Client description: {row.get('company_description','')}"
        ).strip()

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

    all_checkins = lc_invoke(reg, "sheets_list_checkins", {}, state, default=[]) or []
    col_ci_legacy = lc_invoke(reg, "sheets_map_col", {"table": "checkin", "field": "legacy_id"}, state, default="")
    col_ci_status = lc_invoke(reg, "sheets_map_col", {"table": "checkin", "field": "status"}, state, default="")
    col_ci_desc = lc_invoke(reg, "sheets_map_col", {"table": "checkin", "field": "description"}, state, default="")
    col_ci_id = lc_invoke(reg, "sheets_map_col", {"table": "checkin", "field": "checkin_id"}, state, default="")

    k_ci_legacy = _key(col_ci_legacy) if col_ci_legacy else ""
    k_ci_status = _key(col_ci_status) if col_ci_status else ""
    k_ci_desc = _key(col_ci_desc) if col_ci_desc else ""
    k_ci_id = _key(col_ci_id) if col_ci_id else ""

    related_checkins = [
        c for c in (all_checkins or [])
        if k_ci_legacy and _key(_norm_value((c or {}).get(k_ci_legacy, ""))) == _key(legacy_id)
    ]
    related_checkins = related_checkins[-10:]
    recent_activity = _fmt_recent_activity(
        related_checkins=related_checkins,
        k_ci_status=k_ci_status,
        k_ci_desc=k_ci_desc,
        k_ci_id=k_ci_id,
    )

    recent_blob = f"{dispatch_date_str}\n{recent_activity}\n{packed_context}"
    stage = _infer_stage(dispatch_date_str=dispatch_date_str, recent_text_blob=recent_blob)

    process_material = _fmt_process_material(
        project_name=project_name,
        part_number=part_number,
        company_profile_text=company_profile_text,
    )

    def _llm_text(prompt: str) -> str:
        return str(lc_invoke(reg, "llm_generate_text", {"prompt": prompt}, state, fatal=True) or "")

    cues10 = _generate_10_cues_from_context(
        llm_text_fn=_llm_text,
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

    chips = _project_chips_from_10(cues10)

    col_out = lc_invoke(reg, "sheets_map_col", {"table": "project", "field": "ai_critcal_point"}, state, default="")
    ok = lc_invoke(
        reg,
        "sheets_update_project_cell_by_legacy_id",
        {"legacy_id": legacy_id, "column_name": col_out, "value": chips},
        state,
        default=False,
    )

    state["assembly_todo_written"] = bool(ok)
    state["assembly_todo_skipped"] = False
    state["assembly_todo_legacy_id"] = legacy_id
    state["assembly_todo_chips"] = chips
    state["assembly_todo_cues10"] = cues10

    if ok:
        state.setdefault("logs", []).append(f"assembly_todo: wrote chips to Project.ai_critcal_point legacy_id={legacy_id}")
    else:
        state.setdefault("logs", []).append(f"assembly_todo: FAILED writeback to Project.ai_critcal_point legacy_id={legacy_id}")

    # AppSheet cues sync (replace/update same 10 slots)
    try:
        if cues10:
            generated_at = _now_timestamp_str()
            cue_items = [
                {"cue_id": _slot_cue_id(tenant_id=tenant_id, legacy_id=legacy_id, slot=i), "cue": cues10[i - 1]}
                for i in range(1, 11)
            ]
            lc_invoke(
                reg,
                "appsheet_upsert_cues_rows",
                {"legacy_id": legacy_id, "cue_items": cue_items, "generated_at": generated_at},
                state,
                default=None,
            )
            state.setdefault("logs", []).append(f"appsheet_cues: upserted=10 legacy_id={legacy_id}")
    except Exception as e:
        state.setdefault("logs", []).append(f"appsheet_cues: non-fatal failure: {e}")

    return state