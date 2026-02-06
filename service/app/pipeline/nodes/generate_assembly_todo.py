from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import re
from datetime import datetime, date

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.llm_tool import LLMTool
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool
from .rerank_context import rerank_context
import json
import hashlib
import secrets
import string

from ...integrations.appsheet_client import AppSheetClient

# -------------------------
# Prompt loader
# -------------------------

def _find_repo_root(start: Path) -> Path:
    p = start
    for _ in range(8):
        if (p / "packages" / "prompts" / "assembly_todo.md").exists():
            return p
        if p.parent == p:
            break
        p = p.parent
    return start


def _load_prompt() -> str:
    here = Path(__file__).resolve()
    root = _find_repo_root(here.parent.parent.parent.parent)  # nodes -> pipeline -> app -> service -> repo
    path = root / "packages" / "prompts" / "assembly_todo.md"
    return path.read_text(encoding="utf-8")

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

def _rand_cue_id(n: int = 10) -> str:
    return "".join(secrets.choice(_ALPHANUM) for _ in range(n))

def _now_timestamp_str() -> str:
    # Match sheet style like: 01/07/26 12:49 PM
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.now(ZoneInfo("Asia/Kolkata"))
    except Exception:
        dt = datetime.now()
    return dt.strftime("%m/%d/%y %I:%M %p")

def _payload_hash(payload: dict) -> str:
    b = json.dumps(payload or {}, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b).hexdigest()

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

def _split_cues(text: str, max_items: int = 10) -> list[str]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    # remove bullets/numbering
    out: list[str] = []
    for ln in lines:
        ln = re.sub(r"^\-\s*\[\s*\]\s*", "", ln).strip()
        ln = re.sub(r"^\-\s*", "", ln).strip()
        ln = re.sub(r"^\•\s*", "", ln).strip()
        ln = re.sub(r"^\d+[\).]\s*", "", ln).strip()
        if ln:
            out.append(ln)
        if len(out) >= max_items:
            break
    # dedup (case-insensitive)
    seen = set()
    dedup: list[str] = []
    for x in out:
        k = re.sub(r"\s+", " ", x).strip().lower()
        if k and k not in seen:
            dedup.append(x)
            seen.add(k)
    return dedup[:max_items]
def _is_mfg(status: str) -> bool:
    return (status or "").strip().lower() == "mfg"


def _parse_date_loose(s: str) -> Optional[date]:
    """
    Accepts common formats: YYYY-MM-DD, DD-MM-YYYY, MM-DD-YYYY, DD/MM/YYYY, etc.
    Returns a date or None.
    """
    s = (s or "").strip()
    if not s:
        return None

    # ISO-ish
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

    # If it's an ISO datetime
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
    """
    Heuristic:
      - Prefer time remaining to dispatch if available
      - Else use keyword hints from recent activity
    """
    dd = _parse_date_loose(dispatch_date_str)
    if dd:
        today = datetime.now().date()
        days_left = (dd - today).days
        # loose bins
        if days_left >= 14:
            return f"Early Stage (time remaining ~{days_left}d)"
        if 4 <= days_left < 14:
            return f"Mid Stage (time remaining ~{days_left}d)"
        if days_left < 4:
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


def _fmt_vector_risks(
    *,
    problems: List[Dict[str, Any]],
    resolutions: List[Dict[str, Any]],
    ccp: List[Dict[str, Any]],
    dash: List[Dict[str, Any]],
    packed_context: str,
) -> str:
    """
    Make a tight "Vector Risks" block for the new prompt.
    Keep it readable for the model; avoid dumping huge text.
    """
    lines: List[str] = []

    # Prefer your reranked packed_context if present
    pc = (packed_context or "").strip()
    if pc:
        # Keep it bounded
        lines.append("RERANKED_CONTEXT_SNIPPETS:")
        # take first ~30 lines max
        pc_lines = [x.strip() for x in pc.splitlines() if x.strip()]
        lines.extend(["- " + x for x in pc_lines[:30]])
        return _compact_lines(lines, 40)

    # Fallback: build from buckets
    def take_summ(rows: List[Dict[str, Any]], k: int) -> List[str]:
        out = []
        for r in rows[:k]:
            s = (r.get("summary") or r.get("text") or r.get("update_message") or "").strip()
            if not s:
                continue
            s = s.replace("\n", " ")
            if len(s) > 220:
                s = s[:217] + "..."
            out.append(s)
        return out

    p = take_summ(problems, 6)
    r = take_summ(resolutions, 6)
    c = []
    for x in (ccp or [])[:6]:
        nm = (x.get("ccp_name") or "").strip()
        tx = (x.get("text") or "").strip().replace("\n", " ")
        if len(tx) > 180:
            tx = tx[:177] + "..."
        if nm and tx:
            c.append(f"{nm}: {tx}")
        elif tx:
            c.append(tx)

    d = take_summ(dash, 4)

    if p:
        lines.append("PAST_FAILURES:")
        lines.extend(["- " + x for x in p])
    if r:
        lines.append("WHAT_WORKED (RESOLUTIONS):")
        lines.extend(["- " + x for x in r])
    if c:
        lines.append("CCP_RISKS:")
        lines.extend(["- " + x for x in c])
    if d:
        lines.append("DASHBOARD_CONSTRAINTS:")
        lines.extend(["- " + x for x in d])

    return _compact_lines(lines, 40) or "(none found)"


def _fmt_recent_activity(*, related_checkins: List[Dict[str, Any]], sheets: SheetsTool) -> str:
    """
    Recent activity: include last few checkins in a short, model-friendly form.
    """
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
    """
    You don't have Process/RM/Boughtouts ingested yet, so keep this honest.
    """
    # Minimal but useful; once you ingest process/RM, extend this section.
    lines: List[str] = []
    if part_number:
        lines.append(f"Part: {part_number}")
    if project_name:
        lines.append(f"Project: {project_name}")
    if company_profile_text:
        # small hint can help material/process assumptions; still avoid guessing
        cp = company_profile_text.replace("\n", " ").strip()
        if len(cp) > 180:
            cp = cp[:177] + "..."
        lines.append(f"Client context: {cp}")
    if not lines:
        return "(unknown)"
    return " | ".join(lines)


def _clean_chips(text: str, *, stage_label: str) -> str:
    """
    Enforce STRICT output:
      - EXACTLY 3 or 4 lines
      - each line 6-8 words max
      - actionable micro-check vibe
    """
    s = (text or "").strip()
    raw_lines = [ln.strip() for ln in s.splitlines() if ln.strip()]

    # Normalize bullets/numbering
    cand: List[str] = []
    for ln in raw_lines:
        ln = re.sub(r"^\-\s*\[\s*\]\s*", "", ln).strip()  # remove "- [ ]"
        ln = re.sub(r"^\-\s*", "", ln).strip()
        ln = re.sub(r"^\•\s*", "", ln).strip()
        ln = re.sub(r"^\d+[\).]\s*", "", ln).strip()
        if not ln:
            continue
        cand.append(ln)

    # Dedup (case-insensitive)
    seen = set()
    dedup: List[str] = []
    for ln in cand:
        k = re.sub(r"\s+", " ", ln).strip().lower()
        if k and k not in seen:
            dedup.append(ln)
            seen.add(k)

    def clamp_words(line: str) -> str:
        # keep punctuation, just enforce word count
        words = [w for w in re.split(r"\s+", (line or "").strip()) if w]
        if len(words) > 8:
            words = words[:8]
        # If too short, pad with "(unknown)" as last token(s)
        while len(words) < 6:
            words.append("(unknown)")
        return " ".join(words).strip()

    chips = [clamp_words(x) for x in dedup if x]
    chips = chips[:4]

    # If model returned nothing usable, fallback by stage
    if not chips:
        chips = []

    # Ensure 3 items minimum
    def fallback_for_stage(stage: str) -> List[str]:
        st = (stage or "").lower()
        if "late" in st:
            return [
                "Quick look: inner corners paint coverage?",
                "Verify threads clean; no paint clogging?",
                "Snap: handling scratches near mating faces?",
                "Check fasteners: torque marked after tightening?",
            ]
        if "mid" in st:
            return [
                "Check spatter near bolt holes quickly?",
                "Quick look: corner undercut on joints?",
                "Verify heat distortion on flat faces?",
                "Check inside tube for hidden spatter?",
            ]
        # early/unknown
        return [
            "Quick look: backside scratches on sheets?",
            "Feel edge: burr present after cutting?",
            "Check diagonal: part is square now?",
            "Verify raw material grade marking visible?",
        ]

    if len(chips) < 3:
        for fb in fallback_for_stage(stage_label):
            chips.append(clamp_words(fb))
            if len(chips) >= 3:
                break

    # Keep 3 or 4
    chips = chips[:4]
    if len(chips) == 2:
        # safety (should not happen due to padding)
        chips.append(clamp_words("Quick verify: critical feature tolerance checked?"))

    # Prefer 3 unless we have strong 4
    if len(chips) > 4:
        chips = chips[:4]

    # Exactly 3 or 4 lines: if 4 exists ok, else 3
    if len(chips) == 4:
        return "\n".join(chips).strip()
    return "\n".join(chips[:3]).strip()

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

def _generate_10_cues_from_context(
    *,
    llm: LLMTool,
    company_context: str,
    snapshot: str,
    packed_context: str,
    closure_notes: str,
    attachment_context: str,
) -> list[str]:
    tmpl = _load_zai_cues_prompt()
    prompt = (
        tmpl.replace("{company_context}", company_context or "N/A")
        .replace("{snapshot}", snapshot or "N/A")
        .replace("{packed_context}", packed_context or "N/A")
        .replace("{closure_notes}", closure_notes or "N/A")
        .replace("{attachment_context}", attachment_context or "N/A")
    )
    raw = llm.generate_text(prompt)
    obj = _parse_json_loose(raw)
    cues = obj.get("cues") or []

    out: list[str] = []
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

    # hard-pad to 10 (keeps AppSheet shape stable)
    fallback = [
        "Contact supplier for updates, last time ese hi issues aaye the",
        "Check for rusty/scratched tools if surface finish is poor",
        "Critical dimension vernier se confirm kiya",
        "Packing se pehle final visual scan kar lo",
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


def _project_chips_from_10(cues10: list[str]) -> str:
    """
    Project tab wants 3-4 lines. Use first 3 (or 4 if strong).
    We keep it deterministic and from same source as AppSheet.
    """
    cues10 = [c for c in (cues10 or []) if (c or "").strip()]
    if not cues10:
        return ""

    # Prefer 3 lines by default
    picked = cues10[:3]

    # Add 4th only if it’s clearly distinct and present
    if len(cues10) >= 4:
        picked = cues10[:4]

    return "\n".join([_clamp_10_words(x) for x in picked]).strip()

# -------------------------
# Main node
# -------------------------

def generate_assembly_todo(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generates assembly "chips" for a Project.ID (legacy_id) and writes into Project.ai_critcal_point
    ONLY if Project.Status_assembly == 'mfg'.

    RAG strategy (kept):
      - project-scoped query
      - vector search: incidents(PROBLEM/RESOLUTION/MEDIA) + CCP chunks + dashboard updates + company profile
      - rerank/pack with existing rerank_context node
      - prompt uses PREVIOUS_CHIPS to allow "covered chips drop, new chips appear"
    """
    payload = state.get("payload") or {}
    legacy_id = _norm_value(payload.get("legacy_id") or state.get("legacy_id") or "")
    if not legacy_id:
        (state.get("logs") or []).append("assembly_todo: missing legacy_id; skipped")
        state["assembly_todo_skipped"] = True
        return state

    sheets = SheetsTool(settings)

    # Load project row (ID-first)
    pr = sheets.get_project_by_legacy_id(legacy_id)
    if not pr:
        (state.get("logs") or []).append(f"assembly_todo: project row not found for legacy_id={legacy_id}; skipped")
        state["assembly_todo_skipped"] = True
        return state

    # Status gate
    k_status = _key(sheets.map.col("project", "status_assembly"))
    status_val = _norm_value(pr.get(k_status, ""))
    if not _is_mfg(status_val):
        (state.get("logs") or []).append(f"assembly_todo: status_assembly='{status_val}' != 'mfg'; skipped")
        state["assembly_todo_skipped"] = True
        return state

    # Basic project info
    k_pname = _key(sheets.map.col("project", "project_name"))
    k_part = _key(sheets.map.col("project", "part_number"))
    k_tenant = _key(sheets.map.col("project", "company_row_id"))
    k_prev = _key(sheets.map.col("project", "ai_critcal_point"))

    # dispatch_date exists in mapping (used for stage inference)
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
        (state.get("logs") or []).append(
            f"assembly_todo: missing tenant_id(company_row_id) for legacy_id={legacy_id}; skipped"
        )
        state["assembly_todo_skipped"] = True
        return state

    # ---- Build retrieval query (project-scoped) ----
    query_text = (
        f"Micro inspection cues to avoid blind spots during manufacturing.\n"
        f"PROJECT_NAME: {project_name or '(unknown)'}\n"
        f"PART_NUMBER: {part_number or '(unknown)'}\n"
        f"LEGACY_ID: {legacy_id}\n"
        f"Use past failures, CCP requirements, resolutions that worked, and recent updates.\n"
        f"Output should be 3-4 quick micro-checks."
    ).strip()

    embedder = EmbedTool(settings)
    vector_db = VectorTool(settings)

    try:
        q = embedder.embed_query(query_text)
    except Exception as e:
        (state.get("logs") or []).append(f"assembly_todo: embed_query failed: {e}")
        state["assembly_todo_skipped"] = True
        return state

    # ---- Vector retrieval (STRICT legacy_id; stays per project) ----
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

    # Company profile (exact tenant row id)
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

    # ---- Reuse reranker + packer (kept RAG-based) ----
    tmp_state = {
        "thread_snapshot_text": query_text,
        "similar_problems": problems,
        "similar_resolutions": resolutions,
        "similar_media": media,
        "relevant_ccp_chunks": ccp,
        "relevant_dashboard_updates": dash,
        "logs": [],
    }
    tmp_state = rerank_context(settings, tmp_state)
    packed_context = _norm_value(tmp_state.get("packed_context", ""))

    # Also include last few checkins from sheet (current reality)
    k_ci_legacy = _key(sheets.map.col("checkin", "legacy_id"))
    all_checkins = sheets.list_checkins()
    related_checkins = [
        c for c in (all_checkins or [])
        if _key(_norm_value((c or {}).get(k_ci_legacy, ""))) == _key(legacy_id)
    ]
    related_checkins = related_checkins[-10:]

    recent_activity = _fmt_recent_activity(related_checkins=related_checkins, sheets=sheets)

    # Stage inference uses dispatch_date + recent activity blob
    recent_blob = f"{dispatch_date_str}\n{recent_activity}\n{packed_context}"
    stage = _infer_stage(dispatch_date_str=dispatch_date_str, recent_text_blob=recent_blob)

    process_material = _fmt_process_material(
        project_name=project_name,
        part_number=part_number,
        company_profile_text=company_profile_text,
    )

    vector_risks = _fmt_vector_risks(
        problems=problems,
        resolutions=resolutions,
        ccp=ccp,
        dash=dash,
        packed_context=packed_context,
    )

    llm = LLMTool(settings)

    # Single source: generate 10 cues from reranked packed context
    cues10 = _generate_10_cues_from_context(
        llm=llm,
        company_context=company_profile_text,
        snapshot=query_text,                 # project-scoped snapshot
        packed_context=packed_context,
        closure_notes="",                    # project node doesn't have closure; keep empty
        attachment_context="",               # project node doesn't have attachments; keep empty
    )

    # Project tab uses 3–4 lines derived from SAME cues10
    chips = _project_chips_from_10(cues10)

    # Write back to Project tab (same column)
    col_out = sheets.map.col("project", "ai_critcal_point")
    ok = sheets.update_project_cell_by_legacy_id(legacy_id, column_name=col_out, value=chips)

    state["assembly_todo_written"] = bool(ok)
    state["assembly_todo_skipped"] = False
    state["assembly_todo_legacy_id"] = legacy_id
    state["assembly_todo_chips"] = chips
    state["assembly_todo_cues10"] = cues10

    if ok:
        (state.get("logs") or []).append(f"assembly_todo: wrote chips to Project.ai_critcal_point legacy_id={legacy_id}")
    else:
        (state.get("logs") or []).append(f"assembly_todo: FAILED writeback to Project.ai_critcal_point legacy_id={legacy_id}")

    # -------------------------
    # Sync the SAME 10 cues into AppSheet (replace/update same 10 slots)
    # -------------------------
    try:
        client = AppSheetClient(settings)
        if client.enabled() and cues10:
            generated_at = _now_timestamp_str()
            cue_items = [
                {"cue_id": _slot_cue_id(tenant_id=tenant_id, legacy_id=legacy_id, slot=i), "cue": cues10[i - 1]}
                for i in range(1, 11)
            ]
            client.upsert_cues_rows(
                legacy_id=legacy_id,
                cue_items=cue_items,
                generated_at=generated_at,
            )
            (state.get("logs") or []).append(f"appsheet_cues: upserted=10 legacy_id={legacy_id}")
    except Exception as e:
        (state.get("logs") or []).append(f"appsheet_cues: non-fatal failure: {e}")

    return state