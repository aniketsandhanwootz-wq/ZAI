from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import re

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.llm_tool import LLMTool


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


def _is_mfg(status: str) -> bool:
    return (status or "").strip().lower() == "mfg"


def _clean_checklist(text: str) -> str:
    """
    Force output to 5-6 checklist lines: "- [ ] ..."
    """
    s = (text or "").strip()
    lines = [ln.strip() for ln in s.splitlines() if ln.strip()]

    picked: List[str] = []
    for ln in lines:
        # Accept common bullets, normalize later
        if ln.startswith("- [ ]"):
            picked.append(ln)
        elif ln.startswith("-"):
            picked.append("- [ ] " + ln.lstrip("-").strip())
        elif re.match(r"^\d+[\).]\s+", ln):
            picked.append("- [ ] " + re.sub(r"^\d+[\).]\s+", "", ln).strip())

    if not picked:
        # fallback: treat each non-empty line as item
        for ln in lines:
            picked.append("- [ ] " + ln)

    picked = [p for p in picked if p.startswith("- [ ] ") and len(p) > 6]
    picked = picked[:6]

    # ensure 5 items minimum (pad with unknowns)
    while len(picked) < 5:
        picked.append("- [ ] Verify pending items are identified (unknown)")

    # if 6th exists ok, else keep 5
    return "\n".join(picked[:6]).strip()


def generate_assembly_todo(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generates assembly checklist for a Project.ID (legacy_id) and writes into Project.ai_critcal_point
    ONLY if Project.Status_assembly == 'mfg'.
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

    # Status gate: only generate after status is mfg
    k_status = _key(sheets.map.col("project", "status_assembly"))
    status_val = _norm_value(pr.get(k_status, ""))
    if not _is_mfg(status_val):
        (state.get("logs") or []).append(f"assembly_todo: status_assembly='{status_val}' != 'mfg'; skipped")
        state["assembly_todo_skipped"] = True
        return state

    # Basic project info
    k_pname = _key(sheets.map.col("project", "project_name"))
    k_part = _key(sheets.map.col("project", "part_number"))
    project_name = _norm_value(pr.get(k_pname, ""))
    part_number = _norm_value(pr.get(k_part, ""))

    # Gather context from sheets (cheap + cached)
    # Recent checkins for this legacy_id
    k_ci_legacy = _key(sheets.map.col("checkin", "legacy_id"))
    k_ci_status = _key(sheets.map.col("checkin", "status"))
    k_ci_desc = _key(sheets.map.col("checkin", "description"))
    k_ci_id = _key(sheets.map.col("checkin", "checkin_id"))

    all_checkins = sheets.list_checkins()
    related = [c for c in (all_checkins or []) if _key(_norm_value((c or {}).get(k_ci_legacy, ""))) == _key(legacy_id)]
    related = related[-10:]  # last 10 in sheet order

    # Conversations for last 3 checkins (cap)
    conv_lines: List[str] = []
    for c in related[-3:]:
        cid = _norm_value((c or {}).get(k_ci_id, ""))
        if not cid:
            continue
        convos = sheets.get_conversations_for_checkin(cid)
        for rr in (convos or [])[-10:]:
            remark = _norm_value(rr.get("remarks", "")) or _norm_value(rr.get("remark", ""))
            st = _norm_value(rr.get("status", ""))
            if remark or st:
                conv_lines.append(f"[{st}] {remark}".strip() if st else remark)
        if len(conv_lines) >= 20:
            break
    conv_lines = conv_lines[-20:]

    # CCP rows
    k_ccp_legacy = _key(sheets.map.col("ccp", "legacy_id"))
    k_ccp_name = _key(sheets.map.col("ccp", "ccp_name"))
    k_ccp_desc = _key(sheets.map.col("ccp", "description"))

    ccp_rows = sheets.list_ccp()
    ccp_rel = [r for r in (ccp_rows or []) if _key(_norm_value((r or {}).get(k_ccp_legacy, ""))) == _key(legacy_id)]
    ccp_rel = ccp_rel[:10]

    # Dashboard updates
    k_d_legacy = _key(sheets.map.col("dashboard_update", "legacy_id"))
    k_d_msg = _key(sheets.map.col("dashboard_update", "update_message"))
    dash_rows = sheets.list_dashboard_updates()
    dash_rel = [r for r in (dash_rows or []) if _key(_norm_value((r or {}).get(k_d_legacy, ""))) == _key(legacy_id)]
    dash_rel = dash_rel[-10:]

    # Build context string
    ctx: List[str] = []
    ctx.append(f"PROJECT_NAME: {project_name or '(unknown)'}")
    ctx.append(f"PART_NUMBER: {part_number or '(unknown)'}")
    ctx.append(f"LEGACY_ID: {legacy_id}")
    ctx.append(f"STATUS_ASSEMBLY: {status_val or '(unknown)'}")
    ctx.append("")

    if related:
        ctx.append("RECENT_CHECKINS:")
        for c in related[-6:]:
            cid = _norm_value((c or {}).get(k_ci_id, ""))
            st = _norm_value((c or {}).get(k_ci_status, ""))
            desc = _norm_value((c or {}).get(k_ci_desc, ""))
            line = f"- checkin_id={cid} status={st} desc={desc}".strip()
            ctx.append(line)
        ctx.append("")

    if conv_lines:
        ctx.append("RECENT_CONVERSATION_SNIPPETS:")
        for ln in conv_lines[-12:]:
            ctx.append(f"- {ln}")
        ctx.append("")

    if ccp_rel:
        ctx.append("CCP_LIST:")
        for r in ccp_rel[:8]:
            nm = _norm_value((r or {}).get(k_ccp_name, ""))
            ds = _norm_value((r or {}).get(k_ccp_desc, ""))
            ctx.append(f"- {nm}: {ds}".strip(": "))
        ctx.append("")

    if dash_rel:
        ctx.append("DASHBOARD_UPDATES:")
        for r in dash_rel[-8:]:
            msg = _norm_value((r or {}).get(k_d_msg, ""))
            if msg:
                ctx.append(f"- {msg}")
        ctx.append("")

    prompt_template = _load_prompt()
    final_prompt = prompt_template.replace("{{context}}", "\n".join(ctx).strip())

    llm = LLMTool(settings)
    raw = llm.generate_text(final_prompt)
    checklist = _clean_checklist(raw)

    # Write back to Project tab
    col_out = sheets.map.col("project", "ai_critcal_point")
    ok = sheets.update_project_cell_by_legacy_id(legacy_id, column_name=col_out, value=checklist)

    state["assembly_todo"] = checklist
    state["assembly_todo_written"] = bool(ok)
    (state.get("logs") or []).append(
        f"assembly_todo: generated and writeback={'ok' if ok else 'failed'} legacy_id={legacy_id}"
    )
    return state