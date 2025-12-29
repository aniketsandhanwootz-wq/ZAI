from __future__ import annotations

from typing import Any, Dict, Optional, List
import re

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.company_tool import CompanyTool


_CLOSURE_HINTS = re.compile(
    r"\b(resolved|fixed|rework|reworked|ok now|closed|passed|pass|accepted|root cause|rca|tool change|offset|fixture|grind|stress\s*relief|heat\s*treat|calibrat|corrected)\b",
    re.IGNORECASE,
)


def _extract_closure_notes(convo_rows: List[Dict[str, Any]]) -> str:
    picked: List[str] = []

    for r in (convo_rows or [])[-25:]:
        remark = _norm_value(r.get("remarks", "")) or _norm_value(r.get("remark", ""))
        st = _norm_value(r.get("status", ""))
        if not remark and not st:
            continue

        line = f"[{st}] {remark}".strip() if st else remark
        if not line:
            continue

        if st.strip().upper() in ("PASS", "PASSED", "FAIL", "FAILED", "CLOSED", "DONE", "OK"):
            picked.append(line)
            continue

        if _CLOSURE_HINTS.search(line):
            picked.append(line)

    out: List[str] = []
    seen = set()
    for x in picked:
        k = x.strip().lower()
        if k and k not in seen:
            out.append(x)
            seen.add(k)

    out = out[-8:]
    return "\n- " + "\n- ".join(out) if out else ""


def load_sheet_data(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    sheets = SheetsTool(settings)
    payload = state.get("payload") or {}

    checkin_id = payload.get("checkin_id")
    conversation_id = payload.get("conversation_id")
    ccp_id = payload.get("ccp_id")
    legacy_id = payload.get("legacy_id")

    state["checkin_id"] = checkin_id
    state["conversation_id"] = conversation_id
    state["ccp_id"] = ccp_id
    state["legacy_id"] = legacy_id
    state["event_type"] = payload.get("event_type", "")

    k_ci_project = _key(sheets.map.col("checkin", "project_name"))
    k_ci_part = _key(sheets.map.col("checkin", "part_number"))
    k_ci_legacy = _key(sheets.map.col("checkin", "legacy_id"))
    k_ci_status = _key(sheets.map.col("checkin", "status"))
    k_ci_desc = _key(sheets.map.col("checkin", "description"))

    checkin_row: Optional[Dict[str, Any]] = None
    if checkin_id:
        checkin_row = sheets.get_checkin_by_id(str(checkin_id))
    state["checkin_row"] = checkin_row

    project_name = _norm_value((checkin_row or {}).get(k_ci_project, ""))
    part_number = _norm_value((checkin_row or {}).get(k_ci_part, ""))
    legacy_id_from_checkin = _norm_value((checkin_row or {}).get(k_ci_legacy, ""))

    if legacy_id_from_checkin:
        legacy_id = legacy_id_from_checkin
        state["legacy_id"] = legacy_id

    state["project_name"] = project_name or None
    state["part_number"] = part_number or None
    state["checkin_status"] = _norm_value((checkin_row or {}).get(k_ci_status, ""))
    state["checkin_description"] = _norm_value((checkin_row or {}).get(k_ci_desc, ""))

    tenant_id = ""
    project_row = None
    if project_name and part_number and legacy_id:
        project_row = sheets.get_project_row(project_name, part_number, legacy_id)
        if project_row:
            k_tenant = _key(sheets.map.col("project", "company_row_id"))
            tenant_id = _norm_value(project_row.get(k_tenant, ""))
    state["project_row"] = project_row
    state["tenant_id"] = tenant_id or None

    convo_rows = []
    if checkin_id:
        convo_rows = sheets.get_conversations_for_checkin(str(checkin_id))
    state["conversation_rows"] = convo_rows

    # ✅ Closure memory candidates from conversation
    state["closure_notes"] = _extract_closure_notes(convo_rows)

    # ✅ Phase 2: Company context via Glide (optional)
    state["company_name"] = None
    state["company_description"] = None
    state["company_key"] = None
    if tenant_id:
        try:
            ct = CompanyTool(settings)
            ctx = ct.get_company_context(tenant_id)
            if ctx:
                state["company_name"] = ctx.company_name or None
                state["company_description"] = ctx.company_description or None
                state["company_key"] = ctx.company_key or None
                (state.get("logs") or []).append(
                    f"Loaded company context via Glide: name='{ctx.company_name}' key='{ctx.company_key}'"
                )
        except Exception as e:
            (state.get("logs") or []).append(f"Company context lookup failed (non-fatal): {e}")

    (state.get("logs") or []).append("Loaded sheet data (checkin/project/conversation) + extracted closure notes")
    return state
