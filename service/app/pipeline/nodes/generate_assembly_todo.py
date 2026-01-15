from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
import re

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.llm_tool import LLMTool
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool
from .rerank_context import rerank_context


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
        if ln.startswith("- [ ]"):
            picked.append(ln)
        elif ln.startswith("-"):
            picked.append("- [ ] " + ln.lstrip("-").strip())
        elif re.match(r"^\d+[\).]\s+", ln):
            picked.append("- [ ] " + re.sub(r"^\d+[\).]\s+", "", ln).strip())

    if not picked:
        for ln in lines:
            picked.append("- [ ] " + ln)

    picked = [p for p in picked if p.startswith("- [ ] ") and len(p) > 6]
    picked = picked[:6]

    while len(picked) < 5:
        picked.append("- [ ] Verify pending items are identified (unknown)")

    return "\n".join(picked[:6]).strip()


def generate_assembly_todo(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generates assembly checklist for a Project.ID (legacy_id) and writes into Project.ai_critcal_point
    ONLY if Project.Status_assembly == 'mfg'.

    RAG strategy:
      - Build a project-scoped query
      - Vector search: incidents(PROBLEM/RESOLUTION/MEDIA) + CCP chunks + dashboard updates + company profile
      - Rerank/pack with existing rerank_context node
      - Prompt uses PREVIOUS_CHECKLIST to allow "covered items drop, new items appear"
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

    project_name = _norm_value(pr.get(k_pname, ""))
    part_number = _norm_value(pr.get(k_part, ""))
    tenant_id = _norm_value(pr.get(k_tenant, ""))

    prev_checklist = _norm_value(pr.get(k_prev, ""))

    if not tenant_id:
        (state.get("logs") or []).append(f"assembly_todo: missing tenant_id(company_row_id) for legacy_id={legacy_id}; skipped")
        state["assembly_todo_skipped"] = True
        return state

    # ---- Build retrieval query (project-scoped) ----
    query_text = (
        f"Manufacturing critical checklist for assembly project.\n"
        f"PROJECT_NAME: {project_name or '(unknown)'}\n"
        f"PART_NUMBER: {part_number or '(unknown)'}\n"
        f"LEGACY_ID: {legacy_id}\n"
        f"Focus: CCP must-haves + required proofs, past similar problems, what resolutions worked, "
        f"recent dashboard constraints, vendor risks, drawing/revision alignment, final acceptance before dispatch."
    ).strip()

    embedder = EmbedTool(settings)
    vector_db = VectorTool(settings)

    try:
        q = embedder.embed_query(query_text)
    except Exception as e:
        (state.get("logs") or []).append(f"assembly_todo: embed_query failed: {e}")
        state["assembly_todo_skipped"] = True
        return state

    # ---- Vector retrieval (STRICT legacy_id so it stays per project row) ----
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

    # ---- Reuse your reranker + packer ----
    # rerank_context expects these keys
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

    # Also include last few checkins from sheet (cheap, current reality)
    k_ci_legacy = _key(sheets.map.col("checkin", "legacy_id"))
    k_ci_status = _key(sheets.map.col("checkin", "status"))
    k_ci_desc = _key(sheets.map.col("checkin", "description"))
    k_ci_id = _key(sheets.map.col("checkin", "checkin_id"))

    all_checkins = sheets.list_checkins()
    related = [
        c for c in (all_checkins or [])
        if _key(_norm_value((c or {}).get(k_ci_legacy, ""))) == _key(legacy_id)
    ]
    related = related[-10:]

    # ---- Build prompt context ----
    ctx: List[str] = []
    ctx.append(f"PROJECT_NAME: {project_name or '(unknown)'}")
    ctx.append(f"PART_NUMBER: {part_number or '(unknown)'}")
    ctx.append(f"LEGACY_ID: {legacy_id}")
    ctx.append(f"TENANT_ID(company_row_id): {tenant_id}")
    ctx.append(f"STATUS_ASSEMBLY: {status_val or '(unknown)'}")
    ctx.append("")

    if prev_checklist:
        ctx.append("PREVIOUS_CHECKLIST:")
        ctx.append(prev_checklist.strip())
        ctx.append("")

    if company_profile_text:
        ctx.append("COMPANY_PROFILE:")
        ctx.append(company_profile_text)
        ctx.append("")

    if related:
        ctx.append("RECENT_CHECKINS (sheet):")
        for c in related[-6:]:
            cid = _norm_value((c or {}).get(k_ci_id, ""))
            st = _norm_value((c or {}).get(k_ci_status, ""))
            desc = _norm_value((c or {}).get(k_ci_desc, ""))
            ctx.append(f"- checkin_id={cid} status={st} desc={desc}".strip())
        ctx.append("")

    if packed_context:
        ctx.append("RAG_CONTEXT (vector retrieval; reranked & packed):")
        ctx.append(packed_context)
        ctx.append("")
    else:
        ctx.append("RAG_CONTEXT: (none found)")
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
        f"assembly_todo: RAG used (problems={len(problems)} res={len(resolutions)} ccp={len(ccp)} dash={len(dash)})"
    )
    (state.get("logs") or []).append(
        f"assembly_todo: generated and writeback={'ok' if ok else 'failed'} legacy_id={legacy_id}"
    )
    return state