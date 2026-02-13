from __future__ import annotations

from typing import Any, Dict, Optional, List
import re

from ...config import Settings
from ..lc_runtime import lc_registry, lc_invoke


_CLOSURE_HINTS = re.compile(
    r"\b(resolved|fixed|rework|reworked|ok now|closed|passed|pass|accepted|root cause|rca|tool change|offset|fixture|grind|stress\s*relief|heat\s*treat|calibrat|corrected)\b",
    re.IGNORECASE,
)


def _norm_value(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x.strip()
    return str(x).strip()


def _key(s: Any) -> str:
    return re.sub(r"\s+", " ", _norm_value(s)).strip().lower()


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


def _norm_header(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _find_row_value(row: Dict[str, Any], *, preferred_key: Optional[str], fallbacks: List[str]) -> str:
    if not row:
        return ""

    if preferred_key and preferred_key in row:
        v = _norm_value(row.get(preferred_key, ""))
        if v:
            return v

    norm_map = {_norm_header(k): k for k in row.keys()}
    for fb in fallbacks:
        k = norm_map.get(_norm_header(fb))
        if not k:
            continue
        v = _norm_value(row.get(k, ""))
        if v:
            return v

    return ""

def _as_list(x: Any) -> List[Any]:
    """
    Normalize tool outputs that might come as:
      - list
      - {"rows": [...]}
      - {"result": {"rows": [...]}} (handled by lc_invoke unwrap, but keep defensive)
      - None
    """
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        v = x.get("rows")
        if isinstance(v, list):
            return v
    return []

def _as_row(x: Any) -> Optional[Dict[str, Any]]:
    """
    Normalize a single row that might come as:
      - dict (row)
      - {"row": {...}}
      - None
    """
    if x is None:
        return None
    if isinstance(x, dict):
        r = x.get("row")
        if isinstance(r, dict):
            return r
        return x
    return None
def _drive_view_url(file_id: str) -> str:
    fid = (file_id or "").strip()
    if not fid:
        return ""
    return f"https://drive.google.com/uc?export=view&id={fid}"


def load_sheet_data(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    reg = lc_registry(settings, state)
    payload = state.get("payload") or {}

    # ---- IMPORTANT: Avoid stale sheet reads across worker runs ----
    # ToolRegistry (and thus SheetsTool) is cached per process; refresh the key tabs per run.
    lc_invoke(reg, "sheets_refresh_cache", {"tab_key": "checkin"}, state, default=None)
    lc_invoke(reg, "sheets_refresh_cache", {"tab_key": "project"}, state, default=None)
    lc_invoke(reg, "sheets_refresh_cache", {"tab_key": "conversation"}, state, default=None)
    # -------------------------
    # Meta flags / overrides
    # -------------------------
    meta = payload.get("meta") or {}
    state["meta"] = meta

    state["ingest_only"] = bool(meta.get("ingest_only") or payload.get("ingest_only") or False)
    state["media_only"] = bool(meta.get("media_only") or payload.get("media_only") or False)

    meta_tenant_id = _norm_value(meta.get("tenant_id") or "")
    if meta_tenant_id:
        state["tenant_id"] = meta_tenant_id

    checkin_id = payload.get("checkin_id")
    conversation_id = payload.get("conversation_id")
    ccp_id = payload.get("ccp_id")
    legacy_id = payload.get("legacy_id")

    state["checkin_id"] = checkin_id
    state["conversation_id"] = conversation_id
    state["ccp_id"] = ccp_id
    state["legacy_id"] = legacy_id
    state["event_type"] = payload.get("event_type", "")

    # Mapping keys via tool
    col_ci_project = lc_invoke(reg, "sheets_map_col", {"table": "checkin", "field": "project_name"}, state, default="")
    col_ci_part = lc_invoke(reg, "sheets_map_col", {"table": "checkin", "field": "part_number"}, state, default="")
    col_ci_legacy = lc_invoke(reg, "sheets_map_col", {"table": "checkin", "field": "legacy_id"}, state, default="")
    col_ci_status = lc_invoke(reg, "sheets_map_col", {"table": "checkin", "field": "status"}, state, default="")
    col_ci_desc = lc_invoke(reg, "sheets_map_col", {"table": "checkin", "field": "description"}, state, default="")

    k_ci_project = _key(col_ci_project)
    k_ci_part = _key(col_ci_part)
    k_ci_legacy = _key(col_ci_legacy)
    k_ci_status = _key(col_ci_status)
    k_ci_desc = _key(col_ci_desc)

    checkin_row: Optional[Dict[str, Any]] = None
    if checkin_id:
        checkin_row = lc_invoke(
            reg,
            "sheets_get_checkin_by_id",
            {"checkin_id": str(checkin_id)},
            state,
            default=None,
        )
    checkin_row = _as_row(checkin_row)
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

    # -------------------------
    # Extra fields needed for Teams post formatting
    # -------------------------
    col_ci_created_by = lc_invoke(reg, "sheets_map_col", {"table": "checkin", "field": "created_by"}, state, default="")
    col_ci_item_id = lc_invoke(reg, "sheets_map_col", {"table": "checkin", "field": "id"}, state, default="")
    col_ci_insp_img = lc_invoke(reg, "sheets_map_col", {"table": "checkin", "field": "inspection_image"}, state, default="")

    k_ci_created_by = _key(col_ci_created_by) if col_ci_created_by else None
    k_ci_item_id = _key(col_ci_item_id) if col_ci_item_id else None
    k_ci_insp_img = _key(col_ci_insp_img) if col_ci_insp_img else None

    created_by = _find_row_value(
        checkin_row or {},
        preferred_key=k_ci_created_by,
        fallbacks=["Created by", "Created By", "Creator", "created by"],
    )

    item_id = _find_row_value(
        checkin_row or {},
        preferred_key=k_ci_item_id,
        fallbacks=["ID", "Id", "Part ID", "Unique ID"],
    )

    insp_cell = _find_row_value(
        checkin_row or {},
        preferred_key=k_ci_insp_img,
        fallbacks=[
            "Inspection Image URL",
            "Inspection Image",
            "Inspection Images",
            "CheckIn Image",
            "CheckIn Images",
            "Image",
            "Images",
        ],
    )

    state["checkin_created_by"] = created_by or None
    state["checkin_item_id"] = item_id or None

    # Resolve inspection images into URLs (best-effort)
    refs = lc_invoke(reg, "attachment_split_cell_refs", {"cell": insp_cell or ""}, state, default=[]) or []
    state["checkin_image_refs"] = refs

    urls: List[str] = []
    for ref in refs[:3]:
        att = lc_invoke(reg, "attachment_resolve", {"ref": ref}, state, default=None)
        if not att:
            continue

        direct_url = (att.get("direct_url") or "").strip()
        if direct_url:
            urls.append(direct_url)
            continue

        drive_file_id = (att.get("drive_file_id") or "").strip()
        if drive_file_id:
            u = _drive_view_url(drive_file_id)
            if u:
                urls.append(u)

    state["checkin_image_urls"] = urls

    # -------------------------
    # Resolve tenant_id via Project sheet (prefer legacy_id lookup)
    # -------------------------
    tenant_id = _norm_value(state.get("tenant_id") or "")
    project_row = None

    if not tenant_id:
        # 1) Primary: by legacy_id (fast + precise if mapping is correct)
        if legacy_id:
            project_row = lc_invoke(
                reg,
                "sheets_get_project_by_legacy_id",
                {"legacy_id": str(legacy_id)},
                state,
                default=None,
            )

        # 2) Secondary: (only if legacy_id exists; your SheetsTool likely requires it)
        if not project_row and project_name and part_number and legacy_id:
            project_row = lc_invoke(
                reg,
                "sheets_get_project_row_triplet",
                {"project_name": project_name, "part_number": part_number, "legacy_id": legacy_id},
                state,
                default=None,
            )

        # 3) Fallback: scan projects by Project name + Part number
        #    This fixes cases where mapping for "legacy_id" lookup is broken or payload doesn't carry legacy_id reliably.
        if not project_row and project_name and part_number:
            all_projects = _as_list(lc_invoke(reg, "sheets_list_projects", {}, state, default=[]))

            def _row_proj_name(r: Dict[str, Any]) -> str:
                return _find_row_value(
                    r,
                    preferred_key=None,
                    fallbacks=["Project name", "Project Name", "project name"],
                )

            def _row_part_no(r: Dict[str, Any]) -> str:
                return _find_row_value(
                    r,
                    preferred_key=None,
                    fallbacks=["Part number", "Part Number", "part number", "Part no", "Part No"],
                )

            want_pn = _key(project_name)
            want_part = _key(part_number)

            best = None
            for r in all_projects:
                if _key(_row_proj_name(r)) == want_pn and _key(_row_part_no(r)) == want_part:
                    best = r
                    break

            project_row = best
        project_row = _as_row(project_row)
        if project_row:
            # tenant/company row id mapping (robust):
            # - First try mapping.yaml via sheets_map_col(project, company_row_id)
            # - If mapping missing/wrong, fallback to literal header "Company row id"
            col_tenant = lc_invoke(
                reg,
                "sheets_map_col",
                {"table": "project", "field": "company_row_id"},
                state,
                default="",
            )
            tenant_id = _find_row_value(
                project_row,
                preferred_key=_key(col_tenant) if col_tenant else None,
                fallbacks=[
                    "Company row id",
                    "Company Row ID",
                    "Company row ID",
                    "Company Row Id",
                    "Company RowID",
                ],
            )

            # Fill missing values from project row (nice-to-have; also robust to mapping issues)
            if not project_name:
                project_name = _find_row_value(
                    project_row,
                    preferred_key=None,
                    fallbacks=["Project name", "Project Name", "project name"],
                )
            if not part_number:
                part_number = _find_row_value(
                    project_row,
                    preferred_key=None,
                    fallbacks=["Part number", "Part Number", "part number", "Part no", "Part No"],
                )

            state["project_name"] = project_name or None
            state["part_number"] = part_number or None

    state["project_row"] = project_row
    state["tenant_id"] = tenant_id or None

    convo_rows = []
    if checkin_id:
        convo_rows = _as_list(
            lc_invoke(
                reg,
                "sheets_get_conversations_for_checkin",
                {"checkin_id": str(checkin_id)},
                state,
                default=[],
            )
        )
    state["conversation_rows"] = convo_rows

    state["closure_notes"] = _extract_closure_notes(convo_rows)

    # -------------------------
    # Company routing + description fallback (via tools)
    # -------------------------
    state["company_name"] = None
    state["company_description"] = None
    state["company_key"] = None

    try:
        proj_ctx = lc_invoke(
            reg,
            "company_from_project_name",
            {"project_name": project_name or "", "tenant_row_id": tenant_id or ""},
            state,
            default=None,
        )
        if proj_ctx:
            state["company_name"] = proj_ctx.get("company_name") or None
            state["company_key"] = proj_ctx.get("company_key") or None
            state["company_description"] = None
            state.setdefault("logs", []).append(
                f"Derived company from project_name: company='{state['company_name']}' key='{state['company_key']}'"
            )

        if tenant_id:
            glide_ctx = lc_invoke(
                reg,
                "company_get_company_context",
                {"tenant_row_id": tenant_id},
                state,
                default=None,
            )
            if glide_ctx and (glide_ctx.get("company_name") or "").strip():
                state["company_name"] = glide_ctx.get("company_name") or state["company_name"]
                state["company_description"] = glide_ctx.get("company_description") or None
                state["company_key"] = glide_ctx.get("company_key") or state["company_key"]
                state.setdefault("logs", []).append(
                    f"Loaded company context via Glide (override): name='{state['company_name']}' key='{state['company_key']}'"
                )

        tenant_row_id = (state.get("tenant_id") or "").strip()
        desc_final = (state.get("company_description") or "").strip()
        company_name_final = (state.get("company_name") or "").strip()

        if tenant_row_id and desc_final:
            emb = lc_invoke(
                reg,
                "embed_text",
                {"text": f"Company: {company_name_final}\n{desc_final}"},
                state,
                fatal=True,
            )
            lc_invoke(
                reg,
                "vector_upsert_company_profile",
                {
                    "tenant_row_id": tenant_row_id,
                    "company_name": company_name_final,
                    "company_description": desc_final,
                    "embedding": emb,
                },
                state,
                fatal=False,
            )
            state.setdefault("logs", []).append("Upserted company profile vector (company_vectors) via LC tools")
    except Exception as e:
        state.setdefault("logs", []).append(f"Company routing build failed (non-fatal): {e}")

    state.setdefault("logs", []).append("Loaded sheet data (checkin/project/conversation) + extracted closure notes + company routing")
    return state