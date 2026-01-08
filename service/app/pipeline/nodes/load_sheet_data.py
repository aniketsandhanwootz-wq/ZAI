from __future__ import annotations

from typing import Any, Dict, Optional, List
import re

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.company_tool import CompanyTool
from ...tools.company_cache_tool import CompanyCacheTool
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool

from ...tools.drive_tool import DriveTool
from ...tools.attachment_tool import AttachmentResolver, split_cell_refs


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

def _norm_header(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _find_row_value(row: Dict[str, Any], *, preferred_key: Optional[str], fallbacks: List[str]) -> str:
    """
    Return the first NON-EMPTY value.
    - Try preferred_key first
    - Then try fallback headers
    """
    if not row:
        return ""

    # 1) preferred_key
    if preferred_key and preferred_key in row:
        v = _norm_value(row.get(preferred_key, ""))
        if v:  # ✅ skip empty
            return v

    # 2) fallbacks (first non-empty wins)
    norm_map = {_norm_header(k): k for k in row.keys()}
    for fb in fallbacks:
        k = norm_map.get(_norm_header(fb))
        if not k:
            continue
        v = _norm_value(row.get(k, ""))
        if v:  # ✅ skip empty
            return v

    return ""


def _drive_view_url(file_id: str) -> str:
    # Works well with Adaptive Cards IF the file is accessible by link.
    # If not accessible, you must share/permit it (see note below).
    fid = (file_id or "").strip()
    if not fid:
        return ""
    return f"https://drive.google.com/uc?export=view&id={fid}"


def load_sheet_data(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    sheets = SheetsTool(settings)
    payload = state.get("payload") or {}

    # -------------------------
    # ✅ Meta flags / overrides (important for admin media backfill)
    # -------------------------
    meta = payload.get("meta") or {}
    state["meta"] = meta

    # Flags can be passed either at top-level or inside meta
    state["ingest_only"] = bool(meta.get("ingest_only") or payload.get("ingest_only") or False)
    state["media_only"] = bool(meta.get("media_only") or payload.get("media_only") or False)

    # Allow tenant override (admin media backfill resolves tenant up-front)
    meta_tenant_id = _norm_value(str(meta.get("tenant_id") or ""))
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

    # -------------------------
    # ✅ Extra fields needed for Teams post formatting
    # -------------------------
    # Try mapping keys first; if mapping doesn't have these, fallback to raw header names.
    k_ci_created_by = None
    k_ci_item_id = None
    k_ci_insp_img = None

    try:
        k_ci_created_by = _key(sheets.map.col("checkin", "created_by"))
    except Exception:
        pass

    try:
        k_ci_item_id = _key(sheets.map.col("checkin", "id"))
    except Exception:
        pass

    try:
        k_ci_insp_img = _key(sheets.map.col("checkin", "inspection_image"))
    except Exception:
        pass

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
    refs = split_cell_refs(insp_cell) if insp_cell else []
    state["checkin_image_refs"] = refs

    urls: List[str] = []
    try:
        drive = DriveTool(settings)
        resolver = AttachmentResolver(drive)

        for ref in refs[:3]:
            att = resolver.resolve(ref)
            if not att:
                continue

            # Direct URL already
            if att.direct_url:
                u = (att.direct_url or "").strip()
                if u:
                    urls.append(u)
                continue

            # Drive file id => convert to view URL
            if att.drive_file_id:
                u = _drive_view_url(att.drive_file_id)
                if u:
                    urls.append(u)
                continue

    except Exception as e:
        (state.get("logs") or []).append(f"Inspection image resolve failed (non-fatal): {e}")

    state["checkin_image_urls"] = urls
    # -------------------------
    # ✅ Resolve tenant_id via Project sheet (ID-first, then fallback triplet)
    # -------------------------
    tenant_id = _norm_value(str(state.get("tenant_id") or ""))  # may be pre-set by meta override
    project_row = None

    if not tenant_id:
        # 1) ID-first: match Project.ID (legacy_id) (same as history_ingest approach)
        if legacy_id:
            try:
                projects = sheets.list_projects()
                k_p_legacy = _key(sheets.map.col("project", "legacy_id"))
                for pr in projects:
                    pid = _norm_value((pr or {}).get(k_p_legacy, ""))
                    if pid and _key(pid) == _key(str(legacy_id)):
                        project_row = pr
                        break
            except Exception:
                project_row = None

        # 2) Fallback: original triplet lookup
        if not project_row and project_name and part_number and legacy_id:
            project_row = sheets.get_project_row(project_name, part_number, legacy_id)

        if project_row:
            k_tenant = _key(sheets.map.col("project", "company_row_id"))
            tenant_id = _norm_value(project_row.get(k_tenant, ""))

            # Fill missing values from project row (nice-to-have)
            try:
                k_pname = _key(sheets.map.col("project", "project_name"))
                k_ppart = _key(sheets.map.col("project", "part_number"))
                if not project_name:
                    project_name = _norm_value(project_row.get(k_pname, ""))
                if not part_number:
                    part_number = _norm_value(project_row.get(k_ppart, ""))
                state["project_name"] = project_name or None
                state["part_number"] = part_number or None
            except Exception:
                pass

    state["project_row"] = project_row
    state["tenant_id"] = tenant_id or None

    convo_rows = []
    if checkin_id:
        convo_rows = sheets.get_conversations_for_checkin(str(checkin_id))
    state["conversation_rows"] = convo_rows

    # Closure memory candidates from conversation
    state["closure_notes"] = _extract_closure_notes(convo_rows)

    # ✅ Company routing (Phase 2 requirement)
    # 1) Derive from PROJECT NAME (this is your source of truth for Teams channel grouping)
    # 2) If Glide is configured + returns a real name, override (optional enhancement)
    state["company_name"] = None
    state["company_description"] = None
    state["company_key"] = None

    try:
        ct = CompanyTool(settings)

        # Fallback-from-project: always attempt
        proj_ctx = ct.from_project_name(project_name or "", tenant_row_id=tenant_id or "")
        if proj_ctx:
            state["company_name"] = proj_ctx.company_name or None
            state["company_key"] = proj_ctx.company_key or None
            state["company_description"] = None
            (state.get("logs") or []).append(
                f"Derived company from project_name: company='{proj_ctx.company_name}' key='{proj_ctx.company_key}'"
            )

        # Glide override (only if tenant_id exists and Glide returns a real company name)
        if tenant_id:
            glide_ctx = ct.get_company_context(tenant_id)
            if glide_ctx and (glide_ctx.company_name or "").strip():
                state["company_name"] = glide_ctx.company_name or state["company_name"]
                state["company_description"] = glide_ctx.company_description or None
                state["company_key"] = glide_ctx.company_key or state["company_key"]
                (state.get("logs") or []).append(
                    f"Loaded company context via Glide (override): name='{glide_ctx.company_name}' key='{glide_ctx.company_key}'"
                )

        # If we still don't have a key but we have a name, set a stable fallback
        if not state.get("company_key") and state.get("company_name"):
            # last resort: slug-like key using CompanyTool helper via from_project_name
            proj_ctx2 = ct.from_project_name(state["company_name"], tenant_row_id=tenant_id or "")
            if proj_ctx2:
                state["company_key"] = proj_ctx2.company_key or None

    except Exception as e:
        (state.get("logs") or []).append(f"Company routing build failed (non-fatal): {e}")

    # -------------------------
    # ✅ Company description fallback (DB cache -> Glide -> embed -> vectors)
    # -------------------------
    try:
        tenant_row_id = (state.get("tenant_id") or "").strip()
        cur_desc = (state.get("company_description") or "").strip()

        if tenant_row_id and not cur_desc:
            cache = CompanyCacheTool(settings)
            cached = cache.get(tenant_row_id)
            if cached and (cached.get("company_description") or "").strip():
                state["company_description"] = (cached.get("company_description") or "").strip()
                (state.get("logs") or []).append("Loaded company_description from Postgres cache")

        # Still missing? fetch from Glide once and cache it.
        cur_desc = (state.get("company_description") or "").strip()
        if tenant_row_id and not cur_desc:
            ct = CompanyTool(settings)
            glide_ctx = ct.get_company_context(tenant_row_id)
            if glide_ctx and (glide_ctx.company_description or "").strip():
                state["company_description"] = (glide_ctx.company_description or "").strip()
                state["company_name"] = glide_ctx.company_name or state.get("company_name")
                state["company_key"] = glide_ctx.company_key or state.get("company_key")

                # persist cache
                cache = CompanyCacheTool(settings)
                cache.upsert(
                    tenant_row_id=tenant_row_id,
                    company_name=(state.get("company_name") or ""),
                    company_description=(state.get("company_description") or ""),
                    raw={"source": "glide_fallback"},
                    source="glide",
                )
                (state.get("logs") or []).append("Fetched company_description from Glide and cached to Postgres")

        # If we have description now: embed + upsert company vector
        desc_final = (state.get("company_description") or "").strip()
        if tenant_row_id and desc_final:
            company_name = (state.get("company_name") or "").strip()
            embedder = EmbedTool(settings)
            vdb = VectorTool(settings)
            emb = embedder.embed_text(f"Company: {company_name}\n{desc_final}")
            vdb.upsert_company_profile(
                tenant_row_id=tenant_row_id,
                company_name=company_name,
                company_description=desc_final,
                embedding=emb,
            )
            (state.get("logs") or []).append("Upserted company profile vector (company_vectors)")
    except Exception as e:
        (state.get("logs") or []).append(f"Company description fallback/embed failed (non-fatal): {e}")

    (state.get("logs") or []).append("Loaded sheet data (checkin/project/conversation) + extracted closure notes + company routing")
    return state
