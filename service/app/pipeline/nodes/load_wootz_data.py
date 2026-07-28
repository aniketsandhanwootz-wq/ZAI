# service/app/pipeline/nodes/load_wootz_data.py
"""
wootzcheckin (AWS) counterpart to load_sheet_data.py — populates the exact
same state contract, sourced from wootzcheckin's own REST API
(WootzCheckinClient.get_checkin_context) instead of Google Sheets.

Downstream nodes (build_thread_snapshot, analyze_media, analyze_attachments)
were built against SheetsTool's row-dict shape: keys are the CASEFOLD-
normalized sheet column header (see SheetsTool._row_to_dict's docstring —
e.g. header "Inspection Image URL" -> key "inspection image url"). Rather
than forking those nodes, this loader builds `checkin_row`/`conversation_rows`
dicts keyed the same way (via the same sheets_mapping.yaml column labels),
so they keep working completely unchanged for wootzcheckin-sourced events —
AttachmentResolver already treats a plain https:// URL (wootzcheckin's S3/
CloudFront URLs) as a generic direct-download attachment; nothing Drive-
specific is required on this path.

No Drive/Sheets network calls happen here: WootzCheckinClient talks to
wootzcheckin's own service-secret-gated /internal/* API, and
load_sheet_mapping() just reads the local sheets_mapping.yaml for column
label strings (no Google auth).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ...config import Settings
from ...tools.wootzcheckin_client import WootzCheckinClient
from ...tools.mapping_tool import load_sheet_mapping
from ...tools.sheets_tool import _key, _norm_value
from ...tools.company_tool import CompanyTool
from ...tools.company_cache_tool import CompanyCacheTool
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool


def _media_cell(urls: Optional[List[str]]) -> str:
    """Join multiple URLs the same way a multi-value sheet cell would be
    stored, so split_cell_refs() (newline/comma-aware) parses them back out."""
    return "\n".join([u.strip() for u in (urls or []) if (u or "").strip()])


def load_wootz_data(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    payload = state.get("payload") or {}
    meta = payload.get("meta") or {}
    state["meta"] = meta
    state["ingest_only"] = bool(meta.get("ingest_only") or payload.get("ingest_only") or False)
    state["media_only"] = bool(meta.get("media_only") or payload.get("media_only") or False)

    checkin_id = str(payload.get("checkin_id") or state.get("checkin_id") or "").strip()
    conversation_id = payload.get("conversation_id")
    state["checkin_id"] = checkin_id
    state["conversation_id"] = conversation_id
    state["ccp_id"] = None
    state["legacy_id"] = None
    state["dashboard_update_id"] = None
    state["dashboard_update_row"] = None
    state["dashboard_update_message"] = None
    state["event_type"] = payload.get("event_type", "")

    if not checkin_id:
        (state.get("logs") or []).append("load_wootz_data: missing checkin_id")
        return state

    client = WootzCheckinClient(settings)
    ctx = client.get_checkin_context(checkin_id)

    checkin = ctx.get("checkin") or {}
    project = ctx.get("project") or {}
    conversations = ctx.get("conversations") or []

    tenant_id = (project.get("companyRowId") or "").strip()
    legacy_id = (project.get("legacyId") or "").strip()
    project_name = project.get("projectName") or ""
    part_number = project.get("partNumber") or ""

    state["tenant_id"] = tenant_id or None
    state["legacy_id"] = legacy_id or None
    state["project_name"] = project_name or None
    state["part_number"] = part_number or None
    state["dispatch_date"] = project.get("dispatchDate") or None
    state["checkin_status"] = checkin.get("status") or ""
    state["checkin_description"] = checkin.get("description") or ""
    state["checkin_created_by"] = checkin.get("createdBy") or None
    state["checkin_item_id"] = legacy_id or None
    state["created_by_phone"] = None
    state["internal_poc_phones"] = []
    state["project_row"] = None

    image_urls = checkin.get("image") or []
    state["checkin_image_refs"] = list(image_urls)
    state["checkin_image_urls"] = list(image_urls)

    m = load_sheet_mapping()
    k_ci_insp_img = _key(m.col("checkin", "inspection_image_url"))
    k_ci_files = _key(m.col("checkin", "files"))
    k_ci_status = _key(m.col("checkin", "status"))
    k_ci_desc = _key(m.col("checkin", "description"))
    k_ci_project = _key(m.col("checkin", "project_name"))
    k_ci_part = _key(m.col("checkin", "part_number"))
    k_ci_legacy = _key(m.col("checkin", "legacy_id"))

    state["checkin_row"] = {
        k_ci_insp_img: _media_cell(image_urls),
        k_ci_files: _media_cell(checkin.get("files") or []),
        k_ci_status: _norm_value(checkin.get("status") or ""),
        k_ci_desc: _norm_value(checkin.get("description") or ""),
        k_ci_project: _norm_value(project_name),
        k_ci_part: _norm_value(part_number),
        k_ci_legacy: _norm_value(legacy_id),
    }

    k_cv_photo = _key(m.col("conversation", "photos"))
    k_cv_remark = _key(m.col("conversation", "remark"))
    k_cv_status = _key(m.col("conversation", "status"))
    k_cv_id = _key(m.col("conversation", "conversation_id"))
    k_cv_checkin_id = _key(m.col("conversation", "checkin_id"))

    conversation_rows: List[Dict[str, Any]] = []
    for cv in conversations:
        conversation_rows.append({
            k_cv_id: cv.get("conversationId") or "",
            k_cv_checkin_id: checkin_id,
            k_cv_photo: _media_cell(cv.get("photo") or []),
            # analyze_attachments doesn't read conversation "files" today
            # (mirrors load_sheet_data.py's own scope — Files is checkin-level
            # there too), so conversation.files isn't wired into a cell here.
            k_cv_remark: _norm_value(cv.get("message") or ""),
            "remarks": _norm_value(cv.get("message") or ""),
            k_cv_status: _norm_value(cv.get("status") or ""),
            # build_thread_snapshot.py reads plain "remarks"/"status" keys
            # directly (not through sheets.map) — provide both forms.
            "status": _norm_value(cv.get("status") or ""),
        })
    state["conversation_rows"] = conversation_rows
    state["closure_notes"] = ""  # recomputed by build_thread_snapshot.py

    # Company routing — unchanged from load_sheet_data.py: Glide-sourced,
    # keyed on tenant_id/project_name, no Sheets/checkin-row dependency.
    state["company_name"] = None
    state["company_description"] = None
    state["company_key"] = None
    try:
        ct = CompanyTool(settings)
        proj_ctx = ct.from_project_name(project_name or "", tenant_row_id=tenant_id or "")
        if proj_ctx:
            state["company_name"] = proj_ctx.company_name or None
            state["company_key"] = proj_ctx.company_key or None

        if tenant_id:
            glide_ctx = ct.get_company_context(tenant_id)
            if glide_ctx and (glide_ctx.company_name or "").strip():
                state["company_name"] = glide_ctx.company_name or state["company_name"]
                state["company_description"] = glide_ctx.company_description or None
                state["company_key"] = glide_ctx.company_key or state["company_key"]
    except Exception as e:
        (state.get("logs") or []).append(f"load_wootz_data: company routing failed (non-fatal): {e}")

    try:
        tenant_row_id = (state.get("tenant_id") or "").strip()
        cur_desc = (state.get("company_description") or "").strip()
        if tenant_row_id and not cur_desc:
            cache = CompanyCacheTool(settings)
            cached = cache.get(tenant_row_id)
            if cached and (cached.get("company_description") or "").strip():
                state["company_description"] = (cached.get("company_description") or "").strip()

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
    except Exception as e:
        (state.get("logs") or []).append(f"load_wootz_data: company description cache/embed failed (non-fatal): {e}")

    (state.get("logs") or []).append(
        f"load_wootz_data: loaded checkin={checkin_id} tenant={tenant_id} conversations={len(conversation_rows)}"
    )
    return state
