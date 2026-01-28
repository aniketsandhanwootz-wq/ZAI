# service/app/pipeline/ingest/glide_ingest_company.py
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from ...config import Settings
from ...integrations.glide_client import GlideClient
from ...tools.company_cache_tool import CompanyCacheTool
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool

logger = logging.getLogger("zai.glide_ingest_company")


def _s(x: Any) -> str:
    return str(x or "").strip()


def upsert_glide_company_row(settings: Settings, *, row_id: str) -> Dict[str, Any]:
    """
    Incremental upsert for ONE company row from Glide -> company_profiles + company_vectors.
    Called by worker on webhook.
    """
    glide = GlideClient(settings)
    if not glide.enabled():
        return {"ok": True, "skipped": True, "reason": "Glide not configured"}

    table = (settings.glide_company_table or "").strip()
    if not table:
        return {"ok": True, "skipped": True, "reason": "GLIDE_COMPANY_TABLE not set"}

    rowid_col = (settings.glide_company_rowid_column or "$rowID").strip()
    name_col = (settings.glide_company_name_column or "Name").strip()
    desc_col = (settings.glide_company_desc_column or "Short client description").strip()

    rid = _s(row_id)
    if not rid:
        return {"ok": False, "error": "Missing row_id"}

    row: Optional[Dict[str, Any]] = glide.get_row_by_row_id(
        table_name=table,
        row_id=rid,
        rowid_column=rowid_col,
        timeout=30,
    )
    if not row:
        # Treat as ok: row may have been deleted or row_id wrong; don’t crash worker
        return {"ok": True, "not_found": True, "row_id": rid}

    tenant_row_id = _s(row.get(rowid_col))
    if not tenant_row_id:
        # fallback to provided row_id
        tenant_row_id = rid

    company_name = _s(row.get(name_col))
    company_desc = _s(row.get(desc_col))

    cache = CompanyCacheTool(settings)
    cache.upsert(
        tenant_row_id=tenant_row_id,
        company_name=company_name,
        company_description=company_desc,
        raw=row,
        source="glide",
    )

    vec_written = False
    if company_desc:
        embedder = EmbedTool(settings)
        vdb = VectorTool(settings)

        emb = embedder.embed_text(f"Company: {company_name}\n{company_desc}")
        vdb.upsert_company_profile(
            tenant_row_id=tenant_row_id,
            company_name=company_name,
            company_description=company_desc,
            embedding=emb,
        )
        vec_written = True

    return {
        "ok": True,
        "row_id": tenant_row_id,
        "company_name": company_name,
        "has_description": bool(company_desc),
        "vector_upserted": vec_written,
    }


def ingest_glide_company(settings: Settings, *, limit: int = 0) -> Dict[str, Any]:
    """
    Optional bulk backfill for company table (handy for admin ingest).
    NOT required for Phase 3, but useful to validate quickly.
    """
    glide = GlideClient(settings)
    if not glide.enabled():
        return {"ok": True, "skipped": True, "reason": "Glide not configured"}

    table = (settings.glide_company_table or "").strip()
    if not table:
        return {"ok": True, "skipped": True, "reason": "GLIDE_COMPANY_TABLE not set"}

    rows = glide.list_company_rows()
    if limit and limit > 0:
        rows = rows[: int(limit)]

    rowid_col = (settings.glide_company_rowid_column or "$rowID").strip()
    seen = 0
    ok = 0
    not_found = 0
    err = 0

    for r in rows:
        seen += 1
        rid = _s(r.get(rowid_col))
        if not rid:
            continue
        try:
            out = upsert_glide_company_row(settings, row_id=rid)
            if out.get("not_found"):
                not_found += 1
            elif out.get("ok"):
                ok += 1
            else:
                err += 1
        except Exception as e:
            err += 1
            logger.exception("company ingest failed row_id=%s err=%s", rid, e)

    return {
        "ok": err == 0,
        "rows_seen": seen,
        "rows_ok": ok,
        "rows_not_found": not_found,
        "rows_error": err,
    }