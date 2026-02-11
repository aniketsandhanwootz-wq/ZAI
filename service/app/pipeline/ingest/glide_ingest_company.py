# service/app/pipeline/ingest/glide_ingest_company.py
from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Callable, List

from ...config import Settings
from ...integrations.glide_client import GlideClient
from ...tools.company_cache_tool import CompanyCacheTool
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool

logger = logging.getLogger("zai.glide_ingest_company")


def _s(x: Any) -> str:
    return str(x or "").strip()


def _embed_text(settings: Settings, text: str) -> List[float]:
    """
    Adapter so we don't care what EmbedTool's method is named.
    Tries common method names.
    """
    emb = EmbedTool(settings)

    # Most common patterns
    if hasattr(emb, "embed_text") and callable(getattr(emb, "embed_text")):
        return emb.embed_text(text)  # type: ignore[attr-defined]
    if hasattr(emb, "embed") and callable(getattr(emb, "embed")):
        return emb.embed(text)  # type: ignore[attr-defined]
    if hasattr(emb, "embed_query") and callable(getattr(emb, "embed_query")):
        return emb.embed_query(text)  # type: ignore[attr-defined]

    raise RuntimeError("EmbedTool has no known embed method (expected embed_text/embed/embed_query).")


def upsert_glide_company_row_dict(settings: Settings, *, row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Upsert company from an already-fetched Glide row dict (NO Glide API calls).
    Used by glide_reconcile full-table runs.

    Writes:
      - company_profiles (CompanyCacheTool.upsert)
      - company_vectors (VectorTool.upsert_company_profile) if description exists

    Returns:
      {"ok": True, "tenant_row_id": "...", ...} or {"ok": False, "error": "..."}
    """
    rowid_col = (settings.glide_company_rowid_column or "$rowID").strip()
    name_col = (settings.glide_company_name_column or "Name").strip()
    desc_col = (settings.glide_company_desc_column or "Short client description").strip()

    tenant_row_id = _s((row or {}).get(rowid_col))
    if not tenant_row_id:
        return {"ok": False, "error": f"Missing company row id column '{rowid_col}'", "skipped": True}

    company_name = _s((row or {}).get(name_col))
    company_desc = _s((row or {}).get(desc_col))

    # 1) Upsert profile
    cache = CompanyCacheTool(settings)
    cache.upsert(
        tenant_row_id=tenant_row_id,
        company_name=company_name,
        company_description=company_desc,
        raw=row or {},
        source="glide",
    )

    # 2) Upsert vector (only if we have some description)
    # VectorTool already no-ops if description empty, but keep explicit for clarity.
    if company_desc.strip():
        vt = VectorTool(settings)
        # include name in embedding input so matching works even if desc is short
        text_for_embed = f"{company_name}\n{company_desc}".strip()
        vec = _embed_text(settings, text_for_embed)
        vt.upsert_company_profile(
            tenant_row_id=tenant_row_id,
            company_name=company_name,
            company_description=company_desc,
            embedding=vec,
        )

    return {
        "ok": True,
        "tenant_row_id": tenant_row_id,
        "company_name": company_name,
        "has_description": bool(company_desc.strip()),
    }


def upsert_glide_company_row(settings: Settings, *, row_id: str) -> Dict[str, Any]:
    """
    Incremental upsert for ONE company row from Glide -> company_profiles + company_vectors.
    Called by worker on webhook.
    (This is allowed to do 1 Glide call.)
    """
    glide = GlideClient(settings)
    if not glide.enabled():
        return {"ok": True, "skipped": True, "reason": "Glide not configured"}

    table = (settings.glide_company_table or "").strip()
    if not table:
        return {"ok": True, "skipped": True, "reason": "GLIDE_COMPANY_TABLE not set"}

    rowid_col = (settings.glide_company_rowid_column or "$rowID").strip()

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

    # IMPORTANT: NO recursion now — this calls the real row-dict implementation
    return upsert_glide_company_row_dict(settings, row=row)


def ingest_glide_company(settings: Settings, *, limit: int = 0) -> Dict[str, Any]:
    """
    Bulk ingest for company table.
    IMPORTANT: does NOT re-fetch per-row; uses list results directly.
    """
    glide = GlideClient(settings)
    if not glide.enabled():
        return {"ok": True, "skipped": True, "reason": "Glide not configured"}

    table = (settings.glide_company_table or "").strip()
    if not table:
        return {"ok": True, "skipped": True, "reason": "GLIDE_COMPANY_TABLE not set"}

    rows = glide.list_table_rows(table)
    if limit and limit > 0:
        rows = rows[: int(limit)]

    rowid_col = (settings.glide_company_rowid_column or "$rowID").strip()

    seen = 0
    ok = 0
    skipped = 0
    err = 0

    for r in rows:
        seen += 1
        rid = _s((r or {}).get(rowid_col))
        if not rid:
            skipped += 1
            continue
        try:
            out = upsert_glide_company_row_dict(settings, row=r)
            if out.get("ok"):
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
        "rows_skipped": skipped,
        "rows_error": err,
    }