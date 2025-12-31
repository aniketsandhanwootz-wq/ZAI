from __future__ import annotations

import logging

from ...config import load_settings
from ...integrations.glide_client import GlideClient
from ...tools.company_cache_tool import CompanyCacheTool
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool
from .migrate import run_migrations

logger = logging.getLogger("zai.company_ingest")


def main() -> None:
    settings = load_settings()
    run_migrations(settings)

    glide = GlideClient(settings)
    if not glide.enabled():
        raise RuntimeError("Glide not configured (GLIDE_* env vars missing)")

    rows = glide.list_company_rows()
    logger.info("Fetched %d company rows from Glide", len(rows))

    cache = CompanyCacheTool(settings)
    embedder = EmbedTool(settings)
    vdb = VectorTool(settings)

    up_count = 0
    vec_count = 0

    # Column names come from env mapping
    name_col = settings.glide_company_name_column or "Name"
    desc_col = settings.glide_company_desc_column or "nszR1"
    rowid_col = settings.glide_company_rowid_column or "$rowID"

    for r in rows:
        tenant_row_id = (str(r.get(rowid_col, "") or "")).strip()
        if not tenant_row_id:
            continue

        company_name = (str(r.get(name_col, "") or "")).strip()
        company_desc = (str(r.get(desc_col, "") or "")).strip()

        cache.upsert(
            tenant_row_id=tenant_row_id,
            company_name=company_name,
            company_description=company_desc,
            raw=r,
            source="glide",
        )
        up_count += 1

        if company_desc:
            emb = embedder.embed_text(f"Company: {company_name}\n{company_desc}")
            vdb.upsert_company_profile(
                tenant_row_id=tenant_row_id,
                company_name=company_name,
                company_description=company_desc,
                embedding=emb,
            )
            vec_count += 1

    logger.info("Upserted %d company_profiles rows", up_count)
    logger.info("Upserted %d company_vectors rows (non-empty desc)", vec_count)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
