from __future__ import annotations

import logging
from pathlib import Path

import psycopg2

from ...config import Settings

logger = logging.getLogger("zai.migrate")


def run_migrations(settings: Settings) -> None:
    """
    Runs SQL files in packages/db/migrations in order.
    Safe to call multiple times because files use IF NOT EXISTS.
    """
    repo_root = Path(__file__).resolve().parents[4]  # service/app/pipeline/ingest -> repo root
    mig_dir = repo_root / "packages" / "db" / "migrations"

    files = [
        "001_extensions.sql",
        "002_core_tables.sql",
        "003_indexes.sql",
        "004_fix_ai_runs_idempotency.sql",
        "005_incident_vector_type_index.sql",
        "006_artifacts_lookup_indexes.sql",
        "007_company_profiles.sql",
        "008_glide_kb.sql",        
    ]
    logger.info("running migrations from %s", mig_dir)

    with psycopg2.connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            for fn in files:
                p = mig_dir / fn
                sql = p.read_text(encoding="utf-8")
                logger.info("applying %s", fn)
                cur.execute(sql)
        conn.commit()

    # ivfflat requires analyze for best results
    try:
        with psycopg2.connect(settings.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("ANALYZE incident_vectors;")
                cur.execute("ANALYZE ccp_vectors;")
                cur.execute("ANALYZE dashboard_vectors;")
                cur.execute("ANALYZE company_vectors;")
                cur.execute("ANALYZE glide_kb_items;")
                cur.execute("ANALYZE glide_kb_vectors;")                
            conn.commit()
    except Exception:
        # don't fail boot
        logger.warning("ANALYZE failed (non-fatal).")
