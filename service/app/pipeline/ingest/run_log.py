from __future__ import annotations

import logging
import psycopg2
from psycopg2 import errors

from ...config import Settings

logger = logging.getLogger("zai.runlog")


class RunLog:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _conn(self):
        return psycopg2.connect(self.settings.database_url)

    def start(self, tenant_id: str, event_type: str, primary_id: str) -> str:
        """
        Idempotent start:
        - First try to insert a RUNNING row.
        - If unique violation, fetch existing run_id and return it (do NOT crash the job).
        """
        insert_sql = """
        INSERT INTO ai_runs (tenant_id, event_type, primary_id, status, started_at)
        VALUES (%s, %s, %s, 'RUNNING', now())
        RETURNING run_id;
        """

        select_sql = """
        SELECT run_id, status
        FROM ai_runs
        WHERE tenant_id=%s AND event_type=%s AND primary_id=%s
        ORDER BY started_at DESC
        LIMIT 1;
        """

        with self._conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(insert_sql, (tenant_id, event_type, primary_id))
                    run_id = cur.fetchone()[0]
                    return str(run_id)

                except errors.UniqueViolation:
                    conn.rollback()
                    cur.execute(select_sql, (tenant_id, event_type, primary_id))
                    row = cur.fetchone()
                    if row:
                        run_id, status = row
                        logger.info(
                            "Idempotency hit: tenant_id=%s event_type=%s primary_id=%s -> existing run_id=%s status=%s",
                            tenant_id, event_type, primary_id, run_id, status
                        )
                        return str(run_id)

                    # fallback: re-raise if somehow not found
                    raise

    def success(self, run_id: str) -> None:
        sql = "UPDATE ai_runs SET status='SUCCESS', finished_at=now() WHERE run_id=%s;"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))

    def error(self, run_id: str, message: str) -> None:
        sql = "UPDATE ai_runs SET status='ERROR', error_message=%s, finished_at=now() WHERE run_id=%s;"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (message[:2000], run_id))

    def update_tenant(self, run_id: str, tenant_id: str) -> None:
        sql = "UPDATE ai_runs SET tenant_id=%s WHERE run_id=%s;"
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, run_id))
