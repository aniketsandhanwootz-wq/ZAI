from __future__ import annotations

import logging
from typing import Optional
import psycopg2

from ...config import Settings

logger = logging.getLogger("zai.runlog")


class RunLog:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _conn(self):
        return psycopg2.connect(self.settings.database_url)

    def start(self, tenant_id: str, event_type: str, primary_id: str) -> str:
        sql = """
        INSERT INTO ai_runs (tenant_id, event_type, primary_id, status, started_at)
        VALUES (%s, %s, %s, 'RUNNING', now())
        RETURNING run_id;
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (tenant_id, event_type, primary_id))
                run_id = cur.fetchone()[0]
        return str(run_id)

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
