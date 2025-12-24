from __future__ import annotations

from typing import Any, Dict, Optional
import json
import psycopg2


class DBTool:
    def __init__(self, database_url: str):
        self.database_url = database_url

    def _conn(self):
        conn = psycopg2.connect(self.database_url)
        conn.autocommit = True
        return conn

    def artifact_exists(
        self,
        *,
        tenant_id: str,
        checkin_id: str,
        artifact_type: str,
        source_hash: str,
    ) -> bool:
        q = """
        SELECT 1
        FROM artifacts a
        JOIN ai_runs r ON r.run_id = a.run_id
        WHERE r.tenant_id = %s
          AND a.artifact_type = %s
          AND COALESCE(a.meta->>'checkin_id','') = %s
          AND COALESCE(a.meta->>'source_hash','') = %s
        LIMIT 1
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (tenant_id, artifact_type, checkin_id, source_hash))
                return cur.fetchone() is not None

    def insert_artifact(
        self,
        *,
        run_id: str,
        artifact_type: str,
        url: str,
        meta: Dict[str, Any],
    ) -> None:
        q = """
        INSERT INTO artifacts (run_id, artifact_type, url, meta)
        VALUES (%s, %s, %s, %s::jsonb)
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (run_id, artifact_type, url, json.dumps(meta or {})))