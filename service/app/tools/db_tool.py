from __future__ import annotations

from typing import Any, Dict, Set
import json
import psycopg2


class DBTool:
    def __init__(self, database_url: str):
        self.database_url = database_url

    def _conn(self):
        conn = psycopg2.connect(self.database_url)
        conn.autocommit = True
        return conn

    def existing_artifact_source_hashes(
        self,
        *,
        tenant_id: str,
        checkin_id: str,
        artifact_type: str,
    ) -> Set[str]:
        """
        Returns a set of source_hash strings already stored for this tenant+checkin+type.
        Uses meta JSON so we don't need a join.
        """
        q = """
        SELECT COALESCE(meta->>'source_hash','') AS source_hash
        FROM artifacts
        WHERE artifact_type = %s
          AND COALESCE(meta->>'tenant_id','') = %s
          AND COALESCE(meta->>'checkin_id','') = %s
          AND COALESCE(meta->>'source_hash','') <> ''
        """
        out: Set[str] = set()
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (artifact_type, tenant_id, checkin_id))
                for (h,) in cur.fetchall() or []:
                    if h:
                        out.add(str(h))
        return out

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

    def insert_artifact_no_fail(
        self,
        *,
        run_id: str,
        artifact_type: str,
        url: str,
        meta: Dict[str, Any],
    ) -> bool:
        """
        Same as insert_artifact but NEVER raises.
        Returns True if insert succeeded else False.
        """
        try:
            self.insert_artifact(run_id=run_id, artifact_type=artifact_type, url=url, meta=meta)
            return True
        except Exception:
            return False

    def get_artifact_url_by_source_hash(
        self,
        *,
        tenant_id: str,
        checkin_id: str,
        artifact_type: str,
        source_hash: str,
    ) -> str:
        """
        Returns latest url for given (type, tenant, checkin, source_hash) or "".
        Useful to skip re-upload if we already uploaded before.
        """
        q = """
        SELECT COALESCE(url,'') AS url
        FROM artifacts
        WHERE artifact_type = %s
          AND COALESCE(meta->>'tenant_id','') = %s
          AND COALESCE(meta->>'checkin_id','') = %s
          AND COALESCE(meta->>'source_hash','') = %s
        ORDER BY created_at DESC
        LIMIT 1
        """
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(q, (artifact_type, tenant_id, checkin_id, source_hash))
                    row = cur.fetchone()
                    return (row[0] if row else "") or ""
        except Exception:
            return ""
