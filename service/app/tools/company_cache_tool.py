from __future__ import annotations

from typing import Any, Dict, Optional
import json
import psycopg2

from ..config import Settings


class CompanyCacheTool:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _conn(self):
        return psycopg2.connect(self.settings.database_url)

    def get(self, tenant_row_id: str) -> Optional[Dict[str, Any]]:
        tenant_row_id = (tenant_row_id or "").strip()
        if not tenant_row_id:
            return None

        sql = """
        SELECT tenant_row_id, company_name, company_description, source, raw, updated_at
        FROM company_profiles
        WHERE tenant_row_id=%s
        LIMIT 1;
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (tenant_row_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "tenant_row_id": row[0],
                "company_name": row[1] or "",
                "company_description": row[2] or "",
                "source": row[3] or "",
                "raw": row[4] or {},
                "updated_at": row[5],
            }

    def upsert(
        self,
        *,
        tenant_row_id: str,
        company_name: str,
        company_description: str,
        raw: Dict[str, Any] | None = None,
        source: str = "glide",
    ) -> None:
        tenant_row_id = (tenant_row_id or "").strip()
        if not tenant_row_id:
            return

        sql = """
        INSERT INTO company_profiles (
          tenant_row_id, company_name, company_description, source, raw, updated_at
        )
        VALUES (%s,%s,%s,%s,%s::jsonb, now())
        ON CONFLICT (tenant_row_id)
        DO UPDATE SET
          company_name=EXCLUDED.company_name,
          company_description=EXCLUDED.company_description,
          source=EXCLUDED.source,
          raw=EXCLUDED.raw,
          updated_at=now();
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    tenant_row_id,
                    (company_name or "").strip() or None,
                    (company_description or "").strip() or None,
                    (source or "glide").strip(),
                    json.dumps(raw or {}),
                ),
            )
