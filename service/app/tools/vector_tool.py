from __future__ import annotations

from typing import Any, Dict, List, Optional
import hashlib
import psycopg2
from psycopg2.extras import RealDictCursor

from ..config import Settings


def _vec_literal(v: List[float]) -> str:
    return "[" + ",".join(f"{x:.8f}" for x in v) + "]"


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class VectorTool:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _conn(self):
        return psycopg2.connect(self.settings.database_url)

    # ---------- INCIDENT ----------
    def upsert_incident_vector(
        self,
        tenant_id: str,
        checkin_id: str,
        vector_type: str,
        embedding: List[float],
        project_name: Optional[str],
        part_number: Optional[str],
        legacy_id: Optional[str],
        status: Optional[str],
        text: str,
    ) -> None:
        sql = """
        INSERT INTO incident_vectors
          (tenant_id, checkin_id, vector_type, embedding, project_name, part_number, legacy_id, status, summary_text)
        VALUES
          (%s, %s, %s, (%s)::vector, %s, %s, %s, %s, %s)
        ON CONFLICT (tenant_id, checkin_id, vector_type)
        DO UPDATE SET
          embedding = EXCLUDED.embedding,
          project_name = EXCLUDED.project_name,
          part_number = EXCLUDED.part_number,
          legacy_id = EXCLUDED.legacy_id,
          status = EXCLUDED.status,
          summary_text = EXCLUDED.summary_text,
          updated_at = now();
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        tenant_id,
                        checkin_id,
                        vector_type,
                        _vec_literal(embedding),
                        project_name,
                        part_number,
                        legacy_id,
                        status,
                        text,
                    ),
                )

    def search_incidents(
        self,
        tenant_id: str,
        query_embedding: List[float],
        top_k: int = 5,
        project_name: Optional[str] = None,
        part_number: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
        SELECT
          checkin_id,
          project_name,
          part_number,
          legacy_id,
          status,
          summary_text,
          (embedding <=> (%s)::vector) AS distance
        FROM incident_vectors
        WHERE tenant_id = %s
        """
        params: List[Any] = [_vec_literal(query_embedding), tenant_id]

        if project_name:
            sql += " AND project_name = %s"
            params.append(project_name)
        if part_number:
            sql += " AND part_number = %s"
            params.append(part_number)

        sql += " ORDER BY distance ASC LIMIT %s"
        params.append(top_k)

        with self._conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

        return [
            {
                "checkin_id": r["checkin_id"],
                "summary": r["summary_text"],
                "status": r["status"],
                "project_name": r["project_name"],
                "part_number": r["part_number"],
                "distance": float(r["distance"]),
            }
            for r in rows
        ]

    # ---------- CCP ----------
    def upsert_ccp_chunk(
        self,
        tenant_id: str,
        ccp_id: str,
        ccp_name: Optional[str],
        project_name: Optional[str],
        part_number: Optional[str],
        legacy_id: Optional[str],
        chunk_type: str,
        chunk_text: str,
        source_ref: Optional[str],
        embedding: List[float],
    ) -> None:
        content_hash = _sha256(f"{ccp_id}|{chunk_type}|{chunk_text}")

        sql = """
        INSERT INTO ccp_vectors
          (tenant_id, ccp_id, ccp_name, project_name, part_number, legacy_id,
           chunk_type, chunk_text, source_ref, embedding, content_hash)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s,%s,%s,(%s)::vector,%s)
        ON CONFLICT (tenant_id, ccp_id, chunk_type, content_hash)
        DO UPDATE SET
          ccp_name = EXCLUDED.ccp_name,
          project_name = EXCLUDED.project_name,
          part_number = EXCLUDED.part_number,
          legacy_id = EXCLUDED.legacy_id,
          source_ref = EXCLUDED.source_ref,
          embedding = EXCLUDED.embedding,
          updated_at = now();
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        tenant_id,
                        ccp_id,
                        ccp_name,
                        project_name,
                        part_number,
                        legacy_id,
                        chunk_type,
                        chunk_text,
                        source_ref,
                        _vec_literal(embedding),
                        content_hash,
                    ),
                )

    def search_ccp_chunks(
        self,
        tenant_id: str,
        query_embedding: List[float],
        top_k: int = 5,
        project_name: Optional[str] = None,
        part_number: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
        SELECT
          ccp_id,
          ccp_name,
          chunk_text,
          (embedding <=> (%s)::vector) AS distance
        FROM ccp_vectors
        WHERE tenant_id = %s
        """
        params: List[Any] = [_vec_literal(query_embedding), tenant_id]

        if project_name:
            sql += " AND project_name = %s"
            params.append(project_name)
        if part_number:
            sql += " AND part_number = %s"
            params.append(part_number)

        sql += " ORDER BY distance ASC LIMIT %s"
        params.append(top_k)

        with self._conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

        return [
            {
                "ccp_id": r["ccp_id"],
                "ccp_name": r["ccp_name"],
                "text": r["chunk_text"],
                "distance": float(r["distance"]),
            }
            for r in rows
        ]

    def _assert_dims(self, embedding: List[float]) -> None:
        expected = int(getattr(self.settings, "embedding_dims", 0) or 0)
        if expected and len(embedding) != expected:
            raise RuntimeError(f"Embedding dims mismatch: expected {expected}, got {len(embedding)}")
