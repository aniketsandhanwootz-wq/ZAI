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

    def _assert_dims(self, embedding: List[float]) -> None:
        expected = int(getattr(self.settings, "embedding_dims", 0) or 0)
        if expected and len(embedding) != expected:
            raise RuntimeError(
                f"Embedding dims mismatch: expected {expected}, got {len(embedding)}. "
                f"Fix EMBEDDING_MODEL/EMBEDDING_DIMS or DB vector dims."
            )

    # ---------- INCIDENT UPSERT ----------
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
        self._assert_dims(embedding)

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

    # ---------- INCIDENT SEARCH (PROBLEM / RESOLUTION) ----------
    def search_incidents(
        self,
        tenant_id: str,
        query_embedding: List[float],
        top_k: int = 10,
        project_name: Optional[str] = None,
        part_number: Optional[str] = None,
        vector_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        self._assert_dims(query_embedding)

        sql = """
        SELECT
          checkin_id,
          vector_type,
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

        if vector_type:
            sql += " AND vector_type = %s"
            params.append(vector_type)

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
                # Improve recall for ivfflat
                cur.execute("SET ivfflat.probes = 10;")
                cur.execute(sql, params)
                rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "checkin_id": r["checkin_id"],
                    "vector_type": r["vector_type"],
                    "summary": r["summary_text"],
                    "status": r["status"],
                    "project_name": r["project_name"],
                    "part_number": r["part_number"],
                    "legacy_id": r["legacy_id"],
                    "distance": float(r["distance"]),
                }
            )
        return out

    # ---------- CCP UPSERT ----------
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
        self._assert_dims(embedding)
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
    # ---------- DASHBOARD UPSERT ----------
    def upsert_dashboard_update(
        self,
        tenant_id: str,
        project_name: Optional[str],
        part_number: Optional[str],
        legacy_id: Optional[str],
        update_message: str,
        embedding: List[float],
    ) -> None:
        self._assert_dims(embedding)
        msg = (update_message or "").strip()
        if not msg:
            return

        # content_hash changes if message changes => incremental ingestion
        content_hash = _sha256(f"{legacy_id}|{project_name}|{part_number}|{msg}")

        sql = """
        INSERT INTO dashboard_vectors
          (tenant_id, project_name, part_number, legacy_id, update_message, embedding, content_hash)
        VALUES
          (%s, %s, %s, %s, %s, (%s)::vector, %s)
        ON CONFLICT (tenant_id, content_hash)
        DO NOTHING;
        """

        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        tenant_id,
                        project_name,
                        part_number,
                        legacy_id,
                        msg,
                        _vec_literal(embedding),
                        content_hash,
                    ),
                )
    # ---------- CCP SEARCH ----------
    def search_ccp_chunks(
        self,
        tenant_id: str,
        query_embedding: List[float],
        top_k: int = 10,
        project_name: Optional[str] = None,
        part_number: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        self._assert_dims(query_embedding)

        sql = """
        SELECT
          ccp_id,
          ccp_name,
          chunk_text,
          source_ref,
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
                cur.execute("SET ivfflat.probes = 10;")
                cur.execute(sql, params)
                rows = cur.fetchall()

        return [
            {
                "ccp_id": r["ccp_id"],
                "ccp_name": r["ccp_name"],
                "text": r["chunk_text"],
                "source_ref": r["source_ref"],
                "distance": float(r["distance"]),
            }
            for r in rows
        ]

    # ---------- DASHBOARD SEARCH ----------
    def search_dashboard_updates(
        self,
        tenant_id: str,
        query_embedding: List[float],
        top_k: int = 8,
        project_name: Optional[str] = None,
        part_number: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        self._assert_dims(query_embedding)

        sql = """
        SELECT
          project_name,
          part_number,
          legacy_id,
          update_message,
          (embedding <=> (%s)::vector) AS distance
        FROM dashboard_vectors
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
                cur.execute("SET ivfflat.probes = 10;")
                cur.execute(sql, params)
                rows = cur.fetchall()

        return [
            {
                "project_name": r["project_name"],
                "part_number": r["part_number"],
                "legacy_id": r["legacy_id"],
                "update_message": r["update_message"],
                "distance": float(r["distance"]),
            }
            for r in rows
        ]
