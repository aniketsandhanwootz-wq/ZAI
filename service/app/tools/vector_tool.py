from __future__ import annotations

from typing import Any, Dict, List, Optional
import hashlib
import psycopg2
import psycopg2.extras

from ..config import Settings


def _vec_str(v: List[float]) -> str:
    # pgvector literal
    return "[" + ",".join(f"{float(x):.8f}" for x in v) + "]"


def _sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b or b"").hexdigest()


def _norm_text_for_hash(s: str) -> str:
    # makes pdf extraction + captions more stable across whitespace differences
    s = (s or "").replace("\r", "\n")
    s = "\n".join([ln.strip() for ln in s.split("\n") if ln.strip()])
    return s.strip()


class VectorTool:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _conn(self):
        return psycopg2.connect(self.settings.database_url)

    # ---------------------------
    # Hash helpers (stable/idempotent)
    # ---------------------------

    def hash_text(self, s: str) -> str:
        return _sha256_text(_norm_text_for_hash(s))

    def hash_bytes(self, b: bytes) -> str:
        return _sha256_bytes(b)

    def make_ccp_content_hash(
        self,
        *,
        ccp_id: str,
        chunk_type: str,
        stable_key: str,
        chunk_text: str = "",
    ) -> str:
        """
        stable_key examples:
          - "DESC" for description chunks (hash is driven by chunk_text anyway)
          - file_hash for PDFs/images so the same file maps to same hash family
        """
        base = f"{ccp_id}|{chunk_type}|{stable_key}|{_norm_text_for_hash(chunk_text)}"
        return _sha256_text(base)

    # ---------------------------
    # Existence checks (incremental)
    # ---------------------------

    def glide_kb_vector_hash_exists(self, *, tenant_id: str, item_id: str, content_hash: str) -> bool:
        sql = """
        SELECT 1
        FROM glide_kb_vectors
        WHERE tenant_id=%s AND item_id=%s AND content_hash=%s
        LIMIT 1
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (tenant_id, item_id, content_hash))
            return cur.fetchone() is not None


    def upsert_glide_kb_item(
        self,
        *,
        tenant_id: str,
        item_id: str,
        table_name: str,
        row_id: str,
        row_hash: str,
        project_name: str = "",
        part_number: str = "",
        legacy_id: str = "",
        title: str = "",
        rag_text: str = "",
        raw_json: Dict[str, Any] | None = None,
    ) -> None:
        sql = """
        INSERT INTO glide_kb_items (
        tenant_id, item_id, table_name, row_id,
        row_hash,
        project_name, part_number, legacy_id,
        title, rag_text, raw_json,
        updated_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb, now())
        ON CONFLICT (tenant_id, item_id)
        DO UPDATE SET
        table_name=EXCLUDED.table_name,
        row_id=EXCLUDED.row_id,
        row_hash=EXCLUDED.row_hash,
        project_name=EXCLUDED.project_name,
        part_number=EXCLUDED.part_number,
        legacy_id=EXCLUDED.legacy_id,
        title=EXCLUDED.title,
        rag_text=EXCLUDED.rag_text,
        raw_json=EXCLUDED.raw_json,
        updated_at=now()
        """
        import json as _json
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    tenant_id,
                    item_id,
                    table_name or None,
                    row_id or None,
                    row_hash,
                    project_name or None,
                    part_number or None,
                    legacy_id or None,
                    title or None,
                    rag_text or "",
                    _json.dumps(raw_json or {}),
                ),
            )


    def insert_glide_kb_vector_if_new(
        self,
        *,
        tenant_id: str,
        item_id: str,
        chunk_index: int,
        chunk_text: str,
        embedding: List[float],
        content_hash: str,
    ) -> None:
        """
        Idempotent insert: DO NOTHING if the (tenant_id,item_id,content_hash) already exists.
        """
        sql = """
        INSERT INTO glide_kb_vectors (
        tenant_id, item_id,
        chunk_index, chunk_text,
        embedding,
        content_hash,
        updated_at
        )
        VALUES (%s,%s,%s,%s,%s::vector,%s, now())
        ON CONFLICT (tenant_id, item_id, content_hash)
        DO NOTHING
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                (tenant_id, item_id, int(chunk_index), chunk_text or "", _vec_str(embedding), content_hash),
            )
    def ccp_hash_exists(self, *, tenant_id: str, ccp_id: str, chunk_type: str, content_hash: str) -> bool:
        sql = """
        SELECT 1
        FROM ccp_vectors
        WHERE tenant_id=%s AND ccp_id=%s AND chunk_type=%s AND content_hash=%s
        LIMIT 1
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (tenant_id, ccp_id, chunk_type, content_hash))
            return cur.fetchone() is not None

    def dashboard_hash_exists(self, *, tenant_id: str, content_hash: str) -> bool:
        sql = """
        SELECT 1
        FROM dashboard_vectors
        WHERE tenant_id=%s AND content_hash=%s
        LIMIT 1
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (tenant_id, content_hash))
            return cur.fetchone() is not None

    def get_incident_summary_text(self, *, tenant_id: str, checkin_id: str, vector_type: str) -> Optional[str]:
        sql = """
        SELECT summary_text
        FROM incident_vectors
        WHERE tenant_id=%s AND checkin_id=%s AND vector_type=%s
        LIMIT 1
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (tenant_id, checkin_id, vector_type))
            row = cur.fetchone()
            return row[0] if row else None

    def get_ccp_chunk_text(
        self,
        *,
        tenant_id: str,
        ccp_id: str,
        chunk_type: str,
        content_hash: str,
    ) -> Optional[str]:
        sql = """
        SELECT chunk_text
        FROM ccp_vectors
        WHERE tenant_id=%s AND ccp_id=%s AND chunk_type=%s AND content_hash=%s
        LIMIT 1
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (tenant_id, ccp_id, chunk_type, content_hash))
            row = cur.fetchone()
            return row[0] if row else None

    # ---------------------------
    # Upserts
    # ---------------------------

    def upsert_incident_vector(
        self,
        *,
        tenant_id: str,
        checkin_id: str,
        vector_type: str,
        embedding: List[float],
        project_name: str,
        part_number: str,
        legacy_id: str,
        status: str,
        text: str,
    ) -> None:
        sql = """
        INSERT INTO incident_vectors (
          tenant_id, checkin_id, vector_type, embedding,
          project_name, part_number, legacy_id,
          status, summary_text, updated_at
        )
        VALUES (%s,%s,%s,%s::vector,%s,%s,%s,%s,%s, now())
        ON CONFLICT (tenant_id, checkin_id, vector_type)
        DO UPDATE SET
          embedding=EXCLUDED.embedding,
          project_name=EXCLUDED.project_name,
          part_number=EXCLUDED.part_number,
          legacy_id=EXCLUDED.legacy_id,
          status=EXCLUDED.status,
          summary_text=EXCLUDED.summary_text,
          updated_at=now()
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    tenant_id,
                    checkin_id,
                    vector_type,
                    _vec_str(embedding),
                    project_name or None,
                    part_number or None,
                    legacy_id or None,
                    status or None,
                    text or "",
                ),
            )

    def upsert_ccp_chunk(
        self,
        *,
        tenant_id: str,
        ccp_id: str,
        ccp_name: str,
        project_name: str,
        part_number: str,
        legacy_id: str,
        chunk_type: str,
        chunk_text: str,
        source_ref: str,
        embedding: List[float],
        content_hash: Optional[str] = None,
    ) -> None:
        # If caller provides content_hash, we trust it. Else compute from chunk_text.
        h = content_hash or _sha256_text(f"{ccp_id}|{chunk_type}|{_norm_text_for_hash(chunk_text)}")

        sql = """
        INSERT INTO ccp_vectors (
          tenant_id,
          ccp_id, ccp_name,
          project_name, part_number, legacy_id,
          chunk_type, chunk_text, source_ref,
          embedding,
          content_hash,
          updated_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::vector,%s, now())
        ON CONFLICT (tenant_id, ccp_id, chunk_type, content_hash)
        DO UPDATE SET
          ccp_name=EXCLUDED.ccp_name,
          project_name=EXCLUDED.project_name,
          part_number=EXCLUDED.part_number,
          legacy_id=EXCLUDED.legacy_id,
          chunk_text=EXCLUDED.chunk_text,
          source_ref=EXCLUDED.source_ref,
          embedding=EXCLUDED.embedding,
          updated_at=now()
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    tenant_id,
                    ccp_id,
                    ccp_name or None,
                    project_name or None,
                    part_number or None,
                    legacy_id or None,
                    chunk_type,
                    chunk_text,
                    source_ref or "",
                    _vec_str(embedding),
                    h,
                ),
            )

    def upsert_dashboard_update(
        self,
        *,
        tenant_id: str,
        project_name: Optional[str],
        part_number: Optional[str],
        legacy_id: Optional[str],
        update_message: str,
        embedding: List[float],
        content_hash: Optional[str] = None,
    ) -> None:
        h = content_hash or _sha256_text(
            f"{tenant_id}|{legacy_id or ''}|{project_name or ''}|{part_number or ''}|{update_message}"
        )

        sql = """
        INSERT INTO dashboard_vectors (
          tenant_id, project_name, part_number, legacy_id,
          update_message, embedding, content_hash
        )
        VALUES (%s,%s,%s,%s,%s,%s::vector,%s)
        ON CONFLICT (tenant_id, content_hash)
        DO NOTHING
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                (tenant_id, project_name, part_number, legacy_id, update_message, _vec_str(embedding), h),
            )
    # ---------------------------
    # Company profile vectors
    # ---------------------------

    def upsert_company_profile(
        self,
        *,
        tenant_row_id: str,
        company_name: str,
        company_description: str,
        embedding: List[float],
        content_hash: Optional[str] = None,
    ) -> None:
        tenant_row_id = (tenant_row_id or "").strip()
        if not tenant_row_id:
            return

        text = (company_description or "").strip()
        if not text:
            return

        h = content_hash or self.hash_text(f"{company_name or ''}\n{text}")

        sql = """
        INSERT INTO company_vectors (
          tenant_row_id, embedding, company_name, company_description, content_hash, updated_at
        )
        VALUES (%s,%s::vector,%s,%s,%s, now())
        ON CONFLICT (tenant_row_id)
        DO UPDATE SET
          embedding=EXCLUDED.embedding,
          company_name=EXCLUDED.company_name,
          company_description=EXCLUDED.company_description,
          content_hash=EXCLUDED.content_hash,
          updated_at=now()
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    tenant_row_id,
                    _vec_str(embedding),
                    (company_name or "").strip() or None,
                    text,
                    h,
                ),
            )
    def get_company_profile_by_tenant_row_id(self, *, tenant_row_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch the exact company profile by tenant_row_id.
        This is the correct behavior when tenant_id is known.
        """
        tid = (tenant_row_id or "").strip()
        if not tid:
            return None

        sql = """
        SELECT tenant_row_id, company_name, company_description
        FROM company_vectors
        WHERE tenant_row_id=%s
        LIMIT 1
        """
        with self._conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (tid,))
            r = cur.fetchone()
            if not r:
                return None
            return {
                "tenant_row_id": r.get("tenant_row_id"),
                "company_name": r.get("company_name") or "",
                "company_description": r.get("company_description") or "",
            }
    def search_company_profiles(
        self,
        *,
        query_embedding: List[float],
        top_k: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        Finds most relevant company description (global-ish).
        If you later want tenant-scoped companies, add tenant filtering.
        """
        sql = """
        SELECT
          tenant_row_id,
          company_name,
          company_description,
          (embedding <=> %s::vector) AS distance
        FROM company_vectors
        ORDER BY embedding <=> %s::vector
        LIMIT %s
        """
        qv = _vec_str(query_embedding)
        with self._conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (qv, qv, int(top_k)))
            rows = cur.fetchall() or []
            return [
                {
                    "tenant_row_id": r["tenant_row_id"],
                    "company_name": r["company_name"],
                    "company_description": r["company_description"],
                    "distance": float(r["distance"]),
                }
                for r in rows
            ]

    # ---------------------------
    # Search (pgvector cosine distance)
    # ---------------------------

    def search_incidents(
        self,
        *,
        tenant_id: str,
        query_embedding: List[float],
        top_k: int = 30,
        project_name: Optional[str] = None,
        part_number: Optional[str] = None,
        legacy_id: Optional[str] = None,   # NEW
        vector_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        where = ["tenant_id=%s"]
        args: List[Any] = [tenant_id]

        if vector_type:
            where.append("vector_type=%s")
            args.append(vector_type)

        if project_name:
            where.append("project_name=%s")
            args.append(project_name)

        if part_number:
            where.append("part_number=%s")
            args.append(part_number)

        if legacy_id:
            where.append("legacy_id=%s")
            args.append(legacy_id)

        sql = f"""
        SELECT
          checkin_id,
          vector_type,
          summary_text,
          project_name,
          part_number,
          legacy_id,
          status,
          (embedding <=> %s::vector) AS distance
        FROM incident_vectors
        WHERE {" AND ".join(where)}
        ORDER BY embedding <=> %s::vector
        LIMIT %s
        """
        qv = _vec_str(query_embedding)
        with self._conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, [qv, *args, qv, int(top_k)])
            rows = cur.fetchall() or []
            out = []
            for r in rows:
                out.append(
                    {
                        "checkin_id": r["checkin_id"],
                        "vector_type": r["vector_type"],
                        "summary": r["summary_text"],
                        "project_name": r["project_name"],
                        "part_number": r["part_number"],
                        "legacy_id": r["legacy_id"],
                        "status": r["status"],
                        "distance": float(r["distance"]),
                    }
                )
            return out

    def search_ccp_chunks(
        self,
        *,
        tenant_id: str,
        query_embedding: List[float],
        top_k: int = 30,
        project_name: Optional[str] = None,
        part_number: Optional[str] = None,
        legacy_id: Optional[str] = None,   # NEW
    ) -> List[Dict[str, Any]]:
        where = ["tenant_id=%s"]
        args: List[Any] = [tenant_id]

        if project_name:
            where.append("(project_name=%s OR project_name IS NULL)")
            args.append(project_name)

        if part_number:
            where.append("(part_number=%s OR part_number IS NULL)")
            args.append(part_number)

        if legacy_id:
            where.append("(legacy_id=%s OR legacy_id IS NULL)")
            args.append(legacy_id)

        sql = f"""
        SELECT
          ccp_id, ccp_name, chunk_type, chunk_text, source_ref,
          (embedding <=> %s::vector) AS distance
        FROM ccp_vectors
        WHERE {" AND ".join(where)}
        ORDER BY embedding <=> %s::vector
        LIMIT %s
        """
        qv = _vec_str(query_embedding)
        with self._conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, [qv, *args, qv, int(top_k)])
            rows = cur.fetchall() or []
            return [
                {
                    "ccp_id": r["ccp_id"],
                    "ccp_name": r["ccp_name"],
                    "chunk_type": r["chunk_type"],
                    "text": r["chunk_text"],
                    "source_ref": r["source_ref"],
                    "distance": float(r["distance"]),
                }
                for r in rows
            ]

    def search_dashboard_updates(
        self,
        *,
        tenant_id: str,
        query_embedding: List[float],
        top_k: int = 20,
        project_name: Optional[str] = None,
        part_number: Optional[str] = None,
        legacy_id: Optional[str] = None,   # NEW
    ) -> List[Dict[str, Any]]:
        where = ["tenant_id=%s"]
        args: List[Any] = [tenant_id]

        if project_name:
            where.append("(project_name=%s OR project_name IS NULL)")
            args.append(project_name)

        if part_number:
            where.append("(part_number=%s OR part_number IS NULL)")
            args.append(part_number)

        if legacy_id:
            where.append("(legacy_id=%s OR legacy_id IS NULL)")
            args.append(legacy_id)
            
        sql = f"""
        SELECT
          update_message,
          project_name,
          part_number,
          legacy_id,
          (embedding <=> %s::vector) AS distance
        FROM dashboard_vectors
        WHERE {" AND ".join(where)}
        ORDER BY embedding <=> %s::vector
        LIMIT %s
        """
        qv = _vec_str(query_embedding)
        with self._conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, [qv, *args, qv, int(top_k)])
            rows = cur.fetchall() or []
            return [
                {
                    "update_message": r["update_message"],
                    "project_name": r["project_name"],
                    "part_number": r["part_number"],
                    "legacy_id": r["legacy_id"],
                    "distance": float(r["distance"]),
                }
                for r in rows
            ]

    def search_glide_kb_chunks(
        self,
        *,
        tenant_id: str,
        query_embedding: List[float],
        top_k: int = 30,
        project_name: Optional[str] = None,
        part_number: Optional[str] = None,
        legacy_id: Optional[str] = None,
        table_names: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Vector search over Glide KB, joined with item metadata.
        Filters are "soft": match exact OR allow NULL/empty (so generic KB still works).

        table_names: if provided, restrict to those tables (e.g. ["raw_material","processes","boughtouts"])
        """
        if not tenant_id:
            return []

        where = ["i.tenant_id=%s"]
        args: List[Any] = [tenant_id]

        # soft filters (exact OR missing)
        if project_name:
            where.append("(i.project_name=%s OR i.project_name IS NULL OR i.project_name='')")
            args.append(project_name)

        if part_number:
            where.append("(i.part_number=%s OR i.part_number IS NULL OR i.part_number='')")
            args.append(part_number)

        if legacy_id:
            where.append("(i.legacy_id=%s OR i.legacy_id IS NULL OR i.legacy_id='')")
            args.append(legacy_id)

        if table_names:
            # psycopg2 will adapt python list into PG array
            where.append("i.table_name = ANY(%s)")
            args.append(table_names)

        sql = f"""
        SELECT
          i.table_name,
          i.row_id,
          i.item_id,
          i.title,
          i.project_name,
          i.part_number,
          i.legacy_id,
          v.chunk_index,
          v.chunk_text,
          (v.embedding <=> %s::vector) AS distance
        FROM glide_kb_vectors v
        JOIN glide_kb_items i
          ON i.tenant_id=v.tenant_id AND i.item_id=v.item_id
        WHERE {" AND ".join(where)}
        ORDER BY v.embedding <=> %s::vector
        LIMIT %s
        """
        qv = _vec_str(query_embedding)

        with self._conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, [qv, *args, qv, int(top_k)])
            rows = cur.fetchall() or []

        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "table_name": r.get("table_name") or "",
                    "row_id": r.get("row_id") or "",
                    "item_id": r.get("item_id") or "",
                    "title": r.get("title") or "",
                    "project_name": r.get("project_name") or "",
                    "part_number": r.get("part_number") or "",
                    "legacy_id": r.get("legacy_id") or "",
                    "chunk_index": int(r.get("chunk_index") or 0),
                    "text": r.get("chunk_text") or "",
                    "distance": float(r.get("distance") or 0.0),
                }
            )
        return out