# service/app/tools/db_tool.py
# Database utility tool for managing artifacts.
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

    def get_artifact_url_and_meta_by_source_hash(
        self,
        *,
        tenant_id: str,
        checkin_id: str,
        artifact_type: str,
        source_hash: str,
    ) -> tuple[str, Dict[str, Any]]:
        """
        Returns (url, meta_dict) for latest matching artifact row.
        Useful so callers can rebuild thumbnail URLs from meta->drive_file_id.
        """
        q = """
        SELECT COALESCE(url,''), COALESCE(meta,'{}'::jsonb)
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
                    if not row:
                        return "", {}
                    url = (row[0] or "").strip()
                    meta = row[1] if isinstance(row[1], dict) else {}
                    return url, meta
        except Exception:
            return "", {}
        
    def image_captions_by_hash(
        self,
        *,
        tenant_id: str,
        checkin_id: str,
    ) -> Dict[str, str]:
        """
        Returns {source_hash: caption} for IMAGE_CAPTION artifacts for this tenant+checkin.
        Used so media ingest can reuse old captions and still upsert MEDIA vectors.
        """
        q = """
        SELECT
          COALESCE(meta->>'source_hash','') AS source_hash,
          COALESCE(meta->>'caption','') AS caption
        FROM artifacts
        WHERE artifact_type = 'IMAGE_CAPTION'
          AND COALESCE(meta->>'tenant_id','') = %s
          AND COALESCE(meta->>'checkin_id','') = %s
          AND COALESCE(meta->>'source_hash','') <> ''
        ORDER BY created_at DESC
        """
        out: Dict[str, str] = {}
        try:
            with self._conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(q, (tenant_id, checkin_id))
                    for (h, c) in cur.fetchall() or []:
                        hh = str(h or "").strip()
                        if not hh:
                            continue
                        cc = str(c or "").strip()
                        # Keep newest caption per hash
                        if hh not in out and cc:
                            out[hh] = cc
        except Exception:
            return {}
        return out

    # ----------------------------
    # Checkin file artifacts (Files column)
    # ----------------------------

    def upsert_checkin_file_artifact(
        self,
        *,
        tenant_id: str,
        checkin_id: str,
        source_hash: str,
        source_ref: str = "",
        filename: str = "",
        mime_type: str = "",
        byte_size: int = 0,
        drive_file_id: str = "",
        direct_url: str = "",
        content_hash: str = "",
        extracted_text: str = "",
        extracted_json: Dict[str, Any] | None = None,
        analysis_json: Dict[str, Any] | None = None,
    ) -> None:
        q = """
        INSERT INTO checkin_file_artifacts (
          tenant_id, checkin_id, source_hash,
          source_ref, filename, mime_type, byte_size,
          drive_file_id, direct_url, content_hash,
          extracted_text, extracted_json, analysis_json,
          updated_at
        )
        VALUES (
          %s,%s,%s,
          %s,%s,%s,%s,
          %s,%s,%s,
          %s,%s::jsonb,%s::jsonb,
          now()
        )
        ON CONFLICT (tenant_id, checkin_id, source_hash)
        DO UPDATE SET
          source_ref=EXCLUDED.source_ref,
          filename=EXCLUDED.filename,
          mime_type=EXCLUDED.mime_type,
          byte_size=EXCLUDED.byte_size,
          drive_file_id=EXCLUDED.drive_file_id,
          direct_url=EXCLUDED.direct_url,
          content_hash=EXCLUDED.content_hash,
          extracted_text=EXCLUDED.extracted_text,
          extracted_json=EXCLUDED.extracted_json,
          analysis_json=EXCLUDED.analysis_json,
          updated_at=now()
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    q,
                    (
                        tenant_id,
                        checkin_id,
                        source_hash,
                        source_ref or None,
                        filename or None,
                        mime_type or None,
                        int(byte_size or 0),
                        drive_file_id or None,
                        direct_url or None,
                        content_hash or None,
                        extracted_text or "",
                        json.dumps(extracted_json or {}),
                        json.dumps(analysis_json or {}),
                    ),
                )

    def checkin_file_artifact_exists(
        self,
        *,
        tenant_id: str,
        checkin_id: str,
        source_hash: str,
        content_hash: str = "",
    ) -> bool:
        """
        Returns True if already processed.
        If content_hash is given, require that match (stronger idempotency).
        """
        if content_hash:
            q = """
            SELECT 1
            FROM checkin_file_artifacts
            WHERE tenant_id=%s AND checkin_id=%s AND source_hash=%s AND COALESCE(content_hash,'')=%s
            LIMIT 1
            """
            args = (tenant_id, checkin_id, source_hash, content_hash)
        else:
            q = """
            SELECT 1
            FROM checkin_file_artifacts
            WHERE tenant_id=%s AND checkin_id=%s AND source_hash=%s
            LIMIT 1
            """
            args = (tenant_id, checkin_id, source_hash)

        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, args)
                return cur.fetchone() is not None

    def get_checkin_file_briefs(
        self,
        *,
        tenant_id: str,
        checkin_id: str,
        max_items: int = 6,
    ) -> list[dict]:
        """
        Returns brief info for composing prompt context.
        """
        q = """
        SELECT
          source_hash,
          COALESCE(filename,'') AS filename,
          COALESCE(mime_type,'') AS mime_type,
          COALESCE(analysis_json,'{}'::jsonb) AS analysis_json
        FROM checkin_file_artifacts
        WHERE tenant_id=%s AND checkin_id=%s
        ORDER BY updated_at DESC
        LIMIT %s
        """
        out: list[dict] = []
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (tenant_id, checkin_id, int(max_items)))
                rows = cur.fetchall() or []
                for (h, fn, mt, aj) in rows:
                    out.append(
                        {
                            "source_hash": str(h or ""),
                            "filename": str(fn or ""),
                            "mime_type": str(mt or ""),
                            "analysis_json": aj if isinstance(aj, dict) else {},
                        }
                    )
        return out