from __future__ import annotations

from typing import Any, Dict, List, Tuple

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key as s_key, _norm_value as s_norm
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool

from .glide_ingest_base import (
    _norm_text,
    _sha256,
    chunk_text,
    normalize_row_json,
    compute_row_hash,
    build_rag_text,
)


def _get_casefold(row: Dict[str, Any], header_name: str) -> str:
    return s_norm((row or {}).get(s_key(header_name), ""))


def ingest_sheet_projects(settings: Settings, *, limit: int = 0) -> Dict[str, Any]:
    sheets = SheetsTool(settings)
    embed = EmbedTool(settings)
    vec = VectorTool(settings)

    # mapped columns (from sheets_mapping.yaml)
    col_legacy = sheets.map.col("project", "legacy_id")
    col_tenant = sheets.map.col("project", "company_row_id")
    col_pname = sheets.map.col("project", "project_name")
    col_part = sheets.map.col("project", "part_number")

    rows = sheets.list_projects() or []
    if limit and limit > 0:
        rows = rows[:limit]

    seen = 0
    ok = 0
    skipped_missing_tenant = 0
    skipped_missing_rowid = 0
    errors = 0
    err_samples: List[Dict[str, str]] = []
    missing_tenant_samples: List[Dict[str, str]] = []

    for r in rows:
        seen += 1

        tenant_id = _get_casefold(r, col_tenant)
        legacy_id = _get_casefold(r, col_legacy)
        project_name = _get_casefold(r, col_pname)
        part_number = _get_casefold(r, col_part)

        # row id: prefer legacy_id, else composite
        row_id = (legacy_id or "").strip()
        if not row_id:
            # fallback stable-ish id
            if project_name and part_number:
                row_id = f"{project_name}::{part_number}"
            else:
                skipped_missing_rowid += 1
                continue

        if not tenant_id:
            skipped_missing_tenant += 1
            if len(missing_tenant_samples) < 25:
                missing_tenant_samples.append(
                    {
                        "row_id": row_id,
                        "project_name": project_name,
                        "part_number": part_number,
                        "legacy_id": legacy_id,
                        "tenant_col": col_tenant,
                    }
                )
            continue

        try:
            # normalize + hash
            norm_row = normalize_row_json(r, drop_keys=[])
            table_name = "sheets_project"
            row_hash = compute_row_hash(table_name, row_id, norm_row)

            item_id = f"{table_name}:{row_id}".strip()

            title = project_name or legacy_id or row_id

            rag = build_rag_text(
                entity="project",
                title=title,
                project_name=project_name,
                part_number=part_number,
                legacy_id=legacy_id,
                norm_row=norm_row,
                include_keys=None,
            )

            vec.upsert_glide_kb_item(
                tenant_id=tenant_id,
                item_id=item_id,
                table_name=table_name,
                row_id=row_id,
                row_hash=row_hash,
                project_name=project_name,
                part_number=part_number,
                legacy_id=legacy_id,
                title=title,
                rag_text=rag,
                raw_json=norm_row,
            )

            chunks = chunk_text(rag, max_chars=900)
            for i, ch in enumerate(chunks):
                ch_norm = _norm_text(ch)
                if not ch_norm:
                    continue

                content_hash = _sha256(f"{tenant_id}|{item_id}|{i}|{ch_norm}")
                if vec.glide_kb_vector_hash_exists(tenant_id=tenant_id, item_id=item_id, content_hash=content_hash):
                    continue

                emb = embed.embed_text(ch_norm)
                vec.insert_glide_kb_vector_if_new(
                    tenant_id=tenant_id,
                    item_id=item_id,
                    chunk_index=i,
                    chunk_text=ch_norm,
                    embedding=emb,
                    content_hash=content_hash,
                )

            ok += 1

        except Exception as e:
            errors += 1
            if len(err_samples) < 25:
                err_samples.append(
                    {
                        "row_id": row_id,
                        "tenant_id": tenant_id,
                        "error": str(e)[:400],
                    }
                )

    return {
        "ok": True,
        "source": "sheets_project",
        "rows_seen": seen,
        "rows_ok": ok,
        "rows_error": errors,
        "skipped_missing_rowid": skipped_missing_rowid,
        "skipped_missing_tenant": skipped_missing_tenant,
        "missing_tenant_samples": missing_tenant_samples,
        "error_samples": err_samples,
    }