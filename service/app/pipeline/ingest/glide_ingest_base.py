from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ...config import Settings
from ...integrations.glide_client import GlideClient
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool
from .utils import chunk_text


def _key(s: Any) -> str:
    return str(s or "").strip().lower()


def _norm_text(s: Any) -> str:
    t = str(s or "")
    t = t.replace("\r", "\n")
    t = "\n".join([ln.strip() for ln in t.split("\n") if ln.strip()])
    return t.strip()


def _sha256(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _stable_json(obj: Dict[str, Any]) -> str:
    # Stable, deterministic JSON for hashing
    return json.dumps(obj or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _is_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and not v.strip():
        return True
    return False


def normalize_row_json(row: Dict[str, Any], *, drop_keys: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    """
    Normalize a Glide row dict to be hash-stable:
      - stringify non-primitive values
      - trim whitespace
      - optionally drop volatile columns
    """
    drop = set([_key(k) for k in (drop_keys or [])])

    out: Dict[str, Any] = {}
    for k, v in (row or {}).items():
        kk = str(k or "").strip()
        if not kk:
            continue
        if _key(kk) in drop:
            continue

        if isinstance(v, (str, int, float, bool)) or v is None:
            if isinstance(v, str):
                out[kk] = _norm_text(v)
            else:
                out[kk] = v
        else:
            # lists/dicts/etc -> stable string
            try:
                out[kk] = _norm_text(json.dumps(v, ensure_ascii=False, sort_keys=True))
            except Exception:
                out[kk] = _norm_text(str(v))

    return out


def compute_row_hash(table_name: str, row_id: str, norm_row: Dict[str, Any]) -> str:
    base = f"{table_name}|{row_id}|{_stable_json(norm_row)}"
    return _sha256(base)


def build_rag_text(
    *,
    entity: str,
    title: str,
    project_name: str,
    part_number: str,
    legacy_id: str,
    norm_row: Dict[str, Any],
    include_keys: Optional[List[str]] = None,
) -> str:
    """
    Build human-readable RAG text. Keep it deterministic.
    include_keys: if provided, only include these keys (case-insensitive).
    """
    allow = None
    if include_keys:
        allow = set([_key(x) for x in include_keys])

    lines: List[str] = []
    lines.append(f"Entity: {entity}".strip())

    if title:
        lines.append(f"Title: {title}".strip())
    if project_name:
        lines.append(f"Project: {project_name}".strip())
    if part_number:
        lines.append(f"Part Number: {part_number}".strip())
    if legacy_id:
        lines.append(f"Legacy ID: {legacy_id}".strip())

    # deterministic key order
    for k in sorted(norm_row.keys(), key=lambda x: x.lower()):
        if allow is not None and _key(k) not in allow:
            continue
        v = norm_row.get(k)
        if _is_empty(v):
            continue
        vv = _norm_text(v)
        if not vv:
            continue
        lines.append(f"{k}: {vv}")

    return "\n".join([ln for ln in lines if ln.strip()]).strip()


@dataclass
class GlideIngestSpec:
    entity: str
    table_name: str
    tenant_id_column: str = "Company Row ID"   # tenant_id (Glide row id of company)
    rowid_column: str = "Row ID"

    # Common linkage fields (you will tune these to your actual Glide column names)
    project_name_column: str = "Project"
    part_number_column: str = "Part Number"
    legacy_id_column: str = "Legacy ID"
    project_row_id_column: str = "Project Row ID"

    title_column: str = "Name"

    # Hash stability: columns to drop because they change often
    drop_keys: Optional[List[str]] = None

    # RAG selection: keep small & meaningful
    rag_include_keys: Optional[List[str]] = None


def _derive_base_fields(
    row: Dict[str, Any],
    spec: GlideIngestSpec,
    *,
    project_index_by_row_id: Dict[str, Dict[str, str]],
    project_index_by_triplet: Dict[Tuple[str, str], Dict[str, str]],
) -> Tuple[str, str, str, str, str]:
    """
    Returns: (tenant_id, project_name, part_number, legacy_id, title)
    - tenant_id is required for DB partitioning (skip row if missing)
    - legacy_id is resolved if possible via project index
    """
    tenant_id = _norm_text(row.get(spec.tenant_id_column, ""))
    project_name = _norm_text(row.get(spec.project_name_column, ""))
    part_number = _norm_text(row.get(spec.part_number_column, ""))
    legacy_id = _norm_text(row.get(spec.legacy_id_column, ""))
    title = _norm_text(row.get(spec.title_column, ""))

    if not legacy_id:
        proj_row_id = _norm_text(row.get(spec.project_row_id_column, ""))
        if proj_row_id:
            pr = project_index_by_row_id.get(_key(proj_row_id))
            if pr:
                legacy_id = _norm_text(pr.get("legacy_id", "")) or legacy_id
                project_name = project_name or _norm_text(pr.get("project_name", ""))
                part_number = part_number or _norm_text(pr.get("part_number", ""))

    if not legacy_id and project_name and part_number:
        pr2 = project_index_by_triplet.get((_key(project_name), _key(part_number)))
        if pr2:
            legacy_id = _norm_text(pr2.get("legacy_id", "")) or legacy_id

    return tenant_id, project_name, part_number, legacy_id, title


def build_project_indexes(
    *,
    project_rows: List[Dict[str, Any]],
    tenant_id_column: str,
    project_name_column: str,
    part_number_column: str,
    legacy_id_column: str,
    rowid_column: str = "Row ID",
) -> Tuple[Dict[str, Dict[str, str]], Dict[Tuple[str, str], Dict[str, str]]]:
    """
    Build:
      - by project_row_id
      - by (project_name, part_number)
    """
    by_row_id: Dict[str, Dict[str, str]] = {}
    by_trip: Dict[Tuple[str, str], Dict[str, str]] = {}

    for r in project_rows or []:
        rid = _norm_text(r.get(rowid_column, ""))
        tid = _norm_text(r.get(tenant_id_column, ""))
        pname = _norm_text(r.get(project_name_column, ""))
        pnum = _norm_text(r.get(part_number_column, ""))
        lid = _norm_text(r.get(legacy_id_column, ""))

        if rid:
            by_row_id[_key(rid)] = {
                "tenant_id": tid,
                "project_name": pname,
                "part_number": pnum,
                "legacy_id": lid,
            }

        if pname and pnum:
            by_trip[(_key(pname), _key(pnum))] = {
                "tenant_id": tid,
                "project_name": pname,
                "part_number": pnum,
                "legacy_id": lid,
            }

    return by_row_id, by_trip


def ingest_rows(
    settings: Settings,
    *,
    spec: GlideIngestSpec,
    rows: List[Dict[str, Any]],
    project_index_by_row_id: Dict[str, Dict[str, str]],
    project_index_by_triplet: Dict[Tuple[str, str], Dict[str, str]],
    limit: int = 0,
) -> Dict[str, Any]:
    """
    Core ingestion:
      - normalize row json
      - compute row_hash
      - derive tenant/project/part/legacy
      - build rag_text
      - upsert glide_kb_items
      - insert glide_kb_vectors only if content_hash is new
    """
    embed = EmbedTool(settings)
    vec = VectorTool(settings)

    seen = 0
    ok = 0
    skipped_missing_tenant = 0
    skipped_missing_rowid = 0
    errors = 0
    err_samples: List[Dict[str, str]] = []

    if limit and limit > 0:
        rows = rows[:limit]

    for row in rows or []:
        seen += 1

        row_id = _norm_text(row.get(spec.rowid_column, ""))
        if not row_id:
            skipped_missing_rowid += 1
            continue

        tenant_id, project_name, part_number, legacy_id, title = _derive_base_fields(
            row,
            spec,
            project_index_by_row_id=project_index_by_row_id,
            project_index_by_triplet=project_index_by_triplet,
        )
        if not tenant_id:
            skipped_missing_tenant += 1
            continue

        try:
            norm_row = normalize_row_json(row, drop_keys=spec.drop_keys or [])
            row_hash = compute_row_hash(spec.table_name, row_id, norm_row)

            item_id = f"{spec.table_name}:{row_id}".strip()

            rag = build_rag_text(
                entity=spec.entity,
                title=title,
                project_name=project_name,
                part_number=part_number,
                legacy_id=legacy_id,
                norm_row=norm_row,
                include_keys=spec.rag_include_keys,
            )

            # 1) upsert item
            vec.upsert_glide_kb_item(
                tenant_id=tenant_id,
                item_id=item_id,
                table_name=spec.table_name,
                row_id=row_id,
                row_hash=row_hash,
                project_name=project_name,
                part_number=part_number,
                legacy_id=legacy_id,
                title=title,
                rag_text=rag,
                raw_json=norm_row,
            )

            # 2) vectors: chunk + insert if new
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
                        "table": spec.table_name,
                        "row_id": row_id,
                        "tenant_id": tenant_id,
                        "error": str(e)[:400],
                    }
                )

    return {
        "ok": True,
        "table": spec.table_name,
        "entity": spec.entity,
        "rows_seen": seen,
        "rows_ok": ok,
        "rows_error": errors,
        "skipped_missing_rowid": skipped_missing_rowid,
        "skipped_missing_tenant": skipped_missing_tenant,
        "error_samples": err_samples,
    }


def full_scan_table(
    settings: Settings,
    *,
    spec: GlideIngestSpec,
    project_spec: GlideIngestSpec,
    limit: int = 0,
) -> Dict[str, Any]:
    """
    One-time reconciliation job:
      - list projects (to build indexes)
      - list target table rows
      - ingest all
    """
    glide = GlideClient(settings)

    # Build project indexes first (needed for legacy_id resolution)
    project_rows = glide.list_table_rows(project_spec.table_name)
    by_row_id, by_trip = build_project_indexes(
        project_rows=project_rows,
        tenant_id_column=project_spec.tenant_id_column,
        project_name_column=project_spec.project_name_column,
        part_number_column=project_spec.part_number_column,
        legacy_id_column=project_spec.legacy_id_column,
        rowid_column=project_spec.rowid_column,
    )

    rows = glide.list_table_rows(spec.table_name)

    return ingest_rows(
        settings,
        spec=spec,
        rows=rows,
        project_index_by_row_id=by_row_id,
        project_index_by_triplet=by_trip,
        limit=limit,
    )


def incremental_upsert_row(
    settings: Settings,
    *,
    spec: GlideIngestSpec,
    project_spec: GlideIngestSpec,
    row_id: str,
) -> Dict[str, Any]:
    """
    Webhook incremental upsert:
      - build project indexes (small cost; optimize later with caching)
      - fetch single row by Row ID (SQL)
      - ingest that one row
    """
    glide = GlideClient(settings)

    project_rows = glide.list_table_rows(project_spec.table_name)
    by_row_id, by_trip = build_project_indexes(
        project_rows=project_rows,
        tenant_id_column=project_spec.tenant_id_column,
        project_name_column=project_spec.project_name_column,
        part_number_column=project_spec.part_number_column,
        legacy_id_column=project_spec.legacy_id_column,
        rowid_column=project_spec.rowid_column,
    )

    row = glide.get_row_by_row_id(spec.table_name, row_id, rowid_column=spec.rowid_column)
    if not row:
        return {"ok": False, "error": f"Row not found. table={spec.table_name} row_id={row_id}"}

    return ingest_rows(
        settings,
        spec=spec,
        rows=[row],
        project_index_by_row_id=by_row_id,
        project_index_by_triplet=by_trip,
        limit=0,
    )