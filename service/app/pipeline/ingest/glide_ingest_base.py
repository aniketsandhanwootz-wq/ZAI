# service/app/pipeline/ingest/glide_ingest_base.py
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ...config import Settings
from ...integrations.glide_client import GlideClient
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool
from ...tools.company_tool import derive_company_name_from_project_name
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
    return json.dumps(obj or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _is_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and not v.strip():
        return True
    return False


# -------------------------
# Glide key helpers
# -------------------------

def _candidate_keys(col: str) -> List[str]:
    """
    Glide can return keys like:
      - "remote\x1dProject number"
      - "remote Project number"
      - "Project number"
      - "$rowID"
    We'll try a few variants.
    """
    c = (col or "").strip()
    if not c:
        return []
    return [
        c,
        f"remote\x1d{c}",
        f"remote {c}",
        c.replace("\x1d", " ").strip(),
    ]


def _get(row: Dict[str, Any], col: str, default: str = "") -> str:
    for k in _candidate_keys(col):
        if k in (row or {}):
            return _norm_text((row or {}).get(k))
    return default


_PROJECT_NUM_RE = re.compile(r"(\d{1,6})")


def _extract_project_number(s: str) -> str:
    """
    Pull a numeric project number from strings like:
      "Unnati 137 - ...." -> "137"
      "137" -> "137"
      "Unnati 137" -> "137"
    """
    s = (s or "").strip()
    if not s:
        return ""
    m = _PROJECT_NUM_RE.search(s)
    return m.group(1) if m else ""


# -------------------------
# Normalization / hashing
# -------------------------

def normalize_row_json(row: Dict[str, Any], *, drop_keys: Optional[Iterable[str]] = None) -> Dict[str, Any]:
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

    # NOTE: in your Glide KB tables, this often is NOT present as company row id.
    tenant_id_column: str = "Company Row ID"
    rowid_column: str = "Row ID"

    project_name_column: str = "Project"
    part_number_column: str = "Part Number"
    legacy_id_column: str = "Legacy ID"
    project_row_id_column: str = "Project Row ID"

    title_column: str = "Name"

    drop_keys: Optional[List[str]] = None
    rag_include_keys: Optional[List[str]] = None


# -------------------------
# Company index (Name -> Company $rowID)
# -------------------------

def build_company_index(
    *,
    company_rows: List[Dict[str, Any]],
    company_rowid_column: str,
    company_name_column: str,
) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for r in company_rows or []:
        cid = _get(r, company_rowid_column, "")
        name = _get(r, company_name_column, "")
        if not cid or not name:
            continue
        out[_key(name)] = cid
    return out


# -------------------------
# Sheets Project indexes (Project Number / (Project,Part) -> legacy_id)
# -------------------------

def build_sheet_project_indexes(settings: Settings) -> Tuple[Dict[Tuple[str, str], Dict[str, str]], Dict[str, Dict[str, str]]]:
    """
    Uses Google Sheet Project tab to resolve:
      - legacy_id for a (project_name, part_number)
      - legacy_id + canonical project_name from project_number (e.g. 137)
    This is needed because your Glide KB tables don't reliably carry legacy_id.
    """
    try:
        from ...tools.sheets_tool import SheetsTool, _key as s_key, _norm_value as s_norm
    except Exception:
        return {}, {}

    sheets = SheetsTool(settings)

    # columns (best effort; if mapping misses something, just skip)
    def safe_col(tab: str, field: str) -> str:
        try:
            return sheets.map.col(tab, field)
        except Exception:
            return ""

    k_legacy = s_key(safe_col("project", "legacy_id")) if safe_col("project", "legacy_id") else ""
    k_name = s_key(safe_col("project", "project_name")) if safe_col("project", "project_name") else ""
    k_part = s_key(safe_col("project", "part_number")) if safe_col("project", "part_number") else ""
    k_tenant = s_key(safe_col("project", "company_row_id")) if safe_col("project", "company_row_id") else ""

    # optional: if you map these later
    k_pid = safe_col("project", "project_id")
    k_pnum = safe_col("project", "project_number")
    k_pid = s_key(k_pid) if k_pid else ""
    k_pnum = s_key(k_pnum) if k_pnum else ""

    # fallback headers (even if mapping doesn't define them)
    FALLBACK_PROJECT_NUM_KEYS = [
        "project number", "project no", "project#", "project id", "project_id", "project number/id"
    ]

    by_trip: Dict[Tuple[str, str], Dict[str, str]] = {}
    by_num: Dict[str, Dict[str, str]] = {}

    projects = sheets.list_projects() or []
    for pr in projects:
        legacy_id = s_norm((pr or {}).get(k_legacy, "")) if k_legacy else ""
        pname = s_norm((pr or {}).get(k_name, "")) if k_name else ""
        part = s_norm((pr or {}).get(k_part, "")) if k_part else ""
        tenant_id = s_norm((pr or {}).get(k_tenant, "")) if k_tenant else ""

        # build by (project_name, part_number)  -> includes tenant_id
        if pname and part and legacy_id:
            by_trip[(s_key(pname), s_key(part))] = {
                "legacy_id": legacy_id,
                "tenant_id": tenant_id,
                "project_name": pname,
                "part_number": part,
            }

        # build by project number (STRICT: do NOT use part number as candidate)
        candidates: List[str] = []

        # 1) explicit mapped fields (preferred)
        if k_pid:
            candidates.append(s_norm((pr or {}).get(k_pid, "")))
        if k_pnum:
            candidates.append(s_norm((pr or {}).get(k_pnum, "")))

        # 2) fallback: try common sheet headers if present
        for fk in FALLBACK_PROJECT_NUM_KEYS:
            kk = s_key(fk)
            v = s_norm((pr or {}).get(kk, ""))
            if v:
                candidates.append(v)

        # 3) lastly, allow parsing from project_name (e.g. "Unnati 137")
        if pname:
            candidates.append(pname)

        num = ""
        for c in candidates:
            num = _extract_project_number(c)
            if num:
                break

        if num and (legacy_id or pname):
            by_num.setdefault(
                num,
                {
                    "legacy_id": legacy_id,
                    "tenant_id": tenant_id,
                    "project_name": pname,
                },
            )
            
    return by_trip, by_num


# -------------------------
# Project indexes from Glide Project table (optional)
# -------------------------

def build_project_indexes(
    *,
    project_rows: List[Dict[str, Any]],
    tenant_id_column: str,
    project_name_column: str,
    part_number_column: str,
    legacy_id_column: str,
    rowid_column: str = "Row ID",
) -> Tuple[Dict[str, Dict[str, str]], Dict[Tuple[str, str], Dict[str, str]]]:
    by_row_id: Dict[str, Dict[str, str]] = {}
    by_trip: Dict[Tuple[str, str], Dict[str, str]] = {}

    for r in project_rows or []:
        rid = _get(r, rowid_column, "")
        tid = _get(r, tenant_id_column, "")
        pname = _get(r, project_name_column, "")
        pnum = _get(r, part_number_column, "")
        lid = _get(r, legacy_id_column, "")

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


def _derive_base_fields(
    row: Dict[str, Any],
    spec: GlideIngestSpec,
    *,
    project_index_by_row_id: Dict[str, Dict[str, str]],
    project_index_by_triplet: Dict[Tuple[str, str], Dict[str, str]],
    sheet_project_by_triplet: Dict[Tuple[str, str], Dict[str, str]],
    sheet_project_by_number: Dict[str, Dict[str, str]],
    company_index_by_name: Dict[str, str],
) -> Tuple[str, str, str, str, str]:
    tenant_id = _get(row, spec.tenant_id_column, "")
    project_name = _get(row, spec.project_name_column, "")
    part_number = _get(row, spec.part_number_column, "")
    legacy_id = _get(row, spec.legacy_id_column, "")
    title = _get(row, spec.title_column, "")

    # If project_name is just a number (common in your env: "Project number"),
    # upgrade it using Sheets Project mapping (number -> canonical name + legacy_id)
    proj_num = _extract_project_number(project_name) if project_name else ""
    if proj_num and (project_name.strip().isdigit() or len(project_name.strip()) <= 6):
        sp = sheet_project_by_number.get(proj_num)
        if sp:
            project_name = (sp.get("project_name") or project_name).strip()
            legacy_id = (legacy_id or sp.get("legacy_id") or "").strip()

    # Try resolve legacy_id/project/part via project row id (Glide Project table)
    if not legacy_id:
        proj_row_id = _get(row, spec.project_row_id_column, "")
        if proj_row_id:
            pr = project_index_by_row_id.get(_key(proj_row_id))
            if pr:
                legacy_id = _norm_text(pr.get("legacy_id", "")) or legacy_id
                project_name = project_name or _norm_text(pr.get("project_name", ""))
                part_number = part_number or _norm_text(pr.get("part_number", ""))

    # Try resolve legacy_id via (project, part) using Glide Project index first
    if not legacy_id and project_name and part_number:
        pr2 = project_index_by_triplet.get((_key(project_name), _key(part_number)))
        if pr2:
            legacy_id = _norm_text(pr2.get("legacy_id", "")) or legacy_id

    # Then resolve legacy_id via Sheets Project index (most reliable for you)
    if not legacy_id and project_name and part_number:
        sp2 = sheet_project_by_triplet.get((_key(project_name), _key(part_number)))
        if sp2:
            legacy_id = (sp2.get("legacy_id") or "").strip() or legacy_id

    # Tenant derivation fallback:
    # If tenant_id isn't a Glide Company $rowID in row, derive company from project_name
    # and map to Company table $rowID.
    if not tenant_id:
        derived_company = derive_company_name_from_project_name(project_name)
        if derived_company:
            tenant_id = company_index_by_name.get(_key(derived_company), "")

    return tenant_id, project_name, part_number, legacy_id, title


def ingest_rows(
    settings: Settings,
    *,
    spec: GlideIngestSpec,
    rows: List[Dict[str, Any]],
    project_index_by_row_id: Dict[str, Dict[str, str]],
    project_index_by_triplet: Dict[Tuple[str, str], Dict[str, str]],
    sheet_project_by_triplet: Dict[Tuple[str, str], Dict[str, str]],
    sheet_project_by_number: Dict[str, Dict[str, str]],
    company_index_by_name: Dict[str, str],
    limit: int = 0,
) -> Dict[str, Any]:
    embed = EmbedTool(settings)
    vec = VectorTool(settings)

    seen = 0
    ok = 0
    skipped_missing_tenant = 0
    skipped_missing_rowid = 0
    errors = 0
    err_samples: List[Dict[str, str]] = []
    missing_tenant_samples: List[Dict[str, str]] = []

    if limit and limit > 0:
        rows = rows[:limit]

    for row in rows or []:
        seen += 1

        row_id = _get(row, spec.rowid_column, "")
        if not row_id:
            skipped_missing_rowid += 1
            continue

        tenant_id, project_name, part_number, legacy_id, title = _derive_base_fields(
            row,
            spec,
            project_index_by_row_id=project_index_by_row_id,
            project_index_by_triplet=project_index_by_triplet,
            sheet_project_by_triplet=sheet_project_by_triplet,
            sheet_project_by_number=sheet_project_by_number,
            company_index_by_name=company_index_by_name,
        )
        if not tenant_id:
            skipped_missing_tenant += 1
            if len(missing_tenant_samples) < 25:
                missing_tenant_samples.append(
                    {
                        "row_id": row_id,
                        "project_name": project_name,
                        "part_number": part_number,
                        "legacy_id": legacy_id,
                        "tenant_col": spec.tenant_id_column,
                        "project_col": spec.project_name_column,
                    }
                )
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
        "missing_tenant_samples": missing_tenant_samples,
        "error_samples": err_samples,
    }


def full_scan_table(
    settings: Settings,
    *,
    spec: GlideIngestSpec,
    project_spec: GlideIngestSpec,
    limit: int = 0,
) -> Dict[str, Any]:
    glide = GlideClient(settings)

    # 0) Company index (Name -> $rowID)
    company_rows: List[Dict[str, Any]] = []
    company_table = getattr(settings, "glide_company_table", "") or ""
    company_rowid_col = getattr(settings, "glide_company_rowid_column", "") or "$rowID"
    company_name_col = getattr(settings, "glide_company_name_column", "") or "Name"
    if company_table:
        company_rows = glide.list_table_rows(company_table)
    company_index = build_company_index(
        company_rows=company_rows,
        company_rowid_column=company_rowid_col,
        company_name_column=company_name_col,
    )

    # 1) Project indexes:
    #   - Glide project table (optional, usually empty in your setup)
    by_row_id: Dict[str, Dict[str, str]] = {}
    by_trip: Dict[Tuple[str, str], Dict[str, str]] = {}
    if project_spec.table_name:
        project_rows = glide.list_table_rows(project_spec.table_name)
        by_row_id, by_trip = build_project_indexes(
            project_rows=project_rows,
            tenant_id_column=project_spec.tenant_id_column,
            project_name_column=project_spec.project_name_column,
            part_number_column=project_spec.part_number_column,
            legacy_id_column=project_spec.legacy_id_column,
            rowid_column=project_spec.rowid_column,
        )

    #   - Sheets project mapping (your real source of truth)
    sheet_by_trip, sheet_by_num = build_sheet_project_indexes(settings)

    # 2) Fetch target table rows
    rows = glide.list_table_rows(spec.table_name)

    return ingest_rows(
        settings,
        spec=spec,
        rows=rows,
        project_index_by_row_id=by_row_id,
        project_index_by_triplet=by_trip,
        sheet_project_by_triplet=sheet_by_trip,
        sheet_project_by_number=sheet_by_num,
        company_index_by_name=company_index,
        limit=limit,
    )


def incremental_upsert_row(
    settings: Settings,
    *,
    spec: GlideIngestSpec,
    project_spec: GlideIngestSpec,
    row_id: str,
) -> Dict[str, Any]:
    glide = GlideClient(settings)

    # company index
    company_rows: List[Dict[str, Any]] = []
    company_table = getattr(settings, "glide_company_table", "") or ""
    company_rowid_col = getattr(settings, "glide_company_rowid_column", "") or "$rowID"
    company_name_col = getattr(settings, "glide_company_name_column", "") or "Name"
    if company_table:
        company_rows = glide.list_table_rows(company_table)
    company_index = build_company_index(
        company_rows=company_rows,
        company_rowid_column=company_rowid_col,
        company_name_column=company_name_col,
    )

    # glide project index (optional)
    by_row_id: Dict[str, Dict[str, str]] = {}
    by_trip: Dict[Tuple[str, str], Dict[str, str]] = {}
    if project_spec.table_name:
        project_rows = glide.list_table_rows(project_spec.table_name)
        by_row_id, by_trip = build_project_indexes(
            project_rows=project_rows,
            tenant_id_column=project_spec.tenant_id_column,
            project_name_column=project_spec.project_name_column,
            part_number_column=project_spec.part_number_column,
            legacy_id_column=project_spec.legacy_id_column,
            rowid_column=project_spec.rowid_column,
        )

    # sheets project index
    sheet_by_trip, sheet_by_num = build_sheet_project_indexes(settings)

    row = glide.get_row_by_row_id(spec.table_name, row_id, rowid_column=spec.rowid_column)
    if not row:
        return {"ok": False, "error": f"Row not found. table={spec.table_name} row_id={row_id}"}

    return ingest_rows(
        settings,
        spec=spec,
        rows=[row],
        project_index_by_row_id=by_row_id,
        project_index_by_triplet=by_trip,
        sheet_project_by_triplet=sheet_by_trip,
        sheet_project_by_number=sheet_by_num,
        company_index_by_name=company_index,
        limit=0,
    )