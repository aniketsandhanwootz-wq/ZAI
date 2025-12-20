from __future__ import annotations

from typing import Dict, Any, Tuple
from io import BytesIO

import requests
from pypdf import PdfReader

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool
from .utils import chunk_text


def _try_extract_pdf_text_from_url(url: str, timeout: int = 30) -> str:
    """
    MVP: only works if URL is directly downloadable.
    (If AppSheet gives Drive links or relative paths, we'll add Drive download later.)
    """
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        reader = PdfReader(BytesIO(r.content))
        out = []
        for page in reader.pages:
            out.append(page.extract_text() or "")
        return "\n".join(out).strip()
    except Exception:
        return ""


def ingest_ccp(settings: Settings) -> Dict[str, Any]:
    sheets = SheetsTool(settings)
    embedder = EmbedTool(settings)
    vec = VectorTool(settings)

    # ---------------------------
    # Build Project indexes ONCE
    # ---------------------------
    projects = sheets.list_projects()

    col_pid = sheets.map.col("project", "legacy_id")        # "ID"
    col_tenant = sheets.map.col("project", "company_row_id")  # "Company row id"
    col_pname = sheets.map.col("project", "project_name")   # "Project name"
    col_pnum = sheets.map.col("project", "part_number")     # "Part number"

    k_pid = _key(col_pid)
    k_tenant = _key(col_tenant)
    k_pname = _key(col_pname)
    k_pnum = _key(col_pnum)

    # ID-first index: legacy_id -> project info
    project_by_id: Dict[str, Dict[str, str]] = {}

    # Optional fallback: (project_name, part_number, legacy_id) -> tenant_id
    project_by_tuple: Dict[Tuple[str, str, str], str] = {}

    for pr in projects:
        legacy_id = _norm_value(pr.get(k_pid, ""))
        tenant_id = _norm_value(pr.get(k_tenant, ""))
        pname = _norm_value(pr.get(k_pname, ""))
        pnum = _norm_value(pr.get(k_pnum, ""))

        if legacy_id:
            # store canonical project info (even if some fields are empty)
            project_by_id[_key(legacy_id)] = {
                "tenant_id": tenant_id,
                "project_name": pname,
                "part_number": pnum,
                "legacy_id": legacy_id,
            }

        # fallback tuple join needs all 3
        if legacy_id and pname and pnum and tenant_id:
            project_by_tuple[(_key(pname), _key(pnum), _key(legacy_id))] = tenant_id

    # ---------------------------
    # Load CCP rows
    # ---------------------------
    rows = sheets.list_ccp()
    total_rows = len(rows)

    # CCP columns from mapping
    col_ccp_id = sheets.map.col("ccp", "ccp_id")
    col_legacy_id = sheets.map.col("ccp", "legacy_id")
    col_ccp_name = sheets.map.col("ccp", "ccp_name")
    col_desc = sheets.map.col("ccp", "description")
    col_proj = sheets.map.col("ccp", "project_name")
    col_part = sheets.map.col("ccp", "part_number")
    col_files = sheets.map.col("ccp", "files")

    k_ccp_id = _key(col_ccp_id)
    k_legacy_id = _key(col_legacy_id)
    k_ccp_name = _key(col_ccp_name)
    k_desc = _key(col_desc)
    k_proj = _key(col_proj)
    k_part = _key(col_part)
    k_files = _key(col_files)

    # Metrics
    rows_ingested = 0
    chunks_embedded = 0
    pdf_text_chunks = 0

    missing_ccp_id = 0
    missing_legacy_id = 0
    missing_project_match = 0
    missing_tenant = 0
    embed_errors = 0

    for r in rows:
        ccp_id = _norm_value(r.get(k_ccp_id, ""))
        if not ccp_id:
            missing_ccp_id += 1
            continue

        legacy_id = _norm_value(r.get(k_legacy_id, ""))
        if not legacy_id:
            # ID is the stable join key for tenant isolation
            missing_legacy_id += 1
            continue

        ccp_name = _norm_value(r.get(k_ccp_name, ""))
        desc = _norm_value(r.get(k_desc, ""))

        # CCP sheet fields may be blank; we will fill from Project row if needed
        ccp_project_name = _norm_value(r.get(k_proj, ""))
        ccp_part_number = _norm_value(r.get(k_part, ""))

        # ---------------------------
        # Resolve tenant via ID-first
        # ---------------------------
        pr = project_by_id.get(_key(legacy_id))
        tenant_id = (pr or {}).get("tenant_id", "").strip()

        # If ID-first failed, try tuple fallback (sometimes ID exists but formatting differs)
        if not tenant_id and ccp_project_name and ccp_part_number:
            tenant_id = project_by_tuple.get((_key(ccp_project_name), _key(ccp_part_number), _key(legacy_id)), "")

        if not pr and not tenant_id:
            missing_project_match += 1
            continue

        if not tenant_id:
            missing_tenant += 1
            continue

        # Canonical metadata (prefer Project table for consistency)
        project_name = (pr or {}).get("project_name", "") or ccp_project_name
        part_number = (pr or {}).get("part_number", "") or ccp_part_number

        rows_ingested += 1

        # 1) CCP description chunks
        if desc:
            chunks = chunk_text(f"CCP: {ccp_name}\n{desc}")
            for ch in chunks:
                try:
                    emb = embedder.embed_text(ch)
                    vec.upsert_ccp_chunk(
                        tenant_id=tenant_id,
                        ccp_id=ccp_id,
                        ccp_name=ccp_name,
                        project_name=project_name,
                        part_number=part_number,
                        legacy_id=legacy_id,
                        chunk_type="CCP_DESC",
                        chunk_text=ch,
                        source_ref="",
                        embedding=emb,
                    )
                    chunks_embedded += 1
                except Exception:
                    embed_errors += 1

        # 2) CCP PDFs (direct URLs only)
        files_val = _norm_value(r.get(k_files, ""))
        if files_val:
            file_urls = [x.strip() for x in files_val.replace("\n", ",").split(",") if x.strip()]
            for fu in file_urls:
                if fu.lower().endswith(".pdf") and fu.startswith("http"):
                    text = _try_extract_pdf_text_from_url(fu)
                    if not text:
                        continue

                    for ch in chunk_text(text):
                        try:
                            emb = embedder.embed_text(ch)
                            vec.upsert_ccp_chunk(
                                tenant_id=tenant_id,
                                ccp_id=ccp_id,
                                ccp_name=ccp_name,
                                project_name=project_name,
                                part_number=part_number,
                                legacy_id=legacy_id,
                                chunk_type="PDF_TEXT",
                                chunk_text=ch,
                                source_ref=fu,
                                embedding=emb,
                            )
                            chunks_embedded += 1
                            pdf_text_chunks += 1
                        except Exception:
                            embed_errors += 1

    skipped_rows = total_rows - rows_ingested

    return {
        "source": "ccp",
        "rows_seen": rows_ingested,
        "chunks_embedded": chunks_embedded,
        "skipped_rows": skipped_rows,
        "missing_ccp_id": missing_ccp_id,
        "missing_legacy_id": missing_legacy_id,
        "missing_project_match": missing_project_match,
        "missing_tenant": missing_tenant,
        "pdf_text_chunks": pdf_text_chunks,
        "embed_errors": embed_errors,
        "note": "PDF extraction works only for direct-download URLs in MVP.",
    }
