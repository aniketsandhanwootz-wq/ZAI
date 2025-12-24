from __future__ import annotations

from typing import Dict, Any, Tuple, List
from io import BytesIO

from pypdf import PdfReader

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.embed_tool import EmbedTool
from ...tools.llm_tool import LLMTool
from ...tools.vector_tool import VectorTool
from ...tools.drive_tool import DriveTool
from ...tools.attachment_tool import AttachmentResolver, split_cell_refs
from . import utils as ingest_utils


def _extract_pdf_text_from_bytes(data: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(data))
        out = []
        for page in reader.pages:
            out.append(page.extract_text() or "")
        return "\n".join(out).strip()
    except Exception:
        return ""


def ingest_ccp(settings: Settings) -> Dict[str, Any]:
    sheets = SheetsTool(settings)
    embedder = EmbedTool(settings)
    llm = LLMTool(settings)
    vec = VectorTool(settings)

    drive = DriveTool(settings)
    resolver = AttachmentResolver(drive)

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

    project_by_id: Dict[str, Dict[str, str]] = {}
    project_by_tuple: Dict[Tuple[str, str, str], str] = {}

    for pr in projects:
        legacy_id = _norm_value(pr.get(k_pid, ""))
        tenant_id = _norm_value(pr.get(k_tenant, ""))
        pname = _norm_value(pr.get(k_pname, ""))
        pnum = _norm_value(pr.get(k_pnum, ""))

        if legacy_id:
            project_by_id[_key(legacy_id)] = {
                "tenant_id": tenant_id,
                "project_name": pname,
                "part_number": pnum,
                "legacy_id": legacy_id,
            }

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
    col_photos = sheets.map.col("ccp", "photos")
    col_main = sheets.map.col("ccp", "main_image")

    k_ccp_id = _key(col_ccp_id)
    k_legacy_id = _key(col_legacy_id)
    k_ccp_name = _key(col_ccp_name)
    k_desc = _key(col_desc)
    k_proj = _key(col_proj)
    k_part = _key(col_part)
    k_files = _key(col_files)
    k_photos = _key(col_photos)
    k_main = _key(col_main)

    # Metrics
    rows_ingested = 0
    chunks_embedded = 0
    pdf_text_chunks = 0
    image_caption_chunks = 0

    missing_ccp_id = 0
    missing_legacy_id = 0
    missing_project_match = 0
    missing_tenant = 0
    embed_errors = 0

    resolved_files = 0
    unresolved_files = 0
    skipped_existing = 0

    for r in rows:
        ccp_id = _norm_value(r.get(k_ccp_id, ""))
        if not ccp_id:
            missing_ccp_id += 1
            continue

        legacy_id = _norm_value(r.get(k_legacy_id, ""))
        if not legacy_id:
            missing_legacy_id += 1
            continue

        ccp_name = _norm_value(r.get(k_ccp_name, ""))
        desc = _norm_value(r.get(k_desc, ""))

        ccp_project_name = _norm_value(r.get(k_proj, ""))
        ccp_part_number = _norm_value(r.get(k_part, ""))

        # tenant resolution (ID-first)
        pr = project_by_id.get(_key(legacy_id))
        tenant_id = (pr or {}).get("tenant_id", "").strip()

        if not tenant_id and ccp_project_name and ccp_part_number:
            tenant_id = project_by_tuple.get((_key(ccp_project_name), _key(ccp_part_number), _key(legacy_id)), "")

        if not pr and not tenant_id:
            missing_project_match += 1
            continue
        if not tenant_id:
            missing_tenant += 1
            continue

        project_name = (pr or {}).get("project_name", "") or ccp_project_name
        part_number = (pr or {}).get("part_number", "") or ccp_part_number

        rows_ingested += 1

        # 1) CCP description chunks (incremental via content_hash)
        if desc:
            chunks = ingest_utils.chunk_text(f"CCP: {ccp_name}\n{desc}")
            for ch in chunks:
                content_hash = None  # VectorTool will compute if None
                # pre-check by hash requires same hash logic -> we keep it simple:
                # compute same as VectorTool default:
                import hashlib
                content_hash = hashlib.sha256(f"{ccp_id}|CCP_DESC|{ch}".encode("utf-8")).hexdigest()
                if vec.ccp_hash_exists(tenant_id=tenant_id, ccp_id=ccp_id, chunk_type="CCP_DESC", content_hash=content_hash):
                    skipped_existing += 1
                    continue
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
                        content_hash=content_hash,
                    )
                    chunks_embedded += 1
                except Exception:
                    embed_errors += 1

        # 2) Attachments: Files + Photos + Main image
        files_val = _norm_value(r.get(k_files, ""))
        photos_val = _norm_value(r.get(k_photos, ""))
        main_val = _norm_value(r.get(k_main, ""))

        all_refs: List[str] = []
        all_refs.extend(split_cell_refs(files_val))
        all_refs.extend(split_cell_refs(photos_val))
        all_refs.extend(split_cell_refs(main_val))

        # keep sane
        all_refs = all_refs[:50]

        for ref in all_refs:
            att = resolver.resolve(ref)
            if not att:
                continue

            # skip non pdf/image quickly
            if not (att.is_pdf or att.is_image):
                continue

            # --- PDFs ---
            if att.is_pdf:
                data = resolver.fetch_bytes(att)
                if not data:
                    unresolved_files += 1
                    continue

                resolved_files += 1
                text = _extract_pdf_text_from_bytes(data)
                if not text:
                    continue

                for ch in ingest_utils.chunk_text(text):
                    import hashlib
                    content_hash = hashlib.sha256(f"{ccp_id}|PDF_TEXT|{ch}".encode("utf-8")).hexdigest()
                    if vec.ccp_hash_exists(tenant_id=tenant_id, ccp_id=ccp_id, chunk_type="PDF_TEXT", content_hash=content_hash):
                        skipped_existing += 1
                        continue
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
                            source_ref=att.source_ref,
                            embedding=emb,
                            content_hash=content_hash,
                        )
                        chunks_embedded += 1
                        pdf_text_chunks += 1
                    except Exception:
                        embed_errors += 1
                continue

            # --- Images ---
            if att.is_image:
                data = resolver.fetch_bytes(att)
                if not data:
                    unresolved_files += 1
                    continue

                resolved_files += 1

                # Stable-ish caption context
                context = f"CCP Name: {ccp_name}\nProject: {project_name}\nPart: {part_number}\nSourceRef: {att.source_ref}"

                caption = llm.caption_image(image_bytes=data, mime_type=att.mime_type or "image/jpeg", context=context)
                caption = (caption or "").strip()
                if not caption:
                    continue

                # make chunk_text deterministic & tied to the file identity (so re-ingestion stays stable)
                file_key = att.drive_file_id or att.name or att.source_ref
                chunk_text = f"[CCP_IMAGE]\nFILE: {file_key}\n{caption}".strip()

                import hashlib
                content_hash = hashlib.sha256(f"{ccp_id}|IMG_CAPTION|{file_key}".encode("utf-8")).hexdigest()
                if vec.ccp_hash_exists(tenant_id=tenant_id, ccp_id=ccp_id, chunk_type="IMG_CAPTION", content_hash=content_hash):
                    skipped_existing += 1
                    continue

                try:
                    emb = embedder.embed_text(chunk_text)
                    vec.upsert_ccp_chunk(
                        tenant_id=tenant_id,
                        ccp_id=ccp_id,
                        ccp_name=ccp_name,
                        project_name=project_name,
                        part_number=part_number,
                        legacy_id=legacy_id,
                        chunk_type="IMG_CAPTION",
                        chunk_text=chunk_text,
                        source_ref=att.source_ref,
                        embedding=emb,
                        content_hash=content_hash,
                    )
                    chunks_embedded += 1
                    image_caption_chunks += 1
                except Exception:
                    embed_errors += 1

    skipped_rows = total_rows - rows_ingested

    return {
        "source": "ccp",
        "rows_seen": rows_ingested,
        "chunks_embedded": chunks_embedded,
        "pdf_text_chunks": pdf_text_chunks,
        "image_caption_chunks": image_caption_chunks,
        "skipped_rows": skipped_rows,
        "missing_ccp_id": missing_ccp_id,
        "missing_legacy_id": missing_legacy_id,
        "missing_project_match": missing_project_match,
        "missing_tenant": missing_tenant,
        "resolved_files": resolved_files,
        "unresolved_files": unresolved_files,
        "skipped_existing": skipped_existing,
        "embed_errors": embed_errors,
        "note": "Now resolves Drive paths under GOOGLE_DRIVE_ROOT_FOLDER_ID and captions images; skips already-ingested chunks via content_hash.",
    }
