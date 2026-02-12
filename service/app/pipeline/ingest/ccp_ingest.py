from __future__ import annotations

from typing import Dict, Any, Tuple, List, Optional
from io import BytesIO
import re

from ...tools.file_extractors.pdf_extractor import extract_pdf as robust_extract_pdf
from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool
from ...tools.drive_tool import DriveTool
from ...tools.attachment_tool import AttachmentResolver, split_cell_refs
from ...tools.vision_tool import VisionTool
from . import utils as ingest_utils


def _extract_pdf_text_from_bytes(*, filename: str, data: bytes, vision: VisionTool) -> str:
    """
    Uses the shared robust PDF extractor:
      - page sampling (head+tail+middle)
      - OCR fallback for scanned/table-like pages (VisionTool in OCR_MODE)
    """
    try:
        res = robust_extract_pdf(
            filename=filename or "ccp.pdf",
            data=data,
            max_pages=40,
            max_chars=140000,
            vision_caption_fn=vision.caption_image,  # signature matches: (image_bytes, mime_type, context)
        )
        return (res.extracted_text or "").strip()
    except Exception:
        return ""


def _norm_text(s: str) -> str:
    s = (s or "").replace("\r", "\n")
    s = "\n".join([ln.strip() for ln in s.split("\n") if ln.strip()])
    s = re.sub(r"[ \t]{2,}", " ", s)
    return s.strip()


def _is_pdf_bytes(b: bytes) -> bool:
    return (b or b"")[:5] == b"%PDF-"


def _sniff_image_mime(b: bytes) -> Optional[str]:
    bb = b or b""
    if bb.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if bb.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if bb[:4] == b"RIFF" and bb[8:12] == b"WEBP":
        return "image/webp"
    return None


def _build_project_indexes(sheets: SheetsTool) -> tuple[Dict[str, Dict[str, str]], Dict[Tuple[str, str, str], str]]:
    projects = sheets.list_projects()

    col_pid = sheets.map.col("project", "legacy_id")
    col_tenant = sheets.map.col("project", "company_row_id")
    col_pname = sheets.map.col("project", "project_name")
    col_pnum = sheets.map.col("project", "part_number")

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

    return project_by_id, project_by_tuple


def _ingest_one_ccp_row(
    *,
    settings: Settings,
    sheets: SheetsTool,
    embedder: EmbedTool,
    vec: VectorTool,
    resolver: AttachmentResolver,
    vision: VisionTool,
    project_by_id: Dict[str, Dict[str, str]],
    project_by_tuple: Dict[Tuple[str, str, str], str],
    row: Dict[str, Any],
) -> Dict[str, int]:
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

    ccp_id = _norm_value(row.get(k_ccp_id, ""))
    legacy_id = _norm_value(row.get(k_legacy_id, ""))
    ccp_name = _norm_value(row.get(k_ccp_name, ""))
    desc = _norm_value(row.get(k_desc, ""))

    ccp_project_name = _norm_value(row.get(k_proj, ""))
    ccp_part_number = _norm_value(row.get(k_part, ""))

    if not ccp_id or not legacy_id:
        return {"chunks_embedded": 0, "pdf_text_chunks": 0, "image_caption_chunks": 0, "resolved_files": 0, "unresolved_files": 0, "skipped_existing": 0, "embed_errors": 0}

    # tenant resolution (ID-first)
    pr = project_by_id.get(_key(legacy_id))
    tenant_id = (pr or {}).get("tenant_id", "").strip()

    if not tenant_id and ccp_project_name and ccp_part_number:
        tenant_id = project_by_tuple.get((_key(ccp_project_name), _key(ccp_part_number), _key(legacy_id)), "")

    if not tenant_id:
        return {"chunks_embedded": 0, "pdf_text_chunks": 0, "image_caption_chunks": 0, "resolved_files": 0, "unresolved_files": 0, "skipped_existing": 0, "embed_errors": 0}

    project_name = (pr or {}).get("project_name", "") or ccp_project_name
    part_number = (pr or {}).get("part_number", "") or ccp_part_number

    chunks_embedded = 0
    pdf_text_chunks = 0
    image_caption_chunks = 0
    resolved_files = 0
    unresolved_files = 0
    skipped_existing = 0
    embed_errors = 0

    # 1) CCP description chunks
    if desc:
        chunks = ingest_utils.chunk_text(_norm_text(f"CCP: {ccp_name}\n{desc}"))
        for ch in chunks:
            content_hash = vec.make_ccp_content_hash(ccp_id=ccp_id, chunk_type="CCP_DESC", stable_key="DESC", chunk_text=ch)
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

    # 2) Attachments
    files_val = _norm_value(row.get(k_files, ""))
    photos_val = _norm_value(row.get(k_photos, ""))
    main_val = _norm_value(row.get(k_main, ""))

    all_refs: List[str] = []
    all_refs.extend(split_cell_refs(files_val))
    all_refs.extend(split_cell_refs(photos_val))
    all_refs.extend(split_cell_refs(main_val))
    all_refs = all_refs[:50]

    for ref in all_refs:
        att = resolver.resolve(ref)
        if not att:
            continue

        data = resolver.fetch_bytes(att)
        if not data:
            unresolved_files += 1
            continue

        resolved_files += 1
        file_hash = vec.hash_bytes(data)

        is_pdf = att.is_pdf or _is_pdf_bytes(data)
        img_mime = att.mime_type or _sniff_image_mime(data)
        is_img = att.is_image or bool(img_mime and img_mime.startswith("image/"))

        if is_pdf:
            text = _extract_pdf_text_from_bytes(data)
            text = _norm_text(text)
            if not text:
                continue

            for ch in ingest_utils.chunk_text(text):
                content_hash = vec.make_ccp_content_hash(ccp_id=ccp_id, chunk_type="PDF_TEXT", stable_key=file_hash, chunk_text=ch)
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

        if is_img:
            context = (
                f"CCP Name: {ccp_name}\n"
                f"Project: {project_name}\n"
                f"Part: {part_number}\n"
                f"SourceRef: {att.source_ref}\n"
                f"FileHash: {file_hash}"
            ).strip()

            content_hash = vec.make_ccp_content_hash(ccp_id=ccp_id, chunk_type="IMG_CAPTION", stable_key=file_hash, chunk_text="")
            if vec.ccp_hash_exists(tenant_id=tenant_id, ccp_id=ccp_id, chunk_type="IMG_CAPTION", content_hash=content_hash):
                skipped_existing += 1
                continue

            try:
                mime = img_mime or "image/jpeg"
                caption = vision.caption_image(image_bytes=data, mime_type=mime, context=context)
                caption = _norm_text(caption or "")
                if not caption:
                    continue

                chunk_text = f"[CCP_IMAGE]\nFILE_HASH: {file_hash}\n{caption}".strip()
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

    return {
        "chunks_embedded": chunks_embedded,
        "pdf_text_chunks": pdf_text_chunks,
        "image_caption_chunks": image_caption_chunks,
        "resolved_files": resolved_files,
        "unresolved_files": unresolved_files,
        "skipped_existing": skipped_existing,
        "embed_errors": embed_errors,
    }


def ingest_ccp(settings: Settings) -> Dict[str, Any]:
    sheets = SheetsTool(settings)
    embedder = EmbedTool(settings)
    vec = VectorTool(settings)

    drive = DriveTool(settings)
    resolver = AttachmentResolver(drive)
    vision = VisionTool(settings)

    project_by_id, project_by_tuple = _build_project_indexes(sheets)

    rows = sheets.list_ccp()
    total_rows = len(rows)

    col_ccp_id = sheets.map.col("ccp", "ccp_id")
    col_legacy_id = sheets.map.col("ccp", "legacy_id")
    k_ccp_id = _key(col_ccp_id)
    k_legacy_id = _key(col_legacy_id)

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

        # quick tenant resolvability check (so metrics match earlier)
        pr = project_by_id.get(_key(legacy_id))
        tenant_id = (pr or {}).get("tenant_id", "").strip()
        if not tenant_id:
            missing_project_match += 1
            continue

        rows_ingested += 1
        out = _ingest_one_ccp_row(
            settings=settings,
            sheets=sheets,
            embedder=embedder,
            vec=vec,
            resolver=resolver,
            vision=vision,
            project_by_id=project_by_id,
            project_by_tuple=project_by_tuple,
            row=r,
        )
        chunks_embedded += out["chunks_embedded"]
        pdf_text_chunks += out["pdf_text_chunks"]
        image_caption_chunks += out["image_caption_chunks"]
        resolved_files += out["resolved_files"]
        unresolved_files += out["unresolved_files"]
        skipped_existing += out["skipped_existing"]
        embed_errors += out["embed_errors"]

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
        "note": "Hardened: byte-sniff PDF/image + file-hash idempotency + stable hashes; captions via VisionTool; safe to re-run anytime.",
    }


def ingest_ccp_one(settings: Settings, *, ccp_id: str) -> Dict[str, Any]:
    """
    Incremental CCP ingestion: ingest only the CCP row with this CCP ID.
    Called by event_type=CCP_UPDATED.
    """
    target = (ccp_id or "").strip()
    if not target:
        return {"ok": False, "error": "missing ccp_id"}

    sheets = SheetsTool(settings)
    embedder = EmbedTool(settings)
    vec = VectorTool(settings)

    drive = DriveTool(settings)
    resolver = AttachmentResolver(drive)
    vision = VisionTool(settings)

    project_by_id, project_by_tuple = _build_project_indexes(sheets)

    rows = sheets.list_ccp()
    col_ccp_id = sheets.map.col("ccp", "ccp_id")
    k_ccp_id = _key(col_ccp_id)

    hit = None
    for r in rows:
        if _norm_value((r or {}).get(k_ccp_id, "")) == target:
            hit = r
            break

    if not hit:
        return {"ok": True, "skipped": True, "reason": f"ccp_id '{target}' not found in sheet"}

    out = _ingest_one_ccp_row(
        settings=settings,
        sheets=sheets,
        embedder=embedder,
        vec=vec,
        resolver=resolver,
        vision=vision,
        project_by_id=project_by_id,
        project_by_tuple=project_by_tuple,
        row=hit,
    )

    return {"ok": True, "ccp_id": target, **out}
