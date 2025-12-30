from __future__ import annotations

from typing import Any, Dict, List, Optional
import hashlib
import re

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.drive_tool import DriveTool
from ...tools.attachment_tool import AttachmentResolver, split_cell_refs, ResolvedAttachment
from ...tools.vision_tool import VisionTool
from ...tools.annotate_tool import AnnotateTool
from ...tools.db_tool import DBTool


_IMG_EXT_RX = re.compile(r"\.(png|jpe?g|webp|bmp|tiff?)$", re.IGNORECASE)


def _sha256(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def _looks_like_media_ref(s: str) -> bool:
    """
    Accept URLs + Drive rel paths + image-looking names.
    We also allow PDFs via URL or rel path; we'll byte-sniff after download.
    """
    ss = (s or "").strip()
    if not ss:
        return False
    if ss.lower().startswith("http://") or ss.lower().startswith("https://"):
        return True
    if "/" in ss:
        return True
    return bool(_IMG_EXT_RX.search(ss)) or ss.lower().endswith(".pdf")


def _collect_photo_cells_from_additional_rows(rows: List[Dict[str, Any]]) -> List[str]:
    refs: List[str] = []
    for r in rows or []:
        for k, v in (r or {}).items():
            kk = (k or "").strip()
            if not kk:
                continue
            if kk.startswith("photo"):
                cell = _norm_value(v)
                if not cell:
                    continue
                for ref in split_cell_refs(cell):
                    if _looks_like_media_ref(ref):
                        refs.append(ref)
    return refs


def _sniff_mime(data: bytes) -> str:
    if not data:
        return ""
    b = data
    if b.startswith(b"%PDF"):
        return "application/pdf"
    if b[:3] == b"\xFF\xD8\xFF":
        return "image/jpeg"
    if b.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if b[:4] == b"RIFF" and b[8:12] == b"WEBP":
        return "image/webp"
    return ""


def _is_image_mime(m: str) -> bool:
    return (m or "").startswith("image/")


def analyze_media(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Checkin media ingestion (incremental-safe):
      - CheckIN inspection image URL
      - Conversation.Photo (NEW)
      - Additional photos sheet
    For each ref:
      - download bytes (supports Drive id + drive paths)
      - byte-sniff mime
      - Image => caption + defect detect (+ optional annotated upload)
      - PDF => store PDF_ATTACHMENT + add a "PDF:" entry into captions list
      - store artifacts idempotently (by source_hash)
      - append MEDIA OBSERVATIONS into thread_snapshot_text
    """
    allowed = {"CHECKIN_CREATED", "CHECKIN_UPDATED", "CONVERSATION_ADDED"}
    if (state.get("event_type") or "") not in allowed:
        (state.get("logs") or []).append(f"analyze_media: skipped (event_type not in {sorted(allowed)})")
        return state

    tenant_id = (state.get("tenant_id") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()
    run_id = (state.get("run_id") or "").strip()
    if not tenant_id or not checkin_id or not run_id:
        (state.get("logs") or []).append("analyze_media: skipped (missing tenant/checkin/run_id)")
        return state

    sheets = SheetsTool(settings)
    drive = DriveTool(settings)
    resolver = AttachmentResolver(drive)
    db = DBTool(settings.database_url)

    existing_caption_hashes = db.existing_artifact_source_hashes(
        tenant_id=tenant_id,
        checkin_id=checkin_id,
        artifact_type="IMAGE_CAPTION",
    )
    existing_pdf_hashes = db.existing_artifact_source_hashes(
        tenant_id=tenant_id,
        checkin_id=checkin_id,
        artifact_type="PDF_ATTACHMENT",
    )

    # CheckIN inspection image cell
    checkin_row = state.get("checkin_row") or {}
    col_img = sheets.map.col("checkin", "inspection_image_url")
    img_cell = _norm_value(checkin_row.get(_key(col_img), ""))
    main_refs = [r for r in split_cell_refs(img_cell) if _looks_like_media_ref(r)]

    # Conversation.Photo refs (NEW)
    convo_refs: List[str] = []
    try:
        col_convo_photo = sheets.map.col("conversation", "photos")
        k_convo_photo = _key(col_convo_photo)
        for cr in (state.get("conversation_rows") or [])[-50:]:
            cell = _norm_value((cr or {}).get(k_convo_photo, ""))
            for ref in split_cell_refs(cell):
                if _looks_like_media_ref(ref):
                    convo_refs.append(ref)
    except Exception as e:
        (state.get("logs") or []).append(f"analyze_media: conversation photo parse failed (non-fatal): {e}")

    # Additional photos sheet
    add_refs: List[str] = []
    try:
        add_sheet_id = getattr(settings, "additional_photos_spreadsheet_id", "") or ""
        add_tab = getattr(settings, "additional_photos_tab_name", "Checkin Additional photos")
        sheets_add = SheetsTool(settings, spreadsheet_id=add_sheet_id)
        add_rows = sheets_add.list_additional_photos_for_checkin(checkin_id, tab_name=add_tab)
        add_refs = _collect_photo_cells_from_additional_rows(add_rows)
    except Exception as e:
        (state.get("logs") or []).append(f"analyze_media: additional photos read failed (non-fatal): {e}")
        add_refs = []

    # Dedup + cap
    refs: List[str] = []
    seen = set()
    for r in (main_refs or []) + (convo_refs or []) + (add_refs or []):
        rr = (r or "").strip()
        if rr and rr not in seen:
            refs.append(rr)
            seen.add(rr)

    if not refs:
        (state.get("logs") or []).append("analyze_media: no media refs found")
        return state

    refs = refs[:12]

    if not getattr(settings, "vision_api_key", "").strip():
        (state.get("logs") or []).append("analyze_media: skipped (VISION_API_KEY not set)")
        return state

    vision = VisionTool(
        api_key=getattr(settings, "vision_api_key", ""),
        model=getattr(settings, "vision_model", "gemini-2.0-flash"),
    )
    annot = AnnotateTool()

    media_notes: List[str] = []
    new_captions: List[str] = []
    annotated_urls: List[str] = []

    context_hint = (
        f"Project={state.get('project_name') or ''} | Part={state.get('part_number') or ''} | "
        f"Checkin={checkin_id} | Status={state.get('checkin_status') or ''}\n"
        f"Description: {state.get('checkin_description') or ''}"
    ).strip()

    for ref in refs:
        att: Optional[ResolvedAttachment] = resolver.resolve(ref)
        if not att:
            continue

        data = resolver.fetch_bytes(att)
        if not data:
            continue

        source_hash = _sha256(data)

        mime = (att.mime_type or "").strip() or _sniff_mime(data) or "application/octet-stream"
        is_pdf = (mime == "application/pdf") or (att.name or "").lower().endswith(".pdf")
        is_img = _is_image_mime(mime)

        if is_pdf:
            if source_hash in existing_pdf_hashes:
                continue
            db.insert_artifact(
                run_id=run_id,
                artifact_type="PDF_ATTACHMENT",
                url=att.source_ref or att.rel_path or "unknown",
                meta={
                    "tenant_id": tenant_id,
                    "checkin_id": checkin_id,
                    "source_ref": att.source_ref,
                    "source_hash": source_hash,
                    "file_name": att.name,
                    "mime_type": mime,
                },
            )
            existing_pdf_hashes.add(source_hash)
            pdf_line = f"PDF: {att.name or 'attachment'} (no text extracted)"
            new_captions.append(pdf_line)
            media_notes.append(f"- Doc: {pdf_line}")
            continue

        if not is_img:
            continue

        if source_hash in existing_caption_hashes:
            continue

        caption = vision.caption_for_retrieval(
            image_bytes=data,
            mime_type=mime if mime.startswith("image/") else "image/jpeg",
            context_hint=context_hint,
        ).strip()

        db.insert_artifact(
            run_id=run_id,
            artifact_type="IMAGE_CAPTION",
            url=att.source_ref or att.rel_path or "unknown",
            meta={
                "tenant_id": tenant_id,
                "checkin_id": checkin_id,
                "source_ref": att.source_ref,
                "source_hash": source_hash,
                "file_name": att.name,
                "mime_type": mime,
                "caption": caption,
                "vision_model": getattr(settings, "vision_model", ""),
            },
        )
        existing_caption_hashes.add(source_hash)

        if caption:
            new_captions.append(caption)

        dout = vision.detect_defects(
            image_bytes=data,
            mime_type=mime if mime.startswith("image/") else "image/jpeg",
            context_hint=context_hint,
        )
        defects = dout.get("defects") or []
        if not isinstance(defects, list):
            defects = []

        if defects:
            annotated = annot.draw(data, defects)
            fname = f"{source_hash[:10]}_{(att.name or 'image').replace(' ', '_')}.png"
            up = drive.upload_annotated_bytes(
                checkin_id=checkin_id,
                file_name=fname,
                content_bytes=annotated,
                mime_type="image/png",
                make_public=True,
            )
            url = up.get("webViewLink") or up.get("webContentLink") or ""
            if url:
                db.insert_artifact(
                    run_id=run_id,
                    artifact_type="ANNOTATED_IMAGE",
                    url=url,
                    meta={
                        "tenant_id": tenant_id,
                        "checkin_id": checkin_id,
                        "source_ref": att.source_ref,
                        "source_hash": source_hash,
                        "caption": caption,
                        "defects": defects,
                        "vision_model": getattr(settings, "vision_model", ""),
                    },
                )
                annotated_urls.append(url)

        if caption:
            if defects:
                labels = ", ".join([str(d.get("label") or "defect") for d in defects[:5]])
                media_notes.append(f"- Image: {caption} | defects: {labels}")
            else:
                media_notes.append(f"- Image: {caption} | defects: none obvious")

    if annotated_urls:
        state["annotated_image_urls"] = annotated_urls
    if new_captions:
        state["image_captions"] = new_captions

    if media_notes:
        note_block = "MEDIA OBSERVATIONS:\n" + "\n".join(media_notes)
        state["media_notes"] = note_block
        snap = (state.get("thread_snapshot_text") or "").strip()
        state["thread_snapshot_text"] = (snap + "\n\n" + note_block).strip()

    (state.get("logs") or []).append(
        f"analyze_media: done refs={len(refs)} captions={len([c for c in new_captions if not str(c).startswith('PDF:')])} pdfs={len([c for c in new_captions if str(c).startswith('PDF:')])} annotated={len(annotated_urls)}"
    )
    return state
