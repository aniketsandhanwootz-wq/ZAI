from __future__ import annotations

from typing import Any, Dict, List
import hashlib
import re

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.drive_tool import DriveTool
from ...tools.attachment_tool import AttachmentResolver, split_cell_refs
from ...tools.vision_tool import VisionTool
from ...tools.annotate_tool import AnnotateTool
from ...tools.db_tool import DBTool


_IMG_EXT_RX = re.compile(r"\.(png|jpe?g|webp|bmp|tiff?)$", re.IGNORECASE)


def _sha256(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def _looks_like_image_ref(s: str) -> bool:
    """
    Additional Photos sheet me "Photo" column often contains labels (not paths).
    We only accept:
      - URLs
      - strings containing "/" (Drive rel path)
      - strings ending with image extensions
    """
    ss = (s or "").strip()
    if not ss:
        return False
    if ss.lower().startswith("http://") or ss.lower().startswith("https://"):
        return True
    if "/" in ss:
        return True
    return bool(_IMG_EXT_RX.search(ss))


def _collect_photo_cells_from_additional_rows(rows: List[Dict[str, Any]]) -> List[str]:
    """
    Rows are dicts keyed by casefold headers.
    We'll pick columns starting with "photo" but filter only image-like refs.
    """
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
                    if _looks_like_image_ref(ref):
                        refs.append(ref)
    return refs


def analyze_media(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    CHECKIN images + Additional photos:
      - resolve to bytes (Drive prefix map supported)
      - caption + defect detect
      - store artifacts idempotently (by source_hash)
      - append MEDIA OBSERVATIONS into thread_snapshot_text
    """
    # If you want this for more event types later, relax this check.
    if (state.get("event_type") or "") != "CHECKIN_CREATED":
        (state.get("logs") or []).append("analyze_media: skipped (not CHECKIN_CREATED)")
        return state

    tenant_id = (state.get("tenant_id") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()
    run_id = (state.get("run_id") or "").strip()
    if not tenant_id or not checkin_id or not run_id:
        (state.get("logs") or []).append("analyze_media: skipped (missing tenant/checkin/run_id)")
        return state

    if not getattr(settings, "vision_api_key", "").strip():
        (state.get("logs") or []).append("analyze_media: skipped (VISION_API_KEY not set)")
        return state

    sheets = SheetsTool(settings)
    drive = DriveTool(settings)
    resolver = AttachmentResolver(drive)
    db = DBTool(settings.database_url)

    # Prefetch existing caption hashes for idempotency
    existing_caption_hashes = db.existing_artifact_source_hashes(
        tenant_id=tenant_id,
        checkin_id=checkin_id,
        artifact_type="IMAGE_CAPTION",
    )

    # 1) main checkin image cell
    checkin_row = state.get("checkin_row") or {}
    col_img = sheets.map.col("checkin", "inspection_image_url")
    img_cell = _norm_value(checkin_row.get(_key(col_img), ""))
    main_refs = [r for r in split_cell_refs(img_cell) if _looks_like_image_ref(r)]

    # 2) additional photos from separate spreadsheet/tab
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
    refs = []
    seen = set()
    for r in (main_refs or []) + (add_refs or []):
        rr = (r or "").strip()
        if rr and rr not in seen:
            refs.append(rr)
            seen.add(rr)

    if not refs:
        (state.get("logs") or []).append("analyze_media: no image refs (main + additional empty)")
        return state

    refs = refs[:12]

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
        att = resolver.resolve(ref)
        if not att:
            continue

        # process only images
        if not getattr(att, "is_image", False):
            continue

        data = resolver.fetch_bytes(att)
        if not data:
            continue

        source_hash = _sha256(data)

        # Caption idempotency per image (across runs)
        if source_hash in existing_caption_hashes:
            continue

        mime = (att.mime_type or "").strip() or "image/jpeg"

        # 1) caption
        caption = vision.caption_for_retrieval(
            image_bytes=data,
            mime_type=mime,
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

        # 2) defect detect + annotated upload (optional)
        dout = vision.detect_defects(
            image_bytes=data,
            mime_type=mime,
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

        # snapshot notes
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
        f"analyze_media: done refs={len(refs)} captions={len(new_captions)} annotated={len(annotated_urls)}"
    )
    return state
