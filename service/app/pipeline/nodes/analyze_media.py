from __future__ import annotations

from typing import Any, Dict, List
import hashlib

from ...config import Settings
from ...tools.sheets_tool import SheetsTool, _key, _norm_value
from ...tools.drive_tool import DriveTool
from ...tools.attachment_tool import AttachmentResolver, split_cell_refs
from ...tools.vision_tool import VisionTool
from ...tools.annotate_tool import AnnotateTool
from ...tools.db_tool import DBTool


def _sha256(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def analyze_media(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    # New checkins only
    if (state.get("event_type") or "") != "CHECKIN_CREATED":
        (state.get("logs") or []).append("analyze_media: skipped (not CHECKIN_CREATED)")
        return state

    tenant_id = (state.get("tenant_id") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()
    run_id = (state.get("run_id") or "").strip()
    if not tenant_id or not checkin_id or not run_id:
        (state.get("logs") or []).append("analyze_media: skipped (missing tenant/checkin/run_id)")
        return state

    if not getattr(settings, "google_drive_root_folder_id", "").strip():
        (state.get("logs") or []).append("analyze_media: skipped (GOOGLE_DRIVE_ROOT_FOLDER_ID not set)")
        return state

    if not getattr(settings, "vision_api_key", "").strip():
        (state.get("logs") or []).append("analyze_media: skipped (VISION_API_KEY not set)")
        return state

    sheets = SheetsTool(settings)
    drive = DriveTool(settings)
    resolver = AttachmentResolver(drive)
    db = DBTool(settings.database_url)

    # Pull checkin image cell (current mapping: inspection_image_url)
    checkin_row = state.get("checkin_row") or {}
    col_img = sheets.map.col("checkin", "inspection_image_url")
    img_cell = _norm_value(checkin_row.get(_key(col_img), ""))

    refs = split_cell_refs(img_cell)
    if not refs:
        (state.get("logs") or []).append("analyze_media: no image refs in checkin")
        return state

    # Keep it bounded (avoid long runs)
    refs = refs[:5]

    vision = VisionTool(
        api_key=getattr(settings, "vision_api_key", ""),
        model=getattr(settings, "vision_model", "gemini-2.0-flash"),
    )
    annot = AnnotateTool()

    annotated_urls: List[str] = []
    media_notes: List[str] = []

    context_hint = (
        f"Project={state.get('project_name') or ''} | Part={state.get('part_number') or ''} | "
        f"Checkin={checkin_id} | Status={state.get('checkin_status') or ''}\n"
        f"Description: {state.get('checkin_description') or ''}"
    ).strip()

    for ref in refs:
        att = resolver.resolve(ref)
        if not att:
            continue

        # only images
        if not att.is_image and not (att.mime_type or "").startswith("image/"):
            # try anyway if bytes are an image (some Drive items may miss mime)
            pass

        data = resolver.fetch_bytes(att)
        if not data:
            continue

        source_hash = _sha256(data)

        # idempotency: already annotated?
        # idempotency: already processed? (caption or annotated)
        if db.artifact_exists(
            tenant_id=tenant_id,
            checkin_id=checkin_id,
            artifact_type="IMAGE_CAPTION",
            source_hash=source_hash,
        ) or db.artifact_exists(
            tenant_id=tenant_id,
            checkin_id=checkin_id,
            artifact_type="ANNOTATED_IMAGE",
            source_hash=source_hash,
        ):
            (state.get("logs") or []).append(f"analyze_media: already processed source_hash={source_hash[:12]}")
            continue

        # guess mime if missing
        mime = (att.mime_type or "").strip() or "image/jpeg"

        # 1) vision → caption + boxes
        # 1) caption (for retrieval/embedding)
        caption = vision.caption_for_retrieval(
            image_bytes=data,
            mime_type=mime,
            context_hint=context_hint,
        ).strip()

        # 2) defects (for boxes/annotation)
        dout = vision.detect_defects(
            image_bytes=data,
            mime_type=mime,
            context_hint=context_hint,
        )
        defects = dout.get("defects") or []
        if not isinstance(defects, list):
            defects = []

        # 2) annotate
        # 2) annotate+upload ONLY if defects exist
        if not defects:
            # still keep caption for retrieval notes; skip upload
            if caption:
                media_notes.append(f"- Image: {caption} | defects: none obvious")
            continue

        annotated = annot.draw(data, defects)

        # 3) upload to Drive: Annotated/<checkin_id>/...
        fname = f"{source_hash[:10]}_{(att.name or 'image').replace(' ', '_')}.png"
        folder_parts = ["Annotated", checkin_id]
        up = drive.upload_annotated_bytes(
            checkin_id=checkin_id,
            file_name=fname,
            content_bytes=annotated,
            mime_type="image/png",
            make_public=True,
        )
        url = up.get("webViewLink") or up.get("webContentLink") or ""
        if not url:
            continue

        # 4) persist artifact
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

        # keep notes compact for retrieval
        if caption:
            if defects:
                labels = ", ".join([str(d.get("label") or "defect") for d in defects[:5]])
                media_notes.append(f"- Image: {caption} | defects: {labels}")
            else:
                media_notes.append(f"- Image: {caption} | defects: none obvious")

    if annotated_urls:
        state["annotated_image_urls"] = annotated_urls

    if media_notes:
        note_block = "MEDIA OBSERVATIONS:\n" + "\n".join(media_notes)
        state["media_notes"] = note_block

        # append into snapshot so embeddings + retrieval improve
        snap = (state.get("thread_snapshot_text") or "").strip()
        state["thread_snapshot_text"] = (snap + "\n\n" + note_block).strip()

    (state.get("logs") or []).append(
        f"analyze_media: done refs={len(refs)} annotated={len(annotated_urls)}"
    )
    return state