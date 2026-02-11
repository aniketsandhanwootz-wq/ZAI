# service/app/pipeline/nodes/annotate_media.py
from __future__ import annotations

from typing import Any, Dict, List
from uuid import uuid4

from ...config import Settings
from ...tools.annotate_tool import AnnotateTool
from ...tools.drive_tool import DriveTool
import hashlib

from ...tools.db_tool import DBTool

def _sha256(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def _drive_thumbnail_url(file_id: str, *, width: int = 2000) -> str:
    """
    AppSheet Image columns often render best with Drive thumbnail endpoint.
    Requires file to be readable (we set anyone-reader via DriveTool make_public).
    """
    fid = (file_id or "").strip()
    if not fid:
        return ""
    # width param format: sz=w2000
    return f"https://drive.google.com/thumbnail?id={fid}&sz=w{int(width)}"

def annotate_media(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inputs:
      - state["media_images"]: [{image_index, mime_type, image_bytes, source_ref?}, ...]
      - state["defects_by_image"]: [{image_index, defects:[...]} ...]
    Output:
      - state["annotated_image_urls"]: [Drive webViewLink/webContentLink...]
    """
    checkin_id = (state.get("checkin_id") or "").strip()
    images = state.get("media_images") or []
    defects_by_image = state.get("defects_by_image") or []

    tenant_id = (state.get("tenant_id") or "").strip()
    run_id = (state.get("run_id") or "").strip()

    db = DBTool(settings.database_url) if (tenant_id and run_id) else None
    existing_annot_hashes = set()
    if db and tenant_id and checkin_id:
        existing_annot_hashes = db.existing_artifact_source_hashes(
            tenant_id=tenant_id,
            checkin_id=checkin_id,
            artifact_type="ANNOTATED_IMAGE",
        )

    if not checkin_id or not isinstance(images, list) or not images:
        state.setdefault("logs", []).append("annotate_media: skipped (no checkin_id/images)")
        state["annotated_image_urls"] = []
        return state

    # Map image_index -> defects
    defect_map: Dict[int, List[Dict[str, Any]]] = {}
    if isinstance(defects_by_image, list):
        for it in defects_by_image:
            if not isinstance(it, dict):
                continue
            try:
                idx = int(it.get("image_index"))
            except Exception:
                continue
            ds = it.get("defects") or []
            if isinstance(ds, list) and ds:
                defect_map[idx] = ds

    if not defect_map:
        state.setdefault("logs", []).append("annotate_media: no defects detected")
        state["annotated_image_urls"] = []
        return state

    # Drive init must be non-fatal
    try:
        drive = DriveTool(settings)
    except Exception as e:
        state.setdefault("logs", []).append(f"annotate_media: Drive init failed (non-fatal): {e}")
        state["annotated_image_urls"] = []
        return state

    annot = AnnotateTool()
    urls: List[str] = []

    for img in images:
        try:
            idx = int(img.get("image_index"))
        except Exception:
            continue

        defects = defect_map.get(idx) or []
        if not defects:
            continue

        b = img.get("image_bytes")
        if not isinstance(b, (bytes, bytearray)) or not b:
            continue

        # Draw (never crash)
        try:
            annotated_bytes = annot.draw(bytes(b), defects, out_format="PNG")
        except Exception as e:
            state.setdefault("logs", []).append(f"annotate_media: draw failed img={idx} (non-fatal): {e}")
            continue

        annot_hash = _sha256(annotated_bytes)

        # Idempotency: if already uploaded, reuse URL (prefer thumbnail if we have drive_file_id in meta)
        if db and tenant_id and checkin_id and annot_hash in existing_annot_hashes:
            existing_url, existing_meta = db.get_artifact_url_and_meta_by_source_hash(
                tenant_id=tenant_id,
                checkin_id=checkin_id,
                artifact_type="ANNOTATED_IMAGE",
                source_hash=annot_hash,
            )

            drive_file_id = ""
            if isinstance(existing_meta, dict):
                drive_file_id = str(existing_meta.get("drive_file_id") or "").strip()

            thumb = _drive_thumbnail_url(drive_file_id) if drive_file_id else ""
            if thumb:
                urls.append(thumb)
                continue

            if existing_url:
                urls.append(existing_url)
                continue

        # Upload (never crash)
        file_name = f"checkin_{checkin_id}_img_{idx}_annotated_{annot_hash[:10]}.png"
        try:
            up = drive.upload_annotated_bytes(
                checkin_id=checkin_id,
                file_name=file_name,
                content_bytes=annotated_bytes,
                mime_type="image/png",
                make_public=True,
            )
        except Exception as e:
            state.setdefault("logs", []).append(f"annotate_media: upload failed img={idx} (non-fatal): {e}")
            continue

        # Prefer Drive thumbnail URL for AppSheet rendering
        fid = (up.get("file_id") or "").strip()
        thumb = _drive_thumbnail_url(fid) if fid else ""
        link = thumb or (up.get("webContentLink") or up.get("webViewLink") or "").strip()
        if not link:
            continue

        urls.append(link)

        # Record artifact (never crash)
        if db and tenant_id and run_id:
            db.insert_artifact_no_fail(
                run_id=run_id,
                artifact_type="ANNOTATED_IMAGE",
                url=link,
                meta={
                    "tenant_id": tenant_id,
                    "checkin_id": checkin_id,
                    "source_hash": annot_hash,                         # annotated bytes hash
                    "original_source_hash": str(img.get("source_hash") or ""),
                    "image_index": idx,
                    "file_name": file_name,
                    "mime_type": "image/png",

                    # ✅ store drive file id so future idempotency can always rebuild thumbnail link
                    "drive_file_id": fid,
                    "thumbnail_url": thumb,
                },
            )
            existing_annot_hashes.add(annot_hash)

    state["annotated_image_urls"] = urls
    state.setdefault("logs", []).append(f"annotate_media: produced {len(urls)} annotated image links")
    return state
