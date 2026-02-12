# service/app/pipeline/nodes/annotate_media.py
from __future__ import annotations

from typing import Any, Dict, List
import hashlib
import base64

from ...config import Settings
from ..lc_runtime import lc_registry, lc_invoke


def _sha256(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()

def _drive_thumbnail_url(file_id: str, *, width: int = 2000) -> str:
    fid = (file_id or "").strip()
    if not fid:
        return ""
    return f"https://drive.google.com/thumbnail?id={fid}&sz=w{int(width)}"

def annotate_media(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inputs:
      - state["media_images"]: [{image_index, mime_type, image_bytes, source_ref?, source_hash?}, ...]
      - state["defects_by_image"]: [{image_index, defects:[...]} ...]
    Output:
      - state["annotated_image_urls"]: [thumbnail urls...]
    """
    checkin_id = (state.get("checkin_id") or "").strip()
    images = state.get("media_images") or []
    defects_by_image = state.get("defects_by_image") or []

    tenant_id = (state.get("tenant_id") or "").strip()
    run_id = (state.get("run_id") or "").strip()

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

    reg = lc_registry(settings, state)

    existing_annot_hashes = set()
    if tenant_id and checkin_id:
        existing_annot_hashes = set(
            (lc_invoke(
                reg,
                "db_existing_artifact_source_hashes",
                {"tenant_id": tenant_id, "checkin_id": checkin_id, "artifact_type": "ANNOTATED_IMAGE"},
                state,
                default={"hashes": []},
            ) or {}).get("hashes", []) or []
        )

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

        draw = lc_invoke(
            reg,
            "annotate_draw",
            {
                "image_b64": base64.b64encode(bytes(b)).decode("utf-8"),
                "boxes": defects,
                "out_format": "PNG",
            },
            state,
            default=None,
        )
        if not isinstance(draw, dict) or not draw.get("image_b64"):
            state.setdefault("logs", []).append(f"annotate_media: draw failed img={idx} (non-fatal)")
            continue

        try:
            annotated_bytes = base64.b64decode(draw.get("image_b64") or "")
        except Exception:
            continue
        if not annotated_bytes:
            continue

        annot_hash = _sha256(annotated_bytes)

        # Idempotency: reuse existing URL if already uploaded
        if tenant_id and checkin_id and annot_hash in existing_annot_hashes:
            existing = lc_invoke(
                reg,
                "db_get_artifact_url_and_meta_by_source_hash",
                {
                    "tenant_id": tenant_id,
                    "checkin_id": checkin_id,
                    "artifact_type": "ANNOTATED_IMAGE",
                    "source_hash": annot_hash,
                },
                state,
                default=None,
            )
            existing_url = ""
            existing_meta = {}
            if isinstance(existing, (list, tuple)) and len(existing) == 2:
                existing_url, existing_meta = existing[0], existing[1]
            elif isinstance(existing, dict):
                # if DBTool returns dict, accept url/meta keys
                existing_url = str(existing.get("url") or "").strip()
                existing_meta = existing.get("meta") or {}

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

        # Upload to Drive (via LC tool)
        file_name = f"checkin_{checkin_id}_img_{idx}_annotated_{annot_hash[:10]}.png"
        up = lc_invoke(
            reg,
            "drive_upload_annotated_bytes",
            {
                "checkin_id": checkin_id,
                "file_name": file_name,
                "content_b64": base64.b64encode(annotated_bytes).decode("utf-8"),
                "mime_type": "image/png",
                "make_public": True,
            },
            state,
            default=None,
        )
        if not isinstance(up, dict):
            state.setdefault("logs", []).append(f"annotate_media: upload failed img={idx} (non-fatal)")
            continue

        fid = (up.get("file_id") or "").strip()
        thumb = _drive_thumbnail_url(fid) if fid else ""
        link = thumb or (up.get("webContentLink") or up.get("webViewLink") or "").strip()
        if not link:
            continue

        urls.append(link)

        # Record artifact
        if tenant_id and run_id:
            lc_invoke(
                reg,
                "db_insert_artifact_no_fail",
                {
                    "run_id": run_id,
                    "artifact_type": "ANNOTATED_IMAGE",
                    "url": link,
                    "meta": {
                        "tenant_id": tenant_id,
                        "checkin_id": checkin_id,
                        "source_hash": annot_hash,
                        "original_source_hash": str(img.get("source_hash") or ""),
                        "image_index": idx,
                        "file_name": file_name,
                        "mime_type": "image/png",
                        "drive_file_id": fid,
                        "thumbnail_url": thumb,
                    },
                },
                state,
                default=False,
            )
            existing_annot_hashes.add(annot_hash)

    state["annotated_image_urls"] = urls
    state.setdefault("logs", []).append(f"annotate_media: produced {len(urls)} annotated image links")
    return state