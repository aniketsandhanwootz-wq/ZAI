# service/app/pipeline/nodes/annotate_media.py
from __future__ import annotations

from typing import Any, Dict, List
from uuid import uuid4

from ...config import Settings
from ...tools.annotate_tool import AnnotateTool
from ...tools.drive_tool import DriveTool


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

    drive = DriveTool(settings)
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

        annotated_bytes = annot.draw(bytes(b), defects, out_format="PNG")

        suffix = uuid4().hex[:6]
        file_name = f"checkin_{checkin_id}_img_{idx}_annotated_{suffix}.png"

        up = drive.upload_annotated_bytes(
            checkin_id=checkin_id,
            file_name=file_name,
            content_bytes=annotated_bytes,
            mime_type="image/png",
            make_public=True,
        )

        link = (up.get("webViewLink") or up.get("webContentLink") or "").strip()
        if link:
            urls.append(link)

    state["annotated_image_urls"] = urls
    state.setdefault("logs", []).append(f"annotate_media: uploaded {len(urls)} annotated images")
    return state
