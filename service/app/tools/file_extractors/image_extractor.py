# service/app/tools/file_extractors/image_extractor.py
from __future__ import annotations

from typing import Any, Dict, Optional
from .router import ExtractResult

def extract_image(*, filename: str, mime_type: str, data: bytes, vision_caption_fn=None) -> ExtractResult:
    meta: Dict[str, Any] = {"filename": filename, "mime": mime_type, "size_bytes": len(data or b"")}

    caption = ""
    if callable(vision_caption_fn):
        try:
            caption = (vision_caption_fn(image_bytes=data, mime_type=mime_type, context="") or "").strip()
        except Exception as e:
            meta["vision_error"] = str(e)[:200]

    if not caption:
        caption = "(No caption available.)"

    return ExtractResult(
        doc_type="image",
        extracted_text=caption,
        extracted_json={"caption": caption},
        meta=meta,
    )