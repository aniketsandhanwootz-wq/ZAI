# service/app/tools/file_extractors/router.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import hashlib
import mimetypes
from PIL import Image
from io import BytesIO

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b or b"").hexdigest()

def sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

def sniff_mime(filename: Optional[str] = None, mime_type: Optional[str] = None, data: Optional[bytes] = None) -> str:
    """
    Best-effort MIME sniffing (stable + practical):
    Priority:
      1) explicit mime_type if provided and meaningful
      2) PDF magic bytes
      3) filename-based mimetypes
      4) image detection via Pillow
      5) fallback octet-stream
    """
    mt = (mime_type or "").strip().lower()

    # 1) if caller already has a real mime type, trust it (unless it's generic)
    if mt and mt not in ("application/octet-stream", "binary/octet-stream", "application/binary"):
        return mt

    # 2) magic bytes: PDF
    if data and len(data) >= 4 and data[:4] == b"%PDF":
        return "application/pdf"

    # 3) filename-based guess
    if filename:
        g, _ = mimetypes.guess_type(filename)
        if g:
            return g.lower()

    # 4) content-based image detection
    if data:
        try:
            img = Image.open(BytesIO(data))
            fmt = (img.format or "").upper()
            if fmt == "JPEG":
                return "image/jpeg"
            if fmt == "PNG":
                return "image/png"
            if fmt == "GIF":
                return "image/gif"
            if fmt == "WEBP":
                return "image/webp"
            if fmt == "TIFF":
                return "image/tiff"
            if fmt == "BMP":
                return "image/bmp"
            return "image/*"
        except Exception:
            pass

    return "application/octet-stream"

@dataclass
class ExtractResult:
    doc_type: str
    extracted_text: str
    extracted_json: Dict[str, Any]
    meta: Dict[str, Any]

def extract_any(*, filename: str, mime_type: str, data: bytes, vision_caption_fn=None) -> ExtractResult:
    mime = sniff_mime(filename=filename, mime_type=mime_type, data=data)

    if mime == "application/pdf":
        from .pdf_extractor import extract_pdf
        return extract_pdf(filename=filename, data=data)

    if mime.startswith("image/"):
        from .image_extractor import extract_image
        return extract_image(filename=filename, mime_type=mime, data=data, vision_caption_fn=vision_caption_fn)

    if mime in ("text/csv", "text/plain") or filename.lower().endswith(".csv"):
        from .csv_extractor import extract_csv
        return extract_csv(filename=filename, data=data)

    if mime.endswith("spreadsheetml.sheet") or filename.lower().endswith(".xlsx"):
        from .xlsx_extractor import extract_xlsx
        return extract_xlsx(
            filename=filename,
            data=data,
            vision_caption_fn=vision_caption_fn,  # ✅ now captions embedded images too
        )

    return ExtractResult(
        doc_type="unknown",
        extracted_text="(Unsupported file type for extraction. Stored metadata only.)",
        extracted_json={},
        meta={"mime": mime, "size_bytes": len(data or b"")},
    )