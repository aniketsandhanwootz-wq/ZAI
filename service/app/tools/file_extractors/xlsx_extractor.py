# service/app/tools/file_extractors/xlsx_extractor.py
from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, Optional, List
from .router import ExtractResult, sniff_mime

import zipfile


def _extract_embedded_images_from_xlsx_bytes(
    data: bytes,
    *,
    max_images: int = 8,
) -> List[dict]:
    """
    Extract raw embedded images from XLSX by reading the zip (xl/media/*).
    This is more reliable than openpyxl images API.
    Returns: [{"name": "...", "bytes": b"...", "mime": "image/png"}, ...]
    """
    out: List[dict] = []
    try:
        with zipfile.ZipFile(BytesIO(data), "r") as z:
            names = [n for n in z.namelist() if n.lower().startswith("xl/media/")]
            # stable order
            names.sort()
            for n in names[:max_images]:
                b = z.read(n)
                mt = sniff_mime(filename=n, mime_type="", data=b)
                out.append({"name": n.split("/")[-1], "bytes": b, "mime": mt})
    except Exception:
        return []
    return out


def extract_xlsx(
    *,
    filename: str,
    data: bytes,
    max_sheets: int = 3,
    max_rows: int = 120,
    max_cols: int = 25,
    max_chars: int = 60000,
    vision_caption_fn=None,
    max_images: int = 8,
) -> ExtractResult:
    meta: Dict[str, Any] = {
        "filename": filename,
        "max_sheets": max_sheets,
        "max_rows": max_rows,
        "max_cols": max_cols,
        "max_images": max_images,
    }

    try:
        from openpyxl import load_workbook

        # NOTE: read_only=True is fine for cell text; images we extract from zip separately.
        wb = load_workbook(BytesIO(data), read_only=True, data_only=True)
        sheetnames = wb.sheetnames[:max_sheets]
        meta["sheets"] = sheetnames

        chunks: List[str] = []
        for sn in sheetnames:
            ws = wb[sn]
            chunks.append(f"--- SHEET: {sn} ---")
            for r_i, row in enumerate(ws.iter_rows(values_only=True)):
                if r_i >= max_rows:
                    break
                vals = []
                for c_i, v in enumerate(row):
                    if c_i >= max_cols:
                        break
                    vals.append("" if v is None else str(v).strip())
                chunks.append(" | ".join(vals))

                if sum(len(x) for x in chunks) > max_chars:
                    break

            if sum(len(x) for x in chunks) > max_chars:
                break

        wb.close()

        # -------- Embedded images -> caption (if provided) --------
        image_items = _extract_embedded_images_from_xlsx_bytes(data, max_images=max_images)
        meta["embedded_images_found"] = len(image_items)

        image_captions: List[dict] = []
        if image_items:
            chunks.append("\n--- EMBEDDED IMAGES ---")
            for it in image_items:
                name = it.get("name", "image")
                mime = it.get("mime", "application/octet-stream")
                b = it.get("bytes", b"")

                caption = ""
                if callable(vision_caption_fn) and b:
                    try:
                        caption = (vision_caption_fn(image_bytes=b, mime_type=mime, context=f"{filename}::{name}") or "").strip()
                    except Exception as e:
                        caption = ""
                        meta.setdefault("vision_errors", []).append(str(e)[:200])

                if not caption:
                    caption = "(No caption available.)"

                image_captions.append({"name": name, "mime": mime, "caption": caption})
                chunks.append(f"- {name} ({mime}): {caption}")

                if sum(len(x) for x in chunks) > max_chars:
                    break

        text = "\n".join(chunks).strip()
        if len(text) > max_chars:
            text = text[:max_chars] + "\n\n[TRUNCATED]"
        if not text:
            text = "(Empty XLSX.)"

        return ExtractResult(
            doc_type="xlsx",
            extracted_text=text,
            extracted_json={
                "sheets": sheetnames,
                "embedded_images": image_captions,
            },
            meta=meta,
        )

    except Exception as e:
        return ExtractResult(
            doc_type="xlsx",
            extracted_text="(XLSX extraction failed.)",
            extracted_json={},
            meta={**meta, "error": str(e)[:300]},
        )