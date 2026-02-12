# service/app/tools/file_extractors/pdf_extractor.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Callable

from .router import ExtractResult


# ---- Hard defaults (NO .env) ----
DEFAULT_MAX_PAGES = 40
DEFAULT_MAX_CHARS = 140000

# Per-page OCR behavior (NO .env)
DEFAULT_PAGE_MIN_TEXT = 120   # if page text < this -> treat as scanned/table-like
DEFAULT_OCR_MAX_PAGES = 6     # cap OCR calls to avoid spam/cost


def _pick_pages(total: int, max_pages: int) -> List[int]:
    if total <= 0:
        return []
    max_pages = max(1, min(int(max_pages), total))
    if total <= max_pages:
        return list(range(total))

    head = min(10, max_pages // 2 + 2)
    tail = min(8, max_pages // 3 + 1)
    remaining = max_pages - head - tail

    pages = set()
    for i in range(head):
        pages.add(i)
    for i in range(total - tail, total):
        pages.add(i)

    if remaining > 0:
        step = max(1, (total - 1) // (remaining + 1))
        cur = step
        while len(pages) < max_pages and cur < total - 1:
            pages.add(cur)
            cur += step

    out = sorted(pages)
    return out[:max_pages]


def _render_page_png_bytes(page, *, max_dim: int = 1800) -> bytes:
    import fitz  # PyMuPDF

    zoom = 2.2
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)

    if max(pix.width, pix.height) > max_dim:
        scale = max_dim / float(max(pix.width, pix.height))
        mat = fitz.Matrix(zoom * scale, zoom * scale)
        pix = page.get_pixmap(matrix=mat, alpha=False)

    return pix.tobytes("png")


def _needs_ocr(page_text: str, *, min_chars: int) -> bool:
    t = (page_text or "").strip()
    return len(t) < int(min_chars)


def extract_pdf(
    *,
    filename: str,
    data: bytes,
    max_pages: Optional[int] = None,
    max_chars: Optional[int] = None,
    vision_caption_fn: Optional[Callable[..., str]] = None,
) -> ExtractResult:
    """
    Robust PDF extraction:
      - head+tail+middle selection (not just first pages)
      - per-page OCR fallback when a page has little/no extractable text
      - OCR uses VisionTool in OCR_MODE (document/table extraction), not the 6-line caption prompt
    """
    max_pages = int(max_pages or DEFAULT_MAX_PAGES)
    max_chars = int(max_chars or DEFAULT_MAX_CHARS)

    page_min_text = DEFAULT_PAGE_MIN_TEXT
    ocr_max_pages = DEFAULT_OCR_MAX_PAGES

    meta: Dict[str, Any] = {
        "filename": filename,
        "max_pages": max_pages,
        "max_chars": max_chars,
        "page_min_text": page_min_text,
        "ocr_max_pages": ocr_max_pages,
    }

    try:
        import fitz  # PyMuPDF

        doc = fitz.open(stream=data, filetype="pdf")
        total = int(doc.page_count or 0)
        meta["pages_total"] = total

        page_idxs = _pick_pages(total, max_pages)
        meta["pages_selected"] = page_idxs

        parts: List[str] = []
        ocr_used = 0

        for i in page_idxs:
            try:
                page = doc.load_page(int(i))
            except Exception:
                continue

            page_text = ""
            try:
                page_text = (page.get_text("text") or "").strip()
            except Exception:
                page_text = ""

            if page_text:
                parts.append(f"[PAGE {i+1}/{total} TEXT]\n{page_text}")

            # OCR for mixed PDFs (tables/images embedded)
            if callable(vision_caption_fn) and ocr_used < ocr_max_pages and _needs_ocr(page_text, min_chars=page_min_text):
                try:
                    png = _render_page_png_bytes(page)
                    ocr_ctx = f"OCR_MODE:1 | {filename} | page {i+1}/{total}"
                    ocr_txt = (vision_caption_fn(image_bytes=png, mime_type="image/png", context=ocr_ctx) or "").strip()
                    if ocr_txt:
                        parts.append(f"[PAGE {i+1}/{total} OCR]\n{ocr_txt}")
                        ocr_used += 1
                except Exception as e:
                    meta.setdefault("ocr_errors", []).append(str(e)[:200])

            if sum(len(p) for p in parts) > max_chars:
                break

        doc.close()

        text = "\n\n".join([p for p in parts if p.strip()]).strip()
        if len(text) > max_chars:
            text = text[:max_chars] + "\n\n[TRUNCATED]"
        if not text:
            text = "(No extractable text found in PDF.)"

        meta["ocr_pages_used"] = ocr_used

        return ExtractResult(
            doc_type="pdf",
            extracted_text=text,
            extracted_json={},
            meta=meta,
        )

    except Exception as e:
        return ExtractResult(
            doc_type="pdf",
            extracted_text="(PDF extraction failed.)",
            extracted_json={},
            meta={**meta, "error": str(e)[:300]},
        )