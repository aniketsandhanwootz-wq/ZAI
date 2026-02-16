# service/app/tools/file_extractors/pdf_extractor.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
from .router import ExtractResult


def extract_pdf(
    *,
    filename: str,
    data: bytes,
    max_pages: int = 25,
    max_chars: int = 60000,
    vision_caption_fn=None,
    render_dpi: int = 140,
    max_page_images: int = 12,
    min_text_chars_for_no_vision: int = 200,
) -> ExtractResult:
    """
    Deep PDF extraction v1:
      - page-wise text extraction
      - optional page rendering -> vision caption (useful for scanned PDFs / drawings)
      - normalized JSON schema:
          extracted_json["pages"] = [
            {"page": 1, "text": "...", "page_image_caption": "...", "locator": "pdf:FILE:p1"},
            ...
          ]
    """
    meta: Dict[str, Any] = {
        "filename": filename,
        "max_pages": int(max_pages),
        "render_dpi": int(render_dpi),
        "max_page_images": int(max_page_images),
    }

    try:
        import fitz  # PyMuPDF

        doc = fitz.open(stream=data, filetype="pdf")
        meta["pages_total"] = doc.page_count

        pages_out: List[Dict[str, Any]] = []
        chunks: List[str] = []

        pages = min(doc.page_count, int(max_pages))
        rendered = 0

        for i in range(pages):
            pno = i + 1
            page = doc.load_page(i)

            page_text = (page.get_text("text") or "").strip()
            locator = f"pdf:{filename}:p{pno}"

            # Decide if we should render this page for vision:
            # - if page text is short (likely scanned / image-based)
            # - or we still have render budget and want first pages rendered
            should_render = False
            if rendered < int(max_page_images):
                if len(page_text) < int(min_text_chars_for_no_vision):
                    should_render = True
                elif pno <= 2:
                    # render first 2 pages even if text exists (often has title tables/diagrams)
                    should_render = True

            caption = ""
            if should_render and callable(vision_caption_fn):
                try:
                    pix = page.get_pixmap(dpi=int(render_dpi), alpha=False)
                    img_bytes = pix.tobytes("png")
                    caption = (vision_caption_fn(image_bytes=img_bytes, mime_type="image/png", context=f"{filename}::p{pno}") or "").strip()
                    rendered += 1
                except Exception as e:
                    meta.setdefault("vision_errors", []).append(str(e)[:200])

            pages_out.append(
                {
                    "page": pno,
                    "locator": locator,
                    "text": page_text,
                    "page_image_caption": caption,
                }
            )

            # Human-readable compact text
            block = [f"--- PDF PAGE {pno} ({locator}) ---"]
            if page_text:
                block.append(page_text)
            if caption:
                block.append(f"[PAGE_IMAGE_CAPTION] {caption}")
            chunks.append("\n".join(block))

            if sum(len(x) for x in chunks) > max_chars:
                break

        doc.close()

        text = "\n\n".join(chunks).strip()
        if len(text) > max_chars:
            text = text[:max_chars] + "\n\n[TRUNCATED]"
        if not text:
            text = "(No extractable text found in PDF.)"

        meta["pages_extracted"] = len(pages_out)
        meta["pages_rendered_for_vision"] = rendered

        return ExtractResult(
            doc_type="pdf",
            extracted_text=text,
            extracted_json={"pages": pages_out},
            meta=meta,
        )

    except Exception as e:
        return ExtractResult(
            doc_type="pdf",
            extracted_text="(PDF extraction failed.)",
            extracted_json={},
            meta={**meta, "error": str(e)[:300]},
        )