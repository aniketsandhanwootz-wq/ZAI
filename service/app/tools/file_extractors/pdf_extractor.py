# service/app/tools/file_extractors/pdf_extractor.py
from __future__ import annotations

from typing import Any, Dict
from .router import ExtractResult

def extract_pdf(*, filename: str, data: bytes, max_pages: int = 25, max_chars: int = 60000) -> ExtractResult:
    text = ""
    meta: Dict[str, Any] = {"filename": filename, "max_pages": max_pages}

    try:
        import fitz  # PyMuPDF
        doc = fitz.open(stream=data, filetype="pdf")
        meta["pages_total"] = doc.page_count

        parts = []
        pages = min(doc.page_count, int(max_pages))
        for i in range(pages):
            page = doc.load_page(i)
            parts.append(page.get_text("text") or "")
            if sum(len(p) for p in parts) > max_chars:
                break

        doc.close()
        text = "\n\n".join([p.strip() for p in parts if p.strip()]).strip()
        if len(text) > max_chars:
            text = text[:max_chars] + "\n\n[TRUNCATED]"
        if not text:
            text = "(No extractable text found in PDF.)"

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