# service/app/tools/attachments/evidence_builder.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .evidence_schema import EvidenceItem, EvidencePack


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def build_evidence_pack(
    *,
    filename: str,
    mime_type: str,
    doc_type: str,
    content_hash: str,
    extracted_text: str,
    extracted_json: Dict[str, Any],
) -> EvidencePack:
    """
    Best-effort EvidencePack builder (Batch-1 scope).
    Works with current extractors without requiring changes:
      - Always creates file-level evidence
      - If extracted_json contains "pages": produces pdf page locators
      - If extracted_json contains "sheets": produces xlsx sheet locators
    """
    items: List[EvidenceItem] = []

    # 1) Always add file-level evidence with a short snippet
    snippet = (extracted_text or "").strip()
    if len(snippet) > 1200:
        snippet = snippet[:1200] + "…"
    items.append(
        EvidenceItem(
            locator=f"file:{filename}",
            kind="file",
            text=snippet or "(no extracted text)",
            confidence=None,
            extra=None,
        )
    )

    # 2) PDF page-wise evidence if extractor exposes pages
    pages = extracted_json.get("pages")
    if isinstance(pages, list) and pages:
        for p in pages[:60]:  # safety bound
            if not isinstance(p, dict):
                continue
            pno = _safe_int(p.get("page") or p.get("page_number") or p.get("page_index"))
            text = str(p.get("text") or p.get("extracted_text") or "").strip()
            if not text:
                continue
            if len(text) > 1200:
                text = text[:1200] + "…"
            if pno is None:
                continue
            items.append(
                EvidenceItem(
                    locator=f"pdf:{filename}:p{pno}",
                    kind="pdf_page",
                    text=text,
                    page=pno,
                    confidence=None,
                    extra={"source": "extractor_pages"},
                )
            )

    # 3) XLSX sheet-wise evidence if extractor exposes sheets
    sheets = extracted_json.get("sheets")
    if isinstance(sheets, list) and sheets:
        for sh in sheets[:50]:
            if not isinstance(sh, dict):
                continue
            name = str(sh.get("name") or sh.get("sheet") or "").strip()
            if not name:
                continue
            text = str(sh.get("text") or sh.get("extracted_text") or sh.get("summary") or "").strip()
            if not text:
                # fallback: attempt to compress cell grid if present
                grid = sh.get("grid") or sh.get("cells")
                if isinstance(grid, list):
                    text = "Sheet contains tabular data."
            if len(text) > 1200:
                text = text[:1200] + "…"
            items.append(
                EvidenceItem(
                    locator=f"xlsx:{filename}:sheet:{name}",
                    kind="xlsx_sheet",
                    text=text or "(sheet parsed; no summary provided)",
                    sheet=name,
                    confidence=None,
                    extra={"source": "extractor_sheets"},
                )
            )

    # 4) Image doc evidence (generic)
    if doc_type == "image":
        items.append(
            EvidenceItem(
                locator=f"img:{filename}",
                kind="image",
                text=snippet or "(image extracted; see vision caption fields if present)",
            )
        )

    return EvidencePack(
        filename=filename,
        mime_type=mime_type,
        doc_type=doc_type,
        content_hash=content_hash,
        items=items,
    )