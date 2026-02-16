# service/app/tools/attachments/evidence_schema.py
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional


@dataclass
class EvidenceItem:
    """
    Minimal evidence unit with a stable locator string.
    Locator examples:
      - file:Report.pdf
      - pdf:Report.pdf:p7
      - pdf:Report.pdf:p7:img2
      - xlsx:Inspection.xlsx:sheet:DimCheck
      - csv:Measurements.csv:row:18
      - img:photo1.jpg
    """
    locator: str
    kind: str                 # "file" | "pdf_page" | "pdf_image" | "xlsx_sheet" | "csv_row" | "image" | "unknown"
    text: str                 # snippet / summary / extracted piece
    page: Optional[int] = None
    sheet: Optional[str] = None
    row: Optional[int] = None
    col: Optional[int] = None
    confidence: Optional[float] = None
    extra: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class EvidencePack:
    filename: str
    mime_type: str
    doc_type: str
    content_hash: str
    items: List[EvidenceItem]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "filename": self.filename,
            "mime_type": self.mime_type,
            "doc_type": self.doc_type,
            "content_hash": self.content_hash,
            "items": [it.to_dict() for it in self.items],
        }