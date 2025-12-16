from __future__ import annotations

from typing import Dict, Any
import requests
from pypdf import PdfReader
from io import BytesIO

from ...config import Settings
from ...tools.sheets_tool import SheetsTool
from ...tools.embed_tool import EmbedTool
from ...tools.vector_tool import VectorTool
from .utils import chunk_text


def _try_extract_pdf_text_from_url(url: str, timeout: int = 30) -> str:
    """
    MVP: only works if URL is directly downloadable.
    If your AppSheet file links are not direct-download, we'll enhance later (Drive API download).
    """
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        data = BytesIO(r.content)
        reader = PdfReader(data)
        out = []
        for page in reader.pages:
            out.append(page.extract_text() or "")
        return "\n".join(out).strip()
    except Exception:
        return ""


def ingest_ccp(settings: Settings) -> Dict[str, Any]:
    sheets = SheetsTool(settings)
    embedder = EmbedTool(settings)
    vec = VectorTool(settings)

    rows = sheets.list_ccp()
    total = 0
    embedded = 0
    skipped = 0

    for r in rows:
        # Columns per mapping.yaml (actual header names)
        ccp_id = str(r.get("CCP ID", "")).strip()
        if not ccp_id:
            skipped += 1
            continue

        legacy_id = str(r.get("ID", "")).strip()
        ccp_name = (r.get("CCP Name") or "").strip()
        desc = (r.get("Description") or "").strip()

        project_name = (r.get("Project Name") or "").strip()
        part_number = (r.get("Part Number") or "").strip()

        # tenant_id: join via Project tab (Project Name + Part Number + ID)
        project_row = sheets.get_project_row(project_name, part_number, legacy_id) if (project_name and part_number and legacy_id) else None
        tenant_id = (project_row or {}).get("Company Row id", "")
        if not tenant_id:
            # If tenant missing, still skip to avoid cross-tenant leaks
            skipped += 1
            continue

        # 1) CCP description chunk(s)
        if desc:
            chunks = chunk_text(f"CCP: {ccp_name}\n{desc}")
            for ch in chunks:
                emb = embedder.embed_text(ch)
                vec.upsert_ccp_chunk(
                    tenant_id=tenant_id,
                    ccp_id=ccp_id,
                    ccp_name=ccp_name,
                    project_name=project_name,
                    part_number=part_number,
                    legacy_id=legacy_id,
                    chunk_type="CCP_DESC",
                    chunk_text=ch,
                    source_ref="",
                    embedding=emb,
                )
                embedded += 1
        total += 1

        # 2) CCP Files (PDFs) if they are direct URLs
        files_val = (r.get("Files") or "").strip()
        if files_val:
            # split by comma/newline; adjust later if your format differs
            file_urls = [x.strip() for x in files_val.replace("\n", ",").split(",") if x.strip()]
            for fu in file_urls:
                if fu.lower().endswith(".pdf") and fu.startswith("http"):
                    text = _try_extract_pdf_text_from_url(fu)
                    if text:
                        for ch in chunk_text(text):
                            emb = embedder.embed_text(ch)
                            vec.upsert_ccp_chunk(
                                tenant_id=tenant_id,
                                ccp_id=ccp_id,
                                ccp_name=ccp_name,
                                project_name=project_name,
                                part_number=part_number,
                                legacy_id=legacy_id,
                                chunk_type="PDF_TEXT",
                                chunk_text=ch,
                                source_ref=fu,
                                embedding=emb,
                            )
                            embedded += 1

    return {
        "source": "ccp",
        "rows_seen": total,
        "chunks_embedded": embedded,
        "skipped_rows": skipped,
        "note": "PDF extraction works only for direct-download URLs in MVP.",
    }
