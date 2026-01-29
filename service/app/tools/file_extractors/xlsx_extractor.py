# service/app/tools/file_extractors/xlsx_extractor.py
from __future__ import annotations

from io import BytesIO
from typing import Any, Dict
from .router import ExtractResult

def extract_xlsx(*, filename: str, data: bytes, max_sheets: int = 3, max_rows: int = 120, max_cols: int = 25, max_chars: int = 60000) -> ExtractResult:
    meta: Dict[str, Any] = {"filename": filename, "max_sheets": max_sheets}
    try:
        from openpyxl import load_workbook

        wb = load_workbook(BytesIO(data), read_only=True, data_only=True)
        sheetnames = wb.sheetnames[:max_sheets]
        meta["sheets"] = sheetnames

        chunks = []
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
                    if v is None:
                        vals.append("")
                    else:
                        vals.append(str(v).strip())
                chunks.append(" | ".join(vals))
                if sum(len(x) for x in chunks) > max_chars:
                    break
            if sum(len(x) for x in chunks) > max_chars:
                break

        wb.close()

        text = "\n".join(chunks).strip()
        if len(text) > max_chars:
            text = text[:max_chars] + "\n\n[TRUNCATED]"
        if not text:
            text = "(Empty XLSX.)"

        return ExtractResult(
            doc_type="xlsx",
            extracted_text=text,
            extracted_json={"sheets": sheetnames},
            meta=meta,
        )
    except Exception as e:
        return ExtractResult(
            doc_type="xlsx",
            extracted_text="(XLSX extraction failed.)",
            extracted_json={},
            meta={**meta, "error": str(e)[:300]},
        )