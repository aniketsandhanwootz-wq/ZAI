# service/app/tools/file_extractors/csv_extractor.py
from __future__ import annotations

import csv
from io import StringIO
from typing import Any, Dict
from .router import ExtractResult

def extract_csv(*, filename: str, data: bytes, max_rows: int = 200, max_chars: int = 60000) -> ExtractResult:
    meta: Dict[str, Any] = {"filename": filename, "max_rows": max_rows}
    try:
        s = data.decode("utf-8", errors="replace")
        f = StringIO(s)
        reader = csv.reader(f)
        rows = []
        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            rows.append(row)

        # render as a compact text table
        out_lines = []
        for r in rows:
            out_lines.append(" | ".join([str(x).strip() for x in r[:30]]))
            if sum(len(x) for x in out_lines) > max_chars:
                break

        text = "\n".join(out_lines).strip()
        if len(text) > max_chars:
            text = text[:max_chars] + "\n\n[TRUNCATED]"
        if not text:
            text = "(Empty CSV.)"

        return ExtractResult(
            doc_type="csv",
            extracted_text=text,
            extracted_json={"rows": min(len(rows), max_rows)},
            meta=meta,
        )
    except Exception as e:
        return ExtractResult(
            doc_type="csv",
            extracted_text="(CSV extraction failed.)",
            extracted_json={},
            meta={**meta, "error": str(e)[:300]},
        )