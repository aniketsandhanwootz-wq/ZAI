# service/app/tools/file_extractors/csv_extractor.py
from __future__ import annotations

import csv
from io import StringIO
from typing import Any, Dict, List
from .router import ExtractResult


def extract_csv(*, filename: str, data: bytes, max_rows: int = 220, max_cols: int = 40, max_chars: int = 60000) -> ExtractResult:
    meta: Dict[str, Any] = {"filename": filename, "max_rows": int(max_rows), "max_cols": int(max_cols)}

    try:
        s = data.decode("utf-8", errors="replace")
        f = StringIO(s)
        reader = csv.reader(f)

        rows: List[List[str]] = []
        for i, row in enumerate(reader):
            if i >= int(max_rows):
                break
            rows.append([str(x).strip() for x in row[: int(max_cols)]])

        if not rows:
            return ExtractResult(
                doc_type="csv",
                extracted_text="(Empty CSV.)",
                extracted_json={"columns": [], "rows": 0, "row_previews": []},
                meta=meta,
            )

        # Simple header heuristic: first row with mostly non-empty cells
        header = rows[0]
        columns = header

        row_previews: List[Dict[str, Any]] = []
        out_lines: List[str] = []
        out_lines.append(f"--- CSV: {filename} (csv:{filename}) ---")
        out_lines.append("COLUMNS: " + " | ".join(columns))

        # data rows start at 2 (1-based), since header row is 1
        for ridx, r in enumerate(rows[1:], start=2):
            loc = f"csv:{filename}:row:{ridx}"
            # compact preview
            pairs = []
            for c, v in enumerate(r):
                if c >= len(columns):
                    break
                if not v:
                    continue
                pairs.append(f"{columns[c]}={v}")
            line = f"{loc}: " + " | ".join(pairs[:20])
            out_lines.append(line)

            row_previews.append(
                {
                    "locator": loc,
                    "row": ridx,
                    "values": r,
                }
            )

            if sum(len(x) for x in out_lines) > max_chars:
                break

        text = "\n".join(out_lines).strip()
        if len(text) > max_chars:
            text = text[:max_chars] + "\n\n[TRUNCATED]"

        return ExtractResult(
            doc_type="csv",
            extracted_text=text,
            extracted_json={
                "columns": columns,
                "rows": len(rows),
                "row_previews": row_previews[:200],
            },
            meta=meta,
        )

    except Exception as e:
        return ExtractResult(
            doc_type="csv",
            extracted_text="(CSV extraction failed.)",
            extracted_json={},
            meta={**meta, "error": str(e)[:300]},
        )