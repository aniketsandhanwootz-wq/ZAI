# service/app/tools/file_extractors/xlsx_extractor.py
from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, Optional, List, Tuple
from .router import ExtractResult, sniff_mime
import zipfile
import math


def _is_number(x: Any) -> bool:
    if x is None:
        return False
    if isinstance(x, (int, float)) and not (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return True
    if isinstance(x, str):
        s = x.strip().replace(",", "")
        if not s:
            return False
        try:
            float(s)
            return True
        except Exception:
            return False
    return False


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        try:
            v = float(x)
            if math.isnan(v) or math.isinf(v):
                return None
            return v
        except Exception:
            return None
    if isinstance(x, str):
        s = x.strip().replace(",", "")
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None
    return None


def _extract_embedded_images_from_xlsx_bytes(data: bytes, *, max_images: int = 8) -> List[dict]:
    out: List[dict] = []
    try:
        with zipfile.ZipFile(BytesIO(data), "r") as z:
            names = [n for n in z.namelist() if n.lower().startswith("xl/media/")]
            names.sort()
            for n in names[:max_images]:
                b = z.read(n)
                mt = sniff_mime(filename=n, mime_type="", data=b)
                out.append({"name": n.split("/")[-1], "bytes": b, "mime": mt})
    except Exception:
        return []
    return out


def _detect_header_row(rows: List[List[Any]], *, scan_rows: int = 20, min_cols: int = 3) -> Tuple[int, List[str]]:
    best_i = -1
    best_score = -1
    best_headers: List[str] = []

    for i, r in enumerate(rows[:scan_rows]):
        cells = [("" if v is None else str(v).strip()) for v in r]
        non_empty = [c for c in cells if c]
        if len(non_empty) < min_cols:
            continue

        textish = sum(1 for c in non_empty if not _is_number(c) and len(c) <= 60)
        score = (2 * textish) + len(non_empty)

        if score > best_score:
            best_score = score
            best_i = i
            best_headers = cells

    if best_i < 0:
        for i, r in enumerate(rows[:scan_rows]):
            cells = [("" if v is None else str(v).strip()) for v in r]
            if any(cells):
                return i, cells
        return 0, []

    return best_i, best_headers


def _summarize_table(headers: List[str], data_rows: List[List[Any]], *, max_preview_rows: int = 12) -> Dict[str, Any]:
    hdr = [h.strip() if h else "" for h in headers]
    seen = {}
    out_hdr = []
    for j, h in enumerate(hdr):
        key = (h or "").strip().lower()
        if not key:
            key = f"col_{j+1}"
        n = seen.get(key, 0) + 1
        seen[key] = n
        out_hdr.append(h if h else (f"col_{j+1}" if n == 1 else f"{key}_{n}"))

    col_vals: List[List[Any]] = [[] for _ in out_hdr]
    nonempty_counts = [0] * len(out_hdr)

    for r in data_rows:
        for j in range(min(len(out_hdr), len(r))):
            v = r[j]
            col_vals[j].append(v)
            if v is not None and str(v).strip() != "":
                nonempty_counts[j] += 1

    numeric_cols = []
    stats = {}
    for j, name in enumerate(out_hdr):
        vals = col_vals[j]
        nums = [x for x in ([_to_float(v) for v in vals]) if x is not None]
        if len(nums) >= max(5, int(0.2 * max(1, len(vals)))):
            numeric_cols.append(name)
            stats[name] = {
                "count": len(nums),
                "min": min(nums) if nums else None,
                "max": max(nums) if nums else None,
                "avg": (sum(nums) / len(nums)) if nums else None,
            }

    missing = {}
    total_rows = max(1, len(data_rows))
    for j, name in enumerate(out_hdr):
        miss = total_rows - nonempty_counts[j]
        if miss > 0:
            missing[name] = miss

    preview = []
    for r in data_rows[:max_preview_rows]:
        row_obj = {}
        for j, name in enumerate(out_hdr):
            if j < len(r):
                v = r[j]
                row_obj[name] = "" if v is None else str(v).strip()
        preview.append(row_obj)

    return {
        "headers": out_hdr,
        "rows": len(data_rows),
        "numeric_columns": numeric_cols[:12],
        "numeric_stats": stats,
        "missing_cells_by_column": missing,
        "preview_rows": preview,
    }


def extract_xlsx(
    *,
    filename: str,
    data: bytes,
    max_sheets: int = 6,
    max_rows: int = 350,
    max_cols: int = 60,
    max_chars: int = 90000,
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

        wb = load_workbook(BytesIO(data), read_only=True, data_only=True)
        sheetnames = wb.sheetnames[:max_sheets]
        meta["sheets"] = sheetnames

        sheet_summaries: List[Dict[str, Any]] = []
        text_chunks: List[str] = []

        text_chunks.append(f"XLSX FILE: {filename}")
        text_chunks.append(f"SHEETS (max {max_sheets}): " + ", ".join(sheetnames))

        for sn in sheetnames:
            ws = wb[sn]

            rows: List[List[Any]] = []
            for r_i, row in enumerate(ws.iter_rows(values_only=True)):
                if r_i >= max_rows:
                    break
                rows.append(list(row[:max_cols]))

            header_i, headers = _detect_header_row(rows, scan_rows=20, min_cols=3)

            data_rows = []
            for r in rows[header_i + 1 :]:
                if not any(v is not None and str(v).strip() != "" for v in r):
                    continue
                data_rows.append(r)

            summary = _summarize_table(headers, data_rows, max_preview_rows=10)

            sheet_summaries.append({"sheet": sn, "header_row_index": header_i, "table_summary": summary})

            text_chunks.append("\n--- SHEET SUMMARY ---")
            text_chunks.append(f"SHEET: {sn}")
            text_chunks.append(f"HeaderRowIndex: {header_i}")
            text_chunks.append("Headers: " + ", ".join(summary.get("headers") or [])[:1500])

            nc = summary.get("numeric_columns") or []
            if nc:
                text_chunks.append("NumericColumns: " + ", ".join(nc))

            ms = summary.get("missing_cells_by_column") or {}
            if ms:
                top_m = sorted(ms.items(), key=lambda kv: kv[1], reverse=True)[:8]
                text_chunks.append("MissingCellsTop: " + "; ".join([f"{k}={v}" for k, v in top_m]))

            prev = summary.get("preview_rows") or []
            if prev:
                text_chunks.append("PreviewRows:")
                for i, ro in enumerate(prev[:6], start=1):
                    items = list(ro.items())[:12]
                    line = ", ".join([f"{k}={v}" for k, v in items])
                    text_chunks.append(f"- R{i}: {line[:800]}")

            if sum(len(x) for x in text_chunks) > max_chars:
                break

        wb.close()

        image_items = _extract_embedded_images_from_xlsx_bytes(data, max_images=max_images)
        meta["embedded_images_found"] = len(image_items)

        image_captions: List[dict] = []
        if image_items:
            text_chunks.append("\n--- EMBEDDED IMAGES ---")
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
                text_chunks.append(f"- {name} ({mime}): {caption}")

                if sum(len(x) for x in text_chunks) > max_chars:
                    break

        text = "\n".join(text_chunks).strip()
        if len(text) > max_chars:
            text = text[:max_chars] + "\n\n[TRUNCATED]"
        if not text:
            text = "(Empty XLSX.)"

        return ExtractResult(
            doc_type="xlsx",
            extracted_text=text,
            extracted_json={"sheets": sheetnames, "sheet_summaries": sheet_summaries, "embedded_images": image_captions},
            meta=meta,
        )

    except Exception as e:
        return ExtractResult(
            doc_type="xlsx",
            extracted_text="(XLSX extraction failed.)",
            extracted_json={},
            meta={**meta, "error": str(e)[:300]},
        )