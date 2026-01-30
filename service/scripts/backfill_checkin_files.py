from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, Iterable, List, Optional
from app.config import load_settings
from app.tools.sheets_tool import SheetsTool, _key
from app.pipeline.nodes.load_sheet_data import load_sheet_data
from app.pipeline.nodes.build_thread_snapshot import build_thread_snapshot
from app.pipeline.nodes.analyze_attachments import analyze_attachments


def _norm_header(s: str) -> str:
    import re
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _find_files_cell_from_row(checkin_row: Dict[str, Any]) -> str:
    """
    Same spirit as analyze_attachments._find_files_cell but kept here so script can filter fast.
    checkin_row keys are casefold headers (e.g. 'files', 'checkin id', etc.)
    """
    if not checkin_row:
        return ""

    # exact "files"
    for k in checkin_row.keys():
        if _norm_header(k) == "files":
            v = str(checkin_row.get(k) or "").strip()
            if v:
                return v

    # fallback candidates
    candidates = {"files", "file", "attachments", "attachment", "documents", "docs"}
    for k in checkin_row.keys():
        if _norm_header(k) in candidates:
            v = str(checkin_row.get(k) or "").strip()
            if v:
                return v

    return ""


def iter_checkin_ids_with_files(sheets: SheetsTool, *, limit: Optional[int] = None) -> Iterable[str]:
    rows = sheets.list_checkins()

    checkin_id_key = _key(sheets.map.col("checkin", "checkin_id"))
    files_key     = _key(sheets.map.col("checkin", "files"))  # <-- mapping.yaml must have this

    seen = set()
    count = 0

    for r in rows:
        if not isinstance(r, dict):
            continue

        cid = str(r.get(checkin_id_key) or "").strip()
        if not cid or cid in seen:
            continue

        files_cell = str(r.get(files_key) or "").strip()
        if not files_cell:
            continue

        seen.add(cid)
        yield cid

        count += 1
        if limit and count >= int(limit):
            return
def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="Process only first N checkins (0 = all)")
    ap.add_argument("--max-files", type=int, default=6, help="Max files per checkin")
    ap.add_argument("--max-bytes", type=int, default=15_000_000, help="Max bytes per file download")
    args = ap.parse_args(argv)

    settings = load_settings()
    sheets = SheetsTool(settings)

    total = 0
    ok = 0
    err = 0

    limit = args.limit if args.limit and args.limit > 0 else None

    for checkin_id in iter_checkin_ids_with_files(sheets, limit=limit):
        total += 1
        try:
            # Build a minimal pipeline state; reuse existing nodes so tenant_id resolution is correct
            state: Dict[str, Any] = {
                "payload": {
                    "event_type": "MANUAL_TRIGGER",
                    "checkin_id": checkin_id,
                    "meta": {
                        "ingest_only": True,         # ensures no reply/writeback behavior elsewhere if reused
                        "attachments_only": True,    # just a marker for logs
                        "max_files": int(args.max_files),
                        "max_bytes": int(args.max_bytes),
                    },
                },
                "logs": [],
            }

            state = load_sheet_data(settings, state)
            state = build_thread_snapshot(settings, state)
            state = analyze_attachments(settings, state)

            ok += 1
            items = state.get("attachments_analyzed") or []
            print(f"[OK] checkin_id={checkin_id} analyzed={len(items)}")

            # Print what exactly was analyzed/skipped
            for it in items:
                ref = str(it.get("ref") or "").strip()
                fn  = str(it.get("filename") or "").strip()
                okf = it.get("ok")
                skp = it.get("skipped", False)
                reason = str(it.get("reason") or "").strip()

                status = "OK" if okf else "FAIL"
                if skp:
                    status = "SKIP"

                # keep line short but useful
                extra = f" reason={reason}" if reason else ""
                print(f"  - {status} file={fn or '(no-filename)'} ref={ref}{extra}")
        except Exception as e:
            err += 1
            print(f"[ERR] checkin_id={checkin_id} {e}")

        if total % 25 == 0:
            print(f"[PROGRESS] total={total} ok={ok} err={err}")

    print(f"[DONE] total={total} ok={ok} err={err}")
    return 0 if err == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))