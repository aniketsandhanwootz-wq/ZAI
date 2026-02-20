# service/scripts/backfill_checkin_created.py
# Backfill missed AI replies by re-triggering CHECKIN_CREATED events (inline or enqueue).
from __future__ import annotations

import argparse
import time
import sys
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv


def _repo_root() -> Path:
    # .../service/scripts/backfill_checkin_created.py -> parents[2] = repo root
    return Path(__file__).resolve().parents[2]


def _load_env() -> None:
    env_path = _repo_root() / "service" / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
    else:
        load_dotenv(override=True)


def _read_ids_file(path: str) -> List[str]:
    p = Path(path).expanduser()
    if not p.exists():
        raise SystemExit(f"ids file not found: {p}")

    lines = p.read_text(encoding="utf-8").splitlines()
    out: List[str] = []
    for ln in lines:
        s = (ln or "").strip()
        if not s or s.startswith("#"):
            continue
        s = s.split(",")[0].strip()
        if s:
            out.append(s)

    # de-dup preserve order
    seen = set()
    dedup: List[str] = []
    for x in out:
        if x not in seen:
            dedup.append(x)
            seen.add(x)
    return dedup


def _build_payload(checkin_id: str, *, run_source: str) -> Dict[str, object]:
    cid = (checkin_id or "").strip()
    return {
        "event_type": "CHECKIN_CREATED",
        "checkin_id": cid,
        "meta": {
            # ensure reply path
            "force_reply": True,
            # IMPORTANT: do NOT skip ingest for CHECKIN_CREATED backfill
            # otherwise company mapping can fail if state relies on ingest outputs
            "skip_ingest": False,
            "skip_vectors": True,  # keep light
            "ingest_only": False,  # we want reply
            "source": run_source,
            "bypass_idempotency": True,
        },
    }


def main() -> int:
    _load_env()

    ap = argparse.ArgumentParser(description="Backfill CHECKIN_CREATED for given CheckIn IDs.")
    ap.add_argument("--file", required=True, help="Text file with one checkin_id per line")
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep between items (seconds)")
    ap.add_argument("--limit", type=int, default=0, help="Process only first N ids")
    ap.add_argument("--dry-run", action="store_true", help="Print payloads only; do not execute")
    ap.add_argument("--mode", choices=["inline", "enqueue"], default="inline")
    ap.add_argument("--queue", default="default", help="RQ queue name (enqueue only)")
    ap.add_argument("--timeout", type=int, default=900, help="Job timeout seconds (enqueue only)")
    ap.add_argument("--source", default="backfill_checkin_created", help="meta.source")

    args = ap.parse_args()

    ids = _read_ids_file(args.file)
    if args.limit and int(args.limit) > 0:
        ids = ids[: int(args.limit)]

    if not ids:
        print("[backfill] no ids found")
        return 0

    # ensure repo root is on sys.path so `import service...` works reliably
    rr = str(_repo_root())
    if rr not in sys.path:
        sys.path.insert(0, rr)

    ok = 0
    fail = 0
    total = len(ids)

    if args.dry_run:
        for i, cid in enumerate(ids, start=1):
            payload = _build_payload(cid, run_source=str(args.source))
            print(f"[dry-run] {i}/{total} payload={payload}")
        return 0

    if args.mode == "enqueue":
        from service.app.worker_tasks import enqueue_event_task as _enqueue_event_task

        def submit(payload: Dict[str, object]) -> Dict[str, object]:
            return _enqueue_event_task(payload, queue_name=args.queue, job_timeout=int(args.timeout))

    else:
        from service.app.worker_tasks import process_event_task as _process_event_task

        def submit(payload: Dict[str, object]) -> Dict[str, object]:
            return _process_event_task(payload)

    for i, cid in enumerate(ids, start=1):
        payload = _build_payload(cid, run_source=str(args.source))
        try:
            out = submit(payload)
            ok += 1
            if args.mode == "enqueue":
                print(f"[enqueued] {i}/{total} checkin_id={cid} job={out}")
            else:
                print(f"[ok] {i}/{total} checkin_id={cid} run_id={out.get('run_id')}")
        except Exception as e:
            fail += 1
            print(f"[FAIL] {i}/{total} checkin_id={cid} err={e}")

        if args.sleep and float(args.sleep) > 0:
            time.sleep(float(args.sleep))

    print(f"[done] ok={ok} fail={fail} total={total} mode={args.mode}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())