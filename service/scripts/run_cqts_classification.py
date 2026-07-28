"""
run_cqts_classification.py — daily batch entrypoint for CQTS classification.

Scheduled on Render the same way send_cxo_daily_report.py already is (see
that script's own cron configuration for the pattern to mirror). Polls
wootzcheckin for checkins needing (re)classification, then runs each one
through cqts_graph.py with small bounded concurrency — this is an overnight
batch, not latency-sensitive.

Usage:
  cd service
  python scripts/run_cqts_classification.py [--dry-run] [--concurrency N]
"""

from __future__ import annotations

import argparse
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.config import load_settings
from app.pipeline.cqts_graph import run_cqts_classification_for_checkin
from app.tools.wootzcheckin_client import WootzCheckinClient

logger = logging.getLogger("zai.run_cqts_classification")


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run load_context/investigate/classify but skip the write-back call. "
        "Logs the classification that *would* have been written for manual inspection.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Number of checkins to classify in parallel (default 4).",
    )
    args = parser.parse_args()

    _setup_logging()
    t0 = time.time()

    settings = load_settings()
    client = WootzCheckinClient(settings)

    checkin_ids = client.list_checkins_needing_classification()
    logger.info("Found %d checkin(s) needing CQTS classification", len(checkin_ids))

    if not checkin_ids:
        logger.info("Nothing to do.")
        return

    if args.dry_run:
        logger.info("DRY RUN — classification will run but nothing will be written back.")

    processed = 0
    errored = 0
    results = []

    def _run_one(checkin_id: str):
        return run_cqts_classification_for_checkin(settings, checkin_id, dry_run=args.dry_run)

    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        futures = {pool.submit(_run_one, cid): cid for cid in checkin_ids}
        for future in as_completed(futures):
            checkin_id = futures[future]
            try:
                result = future.result()
            except Exception as e:  # pragma: no cover — run_cqts_classification_for_checkin already catches
                logger.exception("Unexpected error for checkin_id=%s", checkin_id)
                result = {"ok": False, "checkin_id": checkin_id, "error": str(e)}

            results.append(result)
            if result.get("ok"):
                processed += 1
                logger.info("OK checkin_id=%s buckets=%s", checkin_id, result.get("buckets"))
            else:
                errored += 1
                logger.error("FAILED checkin_id=%s error=%s", checkin_id, result.get("error"))

    dt = time.time() - t0
    logger.info(
        "Done. total=%d processed=%d errored=%d elapsed=%.1fs%s",
        len(checkin_ids),
        processed,
        errored,
        dt,
        " [DRY RUN]" if args.dry_run else "",
    )


if __name__ == "__main__":
    main()
