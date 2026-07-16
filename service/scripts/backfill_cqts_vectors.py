"""
backfill_cqts_vectors.py — one-time vector backfill for CQTS (Part 4 of the
CQTS plan). Embeds a PROBLEM vector for every existing checkin, and a
RESOLUTION vector for every resolved checkin with non-empty resolution
comments, upserting into ZAI's own incident_vectors. Cheap, embedding-only —
does NOT call the classify LLM and does NOT write anything back to
wootzcheckin. Must run once before the first daily classification run so
retrieve_context.py isn't cold-starting against an empty retrieval memory.

Usage:
  cd service
  python scripts/backfill_cqts_vectors.py [--limit N] [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import time

from app.config import load_settings
from app.tools.embed_tool import EmbedTool
from app.tools.vector_tool import VectorTool
from app.tools.wootzcheckin_client import WootzCheckinClient

logger = logging.getLogger("zai.backfill_cqts_vectors")


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None, help="Stop after N checkins (for a quick trial run).")
    parser.add_argument("--dry-run", action="store_true", help="Log what would be embedded/upserted without writing.")
    args = parser.parse_args()

    _setup_logging()
    t0 = time.time()

    settings = load_settings()
    client = WootzCheckinClient(settings)
    embedder = EmbedTool(settings)
    vector_db = VectorTool(settings)

    processed = 0
    problem_upserts = 0
    resolution_upserts = 0
    errored = 0

    for row in client.iter_all_checkins(page_size=200):
        if args.limit is not None and processed >= args.limit:
            break
        processed += 1
        checkin_id = row["checkinId"]

        try:
            ctx = client.get_checkin_context(checkin_id)
            checkin = ctx.get("checkin") or {}
            project = ctx.get("project") or {}
            conversations = ctx.get("conversations") or []

            tenant_id = project.get("companyRowId") or ""
            legacy_id = project.get("legacyId") or ""
            project_name = project.get("projectName") or ""
            part_number = project.get("partNumber") or ""
            status = checkin.get("status") or ""

            if not tenant_id:
                logger.warning("checkin_id=%s: no tenant/company_row_id, skipping (cannot vector-scope)", checkin_id)
                continue

            problem_lines = [f"CHECK-IN [{status}]: {checkin.get('description', '')}"]
            for cv in conversations:
                problem_lines.append(
                    f"[{cv.get('timestamp', '')}] {cv.get('addedBy', '')}"
                    f" ({cv.get('status') or 'comment'}): {cv.get('message', '')}"
                )
            problem_text = "\n".join(problem_lines).strip()

            if args.dry_run:
                logger.info("[DRY RUN] checkin_id=%s would upsert PROBLEM vector (%d chars)", checkin_id, len(problem_text))
            else:
                emb = embedder.embed_text(problem_text)
                vector_db.upsert_incident_vector(
                    tenant_id=tenant_id,
                    checkin_id=checkin_id,
                    vector_type="PROBLEM",
                    embedding=emb,
                    project_name=project_name,
                    part_number=part_number,
                    legacy_id=legacy_id,
                    status=status,
                    text=problem_text,
                )
            problem_upserts += 1

            resolution_comments = (checkin.get("resolutionComments") or "").strip()
            if status == "resolved" and resolution_comments:
                resolution_text = (
                    f"{project_name} | {part_number} | CHECKIN {checkin_id}\n"
                    f"RESOLUTION / WHAT WORKED (from conversation):\n{resolution_comments}"
                ).strip()

                if args.dry_run:
                    logger.info("[DRY RUN] checkin_id=%s would upsert RESOLUTION vector (%d chars)", checkin_id, len(resolution_text))
                else:
                    emb_res = embedder.embed_text(resolution_text)
                    vector_db.upsert_incident_vector(
                        tenant_id=tenant_id,
                        checkin_id=checkin_id,
                        vector_type="RESOLUTION",
                        embedding=emb_res,
                        project_name=project_name,
                        part_number=part_number,
                        legacy_id=legacy_id,
                        status=status,
                        text=resolution_text,
                    )
                resolution_upserts += 1

            if processed % 50 == 0:
                logger.info("Progress: processed=%d problem=%d resolution=%d errored=%d", processed, problem_upserts, resolution_upserts, errored)

        except Exception:
            errored += 1
            logger.exception("checkin_id=%s: backfill failed, continuing", checkin_id)

    dt = time.time() - t0
    logger.info(
        "Done%s. processed=%d problem_upserts=%d resolution_upserts=%d errored=%d elapsed=%.1fs",
        " [DRY RUN]" if args.dry_run else "",
        processed,
        problem_upserts,
        resolution_upserts,
        errored,
        dt,
    )


if __name__ == "__main__":
    main()
