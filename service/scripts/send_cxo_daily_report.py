from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime
from typing import Dict, List, Tuple

from app.config import load_settings
from app.tools.sheets_tool import SheetsTool
from app.tools.cxo_report_tool import CXOReportTool, Assembly
from app.pipeline.nodes.generate_cxo_report import generate_cxo_report_html
from app.integrations.email_client import EmailClient, EmailMessage

logger = logging.getLogger("zai.cxo_report")


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _strip_html_ws(s: str) -> str:
    return (s or "").strip()


def _extract_section_ul_items(html: str, heading_text: str) -> List[str]:
    if not html:
        return []
    pat = re.compile(
        rf"<h3>\s*{re.escape(heading_text)}\s*</h3>\s*(<ul>.*?</ul>)",
        re.IGNORECASE | re.DOTALL,
    )
    m = pat.search(html)
    if not m:
        return []
    ul = m.group(1) or ""
    return re.findall(r"<li>.*?</li>", ul, flags=re.IGNORECASE | re.DOTALL)


def _merge_li_items(*, sections: list[list[str]]) -> list[str]:
    seen = set()
    out: list[str] = []
    for items in sections or []:
        for li in items or []:
            s = (li or "").strip()
            if not s:
                continue
            if s in seen:
                continue
            seen.add(s)
            out.append(s)
    return out


def _build_final_html(
    *,
    header_html: str,
    major_lis: list[str],
    quality_lis: list[str],
    low_visibility_html_ul: str,
    batch_note_lis: list[str],
) -> str:
    major_ul = "<ul><li>No major movements captured.</li></ul>" if not major_lis else "<ul>" + "".join(major_lis) + "</ul>"
    quality_ul = "<ul><li>No quality issues captured.</li></ul>" if not quality_lis else "<ul>" + "".join(quality_lis) + "</ul>"

    notes_ul = ""
    if batch_note_lis:
        notes_ul = "<h3>Report Notes</h3><ul>" + "".join(batch_note_lis) + "</ul>"

    return (
        (header_html or "")
        + "<h3>Major Movements</h3>"
        + major_ul
        + "<h3>Quality Issues Reported</h3>"
        + quality_ul
        + "<h3>Low visibility Assemblies</h3>"
        + (low_visibility_html_ul or "<ul><li>No low visibility assemblies today.</li></ul>")
        + notes_ul
    )


def _sanitize_html_against_foreign_parts(*, html: str, allowed_part_numbers: List[str]) -> str:
    """
    Strict anti-leak guardrail:
    - If a <li> contains a part-number-like token (alnum with digits+letters) that is NOT in allowed_part_numbers,
      drop that <li>.
    - If a <li> contains one of the allowed part numbers, keep it.
    """
    h = html or ""
    allowed = {str(x or "").strip() for x in (allowed_part_numbers or []) if str(x or "").strip()}
    if not h or not allowed:
        return h

    li_items = re.findall(r"<li>.*?</li>", h, flags=re.IGNORECASE | re.DOTALL)
    if not li_items:
        return h

    def li_ok(li: str) -> bool:
        s = li or ""
        for pn in allowed:
            if pn and pn in s:
                return True

        toks = re.findall(r"\b[a-zA-Z0-9\-_/]{4,40}\b", s)
        for tok in toks:
            t = tok.strip()
            if not t:
                continue
            if any(ch.isdigit() for ch in t) and any(ch.isalpha() for ch in t):
                if t not in allowed:
                    return False
        return True

    cleaned_set = {li for li in li_items if li_ok(li)}

    def _rewrite_section(section_heading: str, html_in: str) -> str:
        pat = re.compile(
            rf"(<h3>\s*{re.escape(section_heading)}\s*</h3>\s*)(<ul>.*?</ul>)",
            re.IGNORECASE | re.DOTALL,
        )
        m = pat.search(html_in)
        if not m:
            return html_in
        ul = m.group(2) or ""
        ul_items = re.findall(r"<li>.*?</li>", ul, flags=re.IGNORECASE | re.DOTALL)
        kept = [li for li in ul_items if li in cleaned_set]
        new_ul = "<ul><li>No updates captured.</li></ul>" if not kept else "<ul>" + "".join(kept) + "</ul>"
        return html_in[: m.start(2)] + new_ul + html_in[m.end(2) :]

    out = h
    out = _rewrite_section("Major Movements", out)
    out = _rewrite_section("Quality Issues Reported", out)
    return out


def _json_bytes_estimate(obj: object) -> int:
    # Small/fast estimate to guide batching
    try:
        import json
        return len(json.dumps(obj, ensure_ascii=False).encode("utf-8"))
    except Exception:
        return 0


def _adaptive_batches(
    *,
    assemblies_sorted: List[Assembly],
    global_checkins: List[Dict[str, str]],
    global_updates: List[Dict[str, str]],
    max_payload_bytes: int,
    hard_max_batch: int,
) -> List[List[Assembly]]:
    """
    Adaptive batching heuristic:
    - Keep batches small enough that (assemblies + global checkins/updates filtered to those assemblies) stays under max_payload_bytes.
    - This reduces chance of Gemini context blowup and reduces retries/503 load.
    """
    if max_payload_bytes <= 0:
        max_payload_bytes = 75000
    if hard_max_batch <= 0:
        hard_max_batch = 20

    # Index global events by part_number for fast filtering
    by_pn_checkins: Dict[str, List[Dict[str, str]]] = {}
    by_pn_updates: Dict[str, List[Dict[str, str]]] = {}
    for c in global_checkins or []:
        pn = str(c.get("part_number") or "").strip()
        if pn:
            by_pn_checkins.setdefault(pn, []).append(c)
    for u in global_updates or []:
        pn = str(u.get("part_number") or "").strip()
        if pn:
            by_pn_updates.setdefault(pn, []).append(u)

    batches: List[List[Assembly]] = []
    cur: List[Assembly] = []

    def batch_payload_bytes(test_batch: List[Assembly]) -> int:
        pns = [a.part_number for a in test_batch if (a.part_number or "").strip()]
        chk: List[Dict[str, str]] = []
        upd: List[Dict[str, str]] = []
        for pn in pns:
            chk.extend(by_pn_checkins.get(pn, []))
            upd.extend(by_pn_updates.get(pn, []))

        # estimate only the JSON bits (prompt adds some fixed overhead)
        return (
            _json_bytes_estimate([{
                "legacy_id": a.legacy_id,
                "project_name": a.project_name,
                "part_name": a.part_name,
                "part_number": a.part_number,
                "dispatch_date": a.dispatch_date,
                "vendor_poc": a.vendor_poc,
                "internal_poc": a.internal_poc,
            } for a in test_batch])
            + _json_bytes_estimate(chk)
            + _json_bytes_estimate(upd)
            + 4000
        )

    for a in assemblies_sorted:
        if not cur:
            cur = [a]
            continue

        test = cur + [a]
        if len(test) > hard_max_batch:
            batches.append(cur)
            cur = [a]
            continue

        if batch_payload_bytes(test) <= max_payload_bytes:
            cur = test
            continue

        # too big -> finalize current, start new
        batches.append(cur)
        cur = [a]

    if cur:
        batches.append(cur)

    return batches


def main() -> None:
    _setup_logging()
    t0 = time.time()

    s = load_settings()

    if not getattr(s, "cxo_report_enabled", False):
        logger.info("CXO report disabled. Exiting.")
        return

    to_email = (getattr(s, "cxo_report_to_email", "") or "").strip()
    from_email = (getattr(s, "cxo_report_from_email", "") or "").strip()
    if not to_email or not from_email:
        raise RuntimeError("CXO report TO/FROM email missing")

    smtp_host = (getattr(s, "smtp_host", "") or "").strip()
    smtp_port = int(getattr(s, "smtp_port", 587) or 587)
    smtp_user = (getattr(s, "smtp_user", "") or "").strip()
    smtp_password = (getattr(s, "smtp_password", "") or "").strip()
    smtp_use_starttls = bool(getattr(s, "smtp_use_starttls", True))

    # Controls (env overrides allowed)
    max_payload_bytes = int(os.getenv("CXO_REPORT_MAX_PAYLOAD_BYTES", str(getattr(s, "cxo_report_max_payload_bytes", 75000) or 75000)))
    hard_max_batch = int(os.getenv("CXO_REPORT_HARD_MAX_BATCH", "20") or "20")
    fail_open = (os.getenv("CXO_REPORT_FAIL_OPEN", str(int(bool(getattr(s, "cxo_report_fail_open", True))))) or "1").strip().lower() in ("1", "true", "yes", "y")

    sheets = SheetsTool(s)
    tool = CXOReportTool(s)

    logger.info("Loading Project sheet assemblies (mfg-only).")
    all_assemblies: List[Assembly] = tool.load_all_assemblies(sheets)
    logger.info("MFG assemblies loaded: %d", len(all_assemblies))

    if not all_assemblies:
        logger.info("No MFG assemblies. Exiting.")
        return

    days = int(getattr(s, "cxo_report_days", 3) or 3)
    start_ts, now_ts = tool.last_n_days_window_ist(days=days)
    logger.info(
        "IST window: days=%d start=%s now=%s | windowing_by=created_at (stable; ignores ingestion updated_at bumps)",
        days,
        start_ts.isoformat(),
        now_ts.isoformat(),
    )

    # stable sort
    assemblies_sorted = sorted(
        all_assemblies,
        key=lambda x: (x.project_name.casefold(), x.part_number.casefold(), x.legacy_id.casefold()),
    )

    # map for name fallback
    assemblies_by_key: Dict[Tuple[str, str], Assembly] = {(a.tenant_id, a.legacy_id): a for a in all_assemblies}

    # -------- GLOBAL fetch (single DB pass per table) --------
    logger.info("Fetching DB checkins/updates globally for %d assemblies...", len(all_assemblies))
    checkin_rows = tool.fetch_checkins_since_for_many(
        keys=[(a.tenant_id, a.legacy_id) for a in all_assemblies],
        start_ts=start_ts,
        limit_per_key=400,
    )
    update_rows = tool.fetch_project_updates_since_for_many(
        keys=[(a.tenant_id, a.legacy_id) for a in all_assemblies],
        start_ts=start_ts,
        limit_per_key=400,
    )

    # Convert DB rows -> prompt JSON (global lists)
    checkins_json = tool.db_checkins_to_prompt_json_global(checkin_rows, assemblies_by_key)
    updates_json = tool.db_updates_to_prompt_json_global(update_rows, assemblies_by_key)

    logger.info("Global events: checkins=%d updates=%d", len(checkins_json), len(updates_json))

    # -------- GLOBAL low visibility (computed BEFORE batching) --------
    mode = (os.getenv("CXO_LOW_VISIBILITY_MODE", "window") or "window").strip().lower()
    if mode not in ("window", "today"):
        mode = "window"

    low = tool.compute_low_visibility(
        assemblies=all_assemblies,
        checkins=checkins_json,
        updates=updates_json,
        mode="today" if mode == "today" else "window",
    )
    low_ul = tool.low_visibility_html(low)

    # -------- Adaptive batching ONLY for LLM summarization --------
    batches = _adaptive_batches(
        assemblies_sorted=assemblies_sorted,
        global_checkins=checkins_json,
        global_updates=updates_json,
        max_payload_bytes=max_payload_bytes,
        hard_max_batch=hard_max_batch,
    )
    logger.info("Adaptive batches=%d (max_payload_bytes=%d hard_max_batch=%d)", len(batches), max_payload_bytes, hard_max_batch)

    major_li_batches: list[list[str]] = []
    quality_li_batches: list[list[str]] = []
    notes: list[str] = []

    # Index global events by part_number AND part_name (prompt rule: match by pn primary and/or name)
    by_key_checkins: Dict[str, List[Dict[str, str]]] = {}
    by_key_updates: Dict[str, List[Dict[str, str]]] = {}

    def _k_pn(x: str) -> str:
        return "pn:" + (x or "").strip().casefold()

    def _k_nm(x: str) -> str:
        return "nm:" + (x or "").strip().casefold()

    for c in checkins_json:
        pn = (c.get("part_number") or "").strip()
        nm = (c.get("part_name") or "").strip()
        if pn:
            by_key_checkins.setdefault(_k_pn(pn), []).append(c)
        if nm:
            by_key_checkins.setdefault(_k_nm(nm), []).append(c)

    for u in updates_json:
        pn = (u.get("part_number") or "").strip()
        nm = (u.get("part_name") or "").strip()
        if pn:
            by_key_updates.setdefault(_k_pn(pn), []).append(u)
        if nm:
            by_key_updates.setdefault(_k_nm(nm), []).append(u)

    for bidx, batch in enumerate(batches, start=1):
        t1 = time.time()
        keys = []
        for a in batch:
            pn = (a.part_number or "").strip()
            nm = (a.part_name or "").strip()
            if pn:
                keys.append("pn:" + pn.casefold())
            if nm:
                keys.append("nm:" + nm.casefold())

        seen = set()
        batch_checkins: List[Dict[str, str]] = []
        batch_updates: List[Dict[str, str]] = []

        for k in keys:
            for c in by_key_checkins.get(k, []):
                cid = (c.get("checkin_id") or "") + "|" + (c.get("vector_type") or "") + "|" + (c.get("part_number") or "") + "|" + (c.get("description") or "")
                if cid in seen:
                    continue
                seen.add(cid)
                batch_checkins.append(c)

        for k in keys:
            for u in by_key_updates.get(k, []):
                uid = (u.get("part_number") or "") + "|" + (u.get("description") or "")
                if uid in seen:
                    continue
                seen.add(uid)
                batch_updates.append(u)

        batch_assemblies_json = tool.assemblies_to_prompt_json(batch)

        logger.info(
            "Batch [%d/%d] assemblies=%d checkins=%d updates=%d | LLM call...",
            bidx,
            len(batches),
            len(batch),
            len(batch_checkins),
            len(batch_updates),
        )

        try:
            html = generate_cxo_report_html(
                settings=s,
                all_assemblies=batch_assemblies_json,
                checkins=batch_checkins,
                project_updates=batch_updates,
            )
            allowed_pns = [x.part_number for x in batch if (x.part_number or "").strip()]
            html = _sanitize_html_against_foreign_parts(html=html, allowed_part_numbers=allowed_pns)

            major_li_batches.append(_extract_section_ul_items(html, "Major Movements"))
            quality_li_batches.append(_extract_section_ul_items(html, "Quality Issues Reported"))

        except Exception as e:
            logger.exception("Batch [%d/%d] LLM failed: %s", bidx, len(batches), type(e).__name__)
            msg = f"<li>LLM batch {bidx}/{len(batches)} failed ({type(e).__name__}). Kept low-visibility computed from data.</li>"
            notes.append(msg)
            if not fail_open:
                raise

        logger.info("Batch [%d/%d] done in %.2fs", bidx, len(batches), time.time() - t1)

    major_lis = _merge_li_items(sections=major_li_batches)
    quality_lis = _merge_li_items(sections=quality_li_batches)

    header_html = (
        f"<p><b>Scope:</b> Manufacturing assemblies={len(all_assemblies)} | "
        f"Window: last {days} day(s) (IST) from {start_ts.strftime('%d/%m %H:%M')} to {now_ts.strftime('%d/%m %H:%M')} | "
        f"Time filter: created_at (not updated_at)</p>"
    )

    final_html = _build_final_html(
        header_html=header_html,
        major_lis=major_lis,
        quality_lis=quality_lis,
        low_visibility_html_ul=low_ul,
        batch_note_lis=notes,
    )

    # subject in IST
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.now(ZoneInfo("Asia/Kolkata"))
    except Exception:
        dt = datetime.now()
    subject = f"CXO Daily Manufacturing Report — {dt.strftime('%d/%m/%Y')}"

    logger.info("Sending email to=%s subject=%s", to_email, subject)

    client = EmailClient(
        host=smtp_host,
        port=smtp_port,
        username=smtp_user,
        password=smtp_password,
        use_starttls=smtp_use_starttls,
    )
    client.send_html(
        EmailMessage(
            subject=subject,
            html_body=_strip_html_ws(final_html),
            to_email=to_email,
            from_email=from_email,
        )
    )

    logger.info("Sent. Total time: %.2fs", time.time() - t0)


if __name__ == "__main__":
    main()