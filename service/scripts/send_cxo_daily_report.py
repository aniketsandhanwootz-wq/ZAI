from __future__ import annotations

import logging
import re
import time
from datetime import datetime
from typing import Dict, List

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


def _wrap_company_section(*, company_row_id: str, company_html: str) -> str:
    hdr = f"<h2>Company Row ID: {company_row_id}</h2>"
    return hdr + (company_html or "")


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


def _build_final_html(*, major_lis: list[str], quality_lis: list[str], low_visibility_html_ul: str) -> str:
    major_ul = "<ul><li>No major movements captured.</li></ul>" if not major_lis else "<ul>" + "".join(major_lis) + "</ul>"
    quality_ul = "<ul><li>No quality issues captured.</li></ul>" if not quality_lis else "<ul>" + "".join(quality_lis) + "</ul>"
    return (
        "<h3>Major Movements</h3>"
        + major_ul
        + "<h3>Quality Issues Reported</h3>"
        + quality_ul
        + "<h3>Low visibility Assemblies</h3>"
        + (low_visibility_html_ul or "<ul><li>No low visibility assemblies today.</li></ul>")
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

        # part-number-like tokens heuristic
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


def _chunked(items: List[Assembly], size: int) -> List[List[Assembly]]:
    if size <= 0:
        size = 20
    out: List[List[Assembly]] = []
    for i in range(0, len(items), size):
        out.append(items[i : i + size])
    return out


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

    sheets = SheetsTool(s)
    tool = CXOReportTool(s)

    logger.info("Loading Project sheet assemblies (mfg-only).")
    all_assemblies: List[Assembly] = tool.load_all_assemblies(sheets)
    logger.info("MFG assemblies loaded: %d", len(all_assemblies))

    if not all_assemblies:
        logger.info("No MFG assemblies. Exiting.")
        return

    by_tenant: Dict[str, List[Assembly]] = {}
    for a in all_assemblies:
        by_tenant.setdefault(a.tenant_id, []).append(a)

    days = int(getattr(s, "cxo_report_days", 3) or 3)
    start_ts, now_ts = tool.last_n_days_window_ist(days=days)
    logger.info("IST window: days=%d start=%s now=%s", days, start_ts.isoformat(), now_ts.isoformat())

    batch_size = int(getattr(s, "cxo_report_batch_size", 20) or 20)

    parts: List[str] = []

    for tenant_id in sorted(by_tenant.keys(), key=lambda x: x.casefold()):
        assemblies = by_tenant[tenant_id]
        if not assemblies:
            continue

        logger.info("Company=%s | assemblies=%d", tenant_id, len(assemblies))

        # map once for name fallback
        assemblies_by_legacy: Dict[str, Assembly] = {a.legacy_id: a for a in assemblies}

        # Sort stable
        assemblies_sorted = sorted(
            assemblies,
            key=lambda x: (x.project_name.casefold(), x.part_number.casefold(), x.legacy_id.casefold()),
        )
        batches = _chunked(assemblies_sorted, batch_size)
        logger.info("Company=%s | batch_size=%d | batches=%d", tenant_id, batch_size, len(batches))

        # for global low-visibility (company-wide)
        company_checkins_all: List[Dict[str, str]] = []
        company_updates_all: List[Dict[str, str]] = []

        # for merging major/quality across batches
        major_li_batches: list[list[str]] = []
        quality_li_batches: list[list[str]] = []

        for bidx, batch in enumerate(batches, start=1):
            t1 = time.time()
            logger.info("Company=%s | Batch [%d/%d] | assemblies=%d", tenant_id, bidx, len(batches), len(batch))

            batch_checkins: List[Dict[str, str]] = []
            batch_updates: List[Dict[str, str]] = []

            for a in batch:
                checkin_rows = tool.fetch_checkins_since_for_legacy(
                    tenant_id=tenant_id,
                    legacy_id=a.legacy_id,
                    start_ts=start_ts,
                    limit=400,
                )
                update_rows = tool.fetch_project_updates_since_for_legacy(
                    tenant_id=tenant_id,
                    legacy_id=a.legacy_id,
                    start_ts=start_ts,
                    limit=400,
                )

                cj = tool.db_checkins_to_prompt_json(checkin_rows, assemblies_by_legacy)
                uj = tool.db_updates_to_prompt_json(update_rows, assemblies_by_legacy)

                batch_checkins.extend(cj)
                batch_updates.extend(uj)

                company_checkins_all.extend(cj)
                company_updates_all.extend(uj)

            batch_assemblies_json = tool.assemblies_to_prompt_json(batch)

            logger.info(
                "Company=%s | Batch [%d/%d] | checkins=%d updates=%d | LLM call...",
                tenant_id,
                bidx,
                len(batches),
                len(batch_checkins),
                len(batch_updates),
            )

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

            logger.info("Company=%s | Batch [%d/%d] | done in %.2fs", tenant_id, bidx, len(batches), time.time() - t1)

        # company-wide low visibility computed by code (exhaustive)
        low = tool.compute_low_visibility(
            assemblies=assemblies,
            checkins=company_checkins_all,
            updates=company_updates_all,
        )
        low_ul = tool.low_visibility_html(low)

        major_lis = _merge_li_items(sections=major_li_batches)
        quality_lis = _merge_li_items(sections=quality_li_batches)

        merged_company_html = _build_final_html(
            major_lis=major_lis,
            quality_lis=quality_lis,
            low_visibility_html_ul=low_ul,
        )

        parts.append(_wrap_company_section(company_row_id=tenant_id, company_html=_strip_html_ws(merged_company_html)))

    if not parts:
        logger.info("No company parts produced. Exiting.")
        return

    final_html = "<hr/>".join(parts)

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
            html_body=final_html,
            to_email=to_email,
            from_email=from_email,
        )
    )

    logger.info("Sent. Total time: %.2fs", time.time() - t0)


if __name__ == "__main__":
    main()