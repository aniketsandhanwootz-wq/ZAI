from __future__ import annotations

import logging
import json
import re
import time
from html import escape
from datetime import datetime
from typing import Dict, List, Tuple

from app.config import load_settings
from app.tools.sheets_tool import SheetsTool
from app.tools.cxo_report_tool import CXOReportTool, Assembly, CXOTableRow
from app.tools.llm_tool import LLMTool
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
    # Small/fast estimate to guide batching (must handle datetime)
    try:
        import json
        from datetime import date, datetime

        def _default(o: object) -> str:
            if isinstance(o, (datetime, date)):
                return o.isoformat()
            return str(o)

        return len(json.dumps(obj, ensure_ascii=False, default=_default).encode("utf-8"))
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


def _is_none_text(v: object) -> bool:
    t = str(v or "").strip().casefold()
    return not t or t in {"none", "na", "n/a", "nil", "-", "--"}


def _split_multiline_items(v: object) -> List[str]:
    s = str(v or "")
    out: List[str] = []
    for ln in s.splitlines():
        x = ln.strip().lstrip("-").strip()
        if not x:
            continue
        out.append(x)
    return out


def _is_email(s: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", (s or "").strip()))


def _email_display_name(email: str) -> str:
    e = (email or "").strip()
    local = e.split("@", 1)[0].strip()
    if not local:
        return e
    parts = [p for p in re.split(r"[._-]+", local) if p]
    if not parts:
        return local.title()
    return " ".join([p.capitalize() for p in parts])


def _render_people_cell(v: str) -> str:
    if _is_none_text(v):
        return "None"
    people_raw = [x.strip() for x in re.split(r"[,;\n]+", v or "") if x.strip()]
    people: List[str] = []
    for p in people_raw:
        if _is_email(p):
            label = escape(_email_display_name(p))
            href = escape(p, quote=True)
            people.append(f"<a href='mailto:{href}'>{label}</a>")
        else:
            people.append(escape(p))
    if not people:
        return "None"
    return "<br/>".join(people)


def _split_ids(v: str) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for tok in re.split(r"[,;\n]+", v or ""):
        x = tok.strip()
        if not x:
            continue
        k = x.casefold()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def _split_project_part_from_id(v: str) -> Tuple[str, str]:
    s = (v or "").strip()
    if not s:
        return "", ""
    if " - " in s:
        left, right = s.split(" - ", 1)
        return left.strip(), right.strip()
    return s, ""


def _render_merged_project_cell(*, ids: List[str], fallback_project: str) -> str:
    project = (fallback_project or "").strip()
    parts: List[str] = []
    seen: set[str] = set()

    for raw_id in ids or []:
        p, part = _split_project_part_from_id(raw_id)
        if not project and p:
            project = p
        token = (part or raw_id).strip()
        if not token:
            continue
        k = token.casefold()
        if k in seen:
            continue
        seen.add(k)
        parts.append(token)

    project_disp = escape(project or "NA")
    if not parts:
        return f"<b>{project_disp}</b>"

    bullets = "".join(f"<li>{escape(x)}</li>" for x in parts)
    return (
        f"<b>{project_disp}</b>"
        "<ul style='margin:4px 0 0 16px; padding:0; list-style-type:disc;'>"
        f"{bullets}"
        "</ul>"
    )


def _format_dispatch_date(v: str) -> str:
    def _one(s: str) -> str:
        t = (s or "").strip()
        if not t:
            return ""
        if "T" in t:
            left = t.split("T", 1)[0].strip()
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", left):
                return left
        try:
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
            return dt.date().isoformat()
        except Exception:
            return t

    parts = [p.strip() for p in re.split(r"[,;\n]+", v or "") if p.strip()]
    if not parts:
        return ""
    return ", ".join([_one(p) for p in parts])


def _render_detail_cell(v: str, *, none_light_red: bool = False) -> str:
    if _is_none_text(v):
        if none_light_red:
            return "<span style='color:#ff9b9b;'>None</span>"
        return "None"
    items = [escape(x) for x in _split_multiline_items(v)]
    if not items:
        if none_light_red:
            return "<span style='color:#ff9b9b;'>None</span>"
        return "None"
    if len(items) == 1:
        return items[0]
    return "<ul style='margin:0; padding-left:18px;'>" + "".join(f"<li>{x}</li>" for x in items) + "</ul>"


def _build_table_report_html(
    *,
    rows: List[CXOTableRow],
    start_ts: datetime,
    now_ts: datetime,
    days: int,
) -> str:
    header_html = (
        f"<p><b>Scope:</b> Manufacturing assemblies={len(rows)} | "
        f"Window: last {days} day(s) (IST) from {start_ts.strftime('%d/%m %H:%M')} to {now_ts.strftime('%d/%m %H:%M')} | "
        f"Time filter: created_at (not updated_at)</p>"
    )

    if not rows:
        return header_html + "<p>No rows found.</p>"

    table_rows: List[str] = []
    for r in rows:
        dispatch_disp = _format_dispatch_date(r.dispatch_date)
        ids = _split_ids(r.legacy_id)
        both_none = _is_none_text(r.major_movements) and _is_none_text(r.quality_issues)
        if both_none:
            project_cell = _render_merged_project_cell(ids=ids, fallback_project=r.project)
        else:
            project_cell = escape(", ".join(ids) if ids else (r.legacy_id or "NA"))
        table_rows.append(
            "<tr>"
            f"<td>{project_cell}</td>"
            f"<td>{escape(dispatch_disp)}</td>"
            f"<td>{_render_detail_cell(r.major_movements, none_light_red=True)}</td>"
            f"<td>{_render_detail_cell(r.quality_issues, none_light_red=True)}</td>"
            f"<td>{_render_people_cell(r.pocs)}</td>"
            f"<td>{_render_people_cell(r.vendor)}</td>"
            "</tr>"
        )

    table_html = (
        "<table border='1' cellpadding='6' cellspacing='0' "
        "style='border-collapse:collapse; width:100%; font-family:Arial,sans-serif; font-size:13px;'>"
        "<colgroup>"
        "<col style='width:20%;'/>"
        "<col style='width:8%;'/>"
        "<col style='width:25%;'/>"
        "<col style='width:25%;'/>"
        "<col style='width:12%;'/>"
        "<col style='width:10%;'/>"
        "</colgroup>"
        "<thead>"
        "<tr>"
        "<th align='left'>Project</th>"
        "<th align='left'>Dispatch Date</th>"
        "<th align='left'>Major Movements</th>"
        "<th align='left'>Quality Issues Reported (if any)</th>"
        "<th align='left'>POCs</th>"
        "<th align='left'>Vendor</th>"
        "</tr>"
        "</thead>"
        "<tbody>"
        + "".join(table_rows)
        + "</tbody></table>"
    )
    return header_html + table_html


def _extract_json_obj(text: str) -> Dict[str, object]:
    s = (text or "").strip()
    if not s:
        return {}

    if s.startswith("```"):
        s = s.strip().strip("`").strip()
        if s.lower().startswith("json"):
            s = s[4:].strip()

    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass

    i = s.find("{")
    j = s.rfind("}")
    if i >= 0 and j > i:
        try:
            obj = json.loads(s[i : j + 1])
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _chunk_rows(rows: List[CXOTableRow], batch_size: int) -> List[List[CXOTableRow]]:
    n = int(batch_size or 20)
    if n <= 0:
        n = 20
    return [rows[i : i + n] for i in range(0, len(rows), n)]


def _llm_batch_prompt(
    *,
    assemblies: List[Dict[str, str]],
    checkins_by_id: Dict[str, List[Dict[str, str]]],
    updates_by_id: Dict[str, List[Dict[str, str]]],
) -> str:
    return (
        "You are preparing CXO table cells for manufacturing updates.\n"
        "For each assembly row, generate concise text for exactly two fields:\n"
        "1) major_movements\n"
        "2) quality_issues\n\n"
        "Rules:\n"
        "- Use only the provided checkins and project updates for that same row id.\n"
        "- Keep language concise and business-readable.\n"
        "- If no content exists for a field, return \"None\".\n"
        "- Return strict JSON only, no markdown.\n\n"
        "Output schema:\n"
        "{\n"
        "  \"rows\": [\n"
        "    {\n"
        "      \"tenant_id\": \"...\",\n"
        "      \"legacy_id\": \"...\",\n"
        "      \"major_movements\": \"...\",\n"
        "      \"quality_issues\": \"...\"\n"
        "    }\n"
        "  ]\n"
        "}\n\n"
        "ASSEMBLIES_JSON:\n"
        + json.dumps(assemblies, ensure_ascii=False)
        + "\n\nCHECKINS_BY_ID_JSON:\n"
        + json.dumps(checkins_by_id, ensure_ascii=False)
        + "\n\nUPDATES_BY_ID_JSON:\n"
        + json.dumps(updates_by_id, ensure_ascii=False)
    )


def _apply_llm_major_quality(
    *,
    settings,
    base_rows: List[CXOTableRow],
    checkin_rows: List[Dict[str, object]],
    update_rows: List[Dict[str, object]],
    batch_size: int,
) -> List[CXOTableRow]:
    """
    Run LLM in fixed-size batches (default 20 rows), then merge results back into rows.
    """
    if not base_rows:
        return []

    by_checkins: Dict[Tuple[str, str], List[Dict[str, object]]] = {}
    by_updates: Dict[Tuple[str, str], List[Dict[str, object]]] = {}

    for r in checkin_rows or []:
        key = (str(r.get("tenant_id") or "").strip(), str(r.get("legacy_id") or "").strip())
        if not key[0] or not key[1]:
            continue
        by_checkins.setdefault(key, []).append(r)

    for r in update_rows or []:
        key = (str(r.get("tenant_id") or "").strip(), str(r.get("legacy_id") or "").strip())
        if not key[0] or not key[1]:
            continue
        by_updates.setdefault(key, []).append(r)

    out_rows = list(base_rows)
    idx_by_key: Dict[Tuple[str, str], int] = {(r.tenant_id, r.legacy_id): i for i, r in enumerate(out_rows)}

    llm = LLMTool(settings)
    batches = _chunk_rows(out_rows, batch_size=batch_size)
    logger.info("LLM batch runs for CXO rows: batches=%d batch_size=%d", len(batches), int(batch_size or 20))

    for bidx, batch in enumerate(batches, start=1):
        batch_assemblies: List[Dict[str, str]] = []
        checkins_by_id: Dict[str, List[Dict[str, str]]] = {}
        updates_by_id: Dict[str, List[Dict[str, str]]] = {}

        for r in batch:
            key = (r.tenant_id, r.legacy_id)
            id_key = f"{r.tenant_id}|{r.legacy_id}"
            batch_assemblies.append(
                {
                    "tenant_id": r.tenant_id,
                    "legacy_id": r.legacy_id,
                    "project": r.project,
                    "pocs": r.pocs,
                    "vendor": r.vendor,
                    "dispatch_date": r.dispatch_date,
                }
            )
            checkins_by_id[id_key] = [
                {
                    "vector_type": str(x.get("vector_type") or ""),
                    "summary_text": str(x.get("summary_text") or ""),
                    "status": str(x.get("status") or ""),
                }
                for x in by_checkins.get(key, [])
            ]
            updates_by_id[id_key] = [
                {
                    "update_message": str(x.get("update_message") or ""),
                }
                for x in by_updates.get(key, [])
            ]

        prompt = _llm_batch_prompt(
            assemblies=batch_assemblies,
            checkins_by_id=checkins_by_id,
            updates_by_id=updates_by_id,
        )

        logger.info("LLM batch [%d/%d] rows=%d", bidx, len(batches), len(batch))
        try:
            raw = llm.generate_text(prompt)
            obj = _extract_json_obj(raw)
            rows_obj = obj.get("rows") if isinstance(obj, dict) else None
            if not isinstance(rows_obj, list):
                logger.warning("LLM batch [%d/%d] invalid JSON payload; keeping deterministic base values", bidx, len(batches))
                continue

            for it in rows_obj:
                if not isinstance(it, dict):
                    continue
                tid = str(it.get("tenant_id") or "").strip()
                lid = str(it.get("legacy_id") or "").strip()
                if not tid or not lid:
                    continue
                idx = idx_by_key.get((tid, lid))
                if idx is None:
                    continue
                cur = out_rows[idx]
                maj = str(it.get("major_movements") or "").strip() or cur.major_movements
                qua = str(it.get("quality_issues") or "").strip() or cur.quality_issues
                out_rows[idx] = CXOTableRow(
                    tenant_id=cur.tenant_id,
                    legacy_id=cur.legacy_id,
                    project=cur.project,
                    pocs=cur.pocs,
                    vendor=cur.vendor,
                    dispatch_date=cur.dispatch_date,
                    major_movements=maj,
                    quality_issues=qua,
                )
        except Exception as e:
            logger.exception("LLM batch [%d/%d] failed: %s; using deterministic base values", bidx, len(batches), type(e).__name__)
            continue

    return out_rows


def main() -> None:
    _setup_logging()
    t0 = time.time()

    s = load_settings()

    if not getattr(s, "cxo_report_enabled", False):
        logger.info("CXO report disabled. Exiting.")
        return

    to_email = (getattr(s, "cxo_report_to_email", "") or "").strip()
    if not to_email:
        raise RuntimeError("CXO_REPORT_TO_EMAIL missing (can be comma-separated)")
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

    rows = tool.build_cxo_table_rows(
        assemblies=assemblies_sorted,
        checkin_rows=checkin_rows,
        update_rows=update_rows,
    )
    batch_size = 20
    rows = _apply_llm_major_quality(
        settings=s,
        base_rows=rows,
        checkin_rows=checkin_rows,
        update_rows=update_rows,
        batch_size=batch_size,
    )
    rows = tool.merge_rows_when_both_none(rows)
    logger.info("Table rows after merge=%d", len(rows))
    final_html = _build_table_report_html(
        rows=rows,
        start_ts=start_ts,
        now_ts=now_ts,
        days=days,
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
