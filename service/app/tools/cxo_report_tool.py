from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

import psycopg2
import psycopg2.extras

from ..config import Settings
from .sheets_tool import SheetsTool


def _cf(x: object) -> str:
    return str(x or "").strip().casefold()


def _clean(x: object) -> str:
    return str(x or "").strip()


def _parse_dispatch_ddmm(s: str) -> str:
    t = (s or "").strip()
    if not t:
        return ""
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(t, fmt)
            return dt.strftime("%d/%m")
        except Exception:
            pass
    return t


def _is_mfg_status(s: str) -> bool:
    v = _cf(s)
    return v in ("mfg", "manufacturing", "in mfg", "in manufacturing")


@dataclass(frozen=True)
class Assembly:
    tenant_id: str              # company_row_id
    legacy_id: str
    project_name: str
    part_number: str
    part_name: str
    dispatch_date: str
    internal_poc: str
    vendor_poc: str
    status_assembly: str        # Project.Status_assembly


class CXOReportTool:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _conn(self):
        return psycopg2.connect(self.settings.database_url)

    # -------------------------
    # Assemblies (from Sheets)
    # -------------------------

    def load_all_assemblies(self, sheets: SheetsTool) -> List[Assembly]:
        rows = sheets.list_projects()
        out: List[Assembly] = []

        for r in rows:
            tenant_id = _clean(r.get("company row id"))
            project_name = _clean(r.get("project name"))
            part_number = _clean(r.get("part number"))
            legacy_id = _clean(r.get("id"))
            dispatch_date = _clean(r.get("dispatch date"))
            internal_poc = _clean(r.get("internal poc"))

            status_assembly = _clean(r.get("status_assembly"))

            part_name = _clean(r.get("part name")) or _clean(r.get("part")) or _clean(r.get("part title"))
            vendor_poc = (
                _clean(r.get("vendor/supplier poc"))
                or _clean(r.get("vendor poc"))
                or _clean(r.get("supplier poc"))
            )

            if not tenant_id or not legacy_id or not project_name or not part_number:
                continue

            # STRICT: only manufacturing
            if not _is_mfg_status(status_assembly):
                continue

            out.append(
                Assembly(
                    tenant_id=tenant_id,
                    legacy_id=legacy_id,
                    project_name=project_name,
                    part_number=part_number,
                    part_name=part_name,
                    dispatch_date=dispatch_date,
                    internal_poc=internal_poc,
                    vendor_poc=vendor_poc,
                    status_assembly=status_assembly,
                )
            )

        return out

    # -------------------------
    # Today window (IST)
    # -------------------------

    @staticmethod
    def today_window_ist(*, now_ist: Optional[datetime] = None) -> Tuple[datetime, datetime]:
        try:
            from zoneinfo import ZoneInfo

            tz = ZoneInfo("Asia/Kolkata")
            now = now_ist.astimezone(tz) if now_ist else datetime.now(tz)
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            return start, now
        except Exception:
            now = now_ist or datetime.now()
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            return start, now

    @staticmethod
    def last_n_days_window_ist(*, days: int = 3, now_ist: Optional[datetime] = None) -> Tuple[datetime, datetime]:
        """
        Returns (start_ts, now_ts) in IST for last N days.
        start_ts = IST midnight (00:00) of (today - (days-1)).
        Example: days=3 => start is midnight 2 days ago.
        """
        try:
            from zoneinfo import ZoneInfo

            tz = ZoneInfo("Asia/Kolkata")
            now = now_ist.astimezone(tz) if now_ist else datetime.now(tz)
            # midnight today
            start_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
            start = start_today.replace()  # copy
            # go back (days-1) midnights
            delta_days = max(1, int(days or 3)) - 1
            if delta_days:
                from datetime import timedelta
                start = start_today - timedelta(days=delta_days)
            return start, now
        except Exception:
            from datetime import timedelta
            now = now_ist or datetime.now()
            start_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
            delta_days = max(1, int(days or 3)) - 1
            start = start_today - timedelta(days=delta_days)
            return start, now
    # -------------------------
    # Today fetch (DB) - per legacy_id (bounded payload)
    # -------------------------

    def fetch_checkins_since_for_legacy(
        self,
        *,
        tenant_id: str,
        legacy_id: str,
        start_ts: datetime,
        limit: int = 400,
    ) -> List[Dict[str, Any]]:
        if not legacy_id:
            return []

        sql = """
        SELECT
          checkin_id,
          vector_type,
          summary_text,
          project_name,
          part_number,
          legacy_id,
          status,
          updated_at
        FROM incident_vectors
        WHERE tenant_id=%s
          AND legacy_id=%s
          AND updated_at >= %s
        ORDER BY updated_at DESC
        LIMIT %s
        """
        with self._conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (tenant_id, legacy_id, start_ts, int(limit)))
            return list(cur.fetchall() or [])

    def fetch_project_updates_since_for_legacy(
        self,
        *,
        tenant_id: str,
        legacy_id: str,
        start_ts: datetime,
        limit: int = 400,
    ) -> List[Dict[str, Any]]:
        if not legacy_id:
            return []

        sql = """
        SELECT
          update_message,
          project_name,
          part_number,
          legacy_id,
          updated_at
        FROM dashboard_vectors
        WHERE tenant_id=%s
          AND legacy_id=%s
          AND updated_at >= %s
        ORDER BY updated_at DESC
        LIMIT %s
        """
        with self._conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (tenant_id, legacy_id, start_ts, int(limit)))
            return list(cur.fetchall() or [])
    # -------------------------
    # Low visibility (strict)
    # -------------------------

    @staticmethod
    def compute_low_visibility(
        *,
        assemblies: List[Assembly],
        checkins: List[Dict[str, Any]],
        updates: List[Dict[str, Any]],
    ) -> List[Assembly]:
        present_keys: set[str] = set()

        def add_key(part_number: str, part_name: str):
            pn = _cf(part_number)
            if pn:
                present_keys.add(f"pn:{pn}")
            nm = _cf(part_name)
            if nm:
                present_keys.add(f"nm:{nm}")

        for c in checkins or []:
            add_key(_clean(c.get("part_number")), _clean(c.get("part_name")))

        for u in updates or []:
            add_key(_clean(u.get("part_number")), _clean(u.get("part_name")))

        low: List[Assembly] = []
        for a in assemblies:
            pn = _cf(a.part_number)
            nm = _cf(a.part_name)
            hit = (pn and f"pn:{pn}" in present_keys) or (nm and f"nm:{nm}" in present_keys)
            if not hit:
                low.append(a)

        return low

    @staticmethod
    def low_visibility_html(low: List[Assembly]) -> str:
        if not low:
            return "<ul><li>No low visibility assemblies today.</li></ul>"

        groups: Dict[str, List[Assembly]] = {}
        for a in low:
            groups.setdefault(a.project_name or "Unknown Project", []).append(a)

        html = ["<ul>"]
        for proj in sorted(groups.keys(), key=lambda x: x.casefold()):
            html.append(f"<li>{proj}<ul>")
            items = groups[proj]
            items.sort(key=lambda x: (x.part_name.casefold(), x.part_number.casefold(), x.legacy_id.casefold()))
            for a in items:
                ddmm = _parse_dispatch_ddmm(a.dispatch_date)
                part_name = a.part_name or "Part"
                html.append(f"<li>{part_name} – {a.part_number} – Dispatch on {ddmm}</li>")
            html.append("</ul></li>")
        html.append("</ul>")
        return "".join(html)

    # -------------------------
    # Prompt inputs formatting
    # -------------------------

    @staticmethod
    def assemblies_to_prompt_json(assemblies: List[Assembly]) -> List[Dict[str, Any]]:
        return [
            {
                "legacy_id": a.legacy_id,
                "project_name": a.project_name,
                "part_name": a.part_name,
                "part_number": a.part_number,
                "dispatch_date": a.dispatch_date,
                "vendor_poc": a.vendor_poc,
                "internal_poc": a.internal_poc,
            }
            for a in assemblies
        ]

    @staticmethod
    def db_checkins_to_prompt_json(rows: List[Dict[str, Any]], assemblies_by_legacy: Dict[str, Assembly]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in rows or []:
            lid = _clean(r.get("legacy_id"))
            a = assemblies_by_legacy.get(lid)
            out.append(
                {
                    "checkin_id": _clean(r.get("checkin_id")),
                    "project_name": _clean(r.get("project_name")) or (a.project_name if a else ""),
                    "part_number": _clean(r.get("part_number")) or (a.part_number if a else ""),
                    "part_name": (a.part_name if a else ""),
                    "status": _clean(r.get("status")),
                    "vector_type": _clean(r.get("vector_type")),
                    "description": _clean(r.get("summary_text")),
                }
            )
        return out

    @staticmethod
    def db_updates_to_prompt_json(rows: List[Dict[str, Any]], assemblies_by_legacy: Dict[str, Assembly]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in rows or []:
            lid = _clean(r.get("legacy_id"))
            a = assemblies_by_legacy.get(lid)
            out.append(
                {
                    "project_name": _clean(r.get("project_name")) or (a.project_name if a else ""),
                    "part_number": _clean(r.get("part_number")) or (a.part_number if a else ""),
                    "part_name": (a.part_name if a else ""),
                    "description": _clean(r.get("update_message")),
                    "added_by": "",
                }
            )
        return out