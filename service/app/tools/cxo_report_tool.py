from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Literal
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

        # Window by created_at (stable) instead of updated_at (can be bumped by ingestion/upserts).
        sql = """
        SELECT
        checkin_id,
        vector_type,
        summary_text,
        project_name,
        part_number,
        legacy_id,
        status,
        created_at,
        updated_at
        FROM incident_vectors
        WHERE tenant_id=%s
        AND legacy_id=%s
        AND created_at >= %s
        ORDER BY created_at DESC
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

        # Window by created_at. Do NOT select updated_at here (column may not exist in some DBs).
        sql = """
        SELECT
        update_message,
        project_name,
        part_number,
        legacy_id,
        created_at
        FROM dashboard_vectors
        WHERE tenant_id=%s
        AND legacy_id=%s
        AND created_at >= %s
        ORDER BY created_at DESC
        LIMIT %s
        """
        with self._conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (tenant_id, legacy_id, start_ts, int(limit)))
            return list(cur.fetchall() or [])


    def fetch_checkins_since_for_many(
        self,
        *,
        keys: List[Tuple[str, str]],  # [(tenant_id, legacy_id), ...]
        start_ts: datetime,
        limit_per_key: int = 400,
        chunk_keys: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Fetch checkins for MANY (tenant_id, legacy_id) pairs.

        IMPORTANT: Uses created_at for windowing to avoid updated_at bumps polluting last-N-days.
        """
        keys = [
            (str(t or "").strip(), str(l or "").strip())
            for (t, l) in (keys or [])
            if str(t or "").strip() and str(l or "").strip()
        ]
        if not keys:
            return []

        sql = """
        WITH wanted(tenant_id, legacy_id) AS (
        SELECT * FROM unnest(%s::text[], %s::text[])
        ),
        ranked AS (
        SELECT
            v.checkin_id,
            v.vector_type,
            v.summary_text,
            v.project_name,
            v.part_number,
            v.legacy_id,
            v.status,
            v.created_at,
            v.updated_at,
            v.tenant_id,
            row_number() OVER (PARTITION BY v.tenant_id, v.legacy_id ORDER BY v.created_at DESC) AS rn
        FROM incident_vectors v
        JOIN wanted w
            ON w.tenant_id=v.tenant_id AND w.legacy_id=v.legacy_id
        WHERE v.created_at >= %s
        )
        SELECT
        checkin_id, vector_type, summary_text, project_name, part_number, legacy_id, status, created_at, updated_at, tenant_id
        FROM ranked
        WHERE rn <= %s
        ORDER BY created_at DESC
        """

        out: List[Dict[str, Any]] = []

        def chunks(xs: List[Tuple[str, str]], n: int) -> List[List[Tuple[str, str]]]:
            if n <= 0:
                n = 200
            return [xs[i : i + n] for i in range(0, len(xs), n)]

        for ck in chunks(keys, chunk_keys):
            tids = [t for (t, _) in ck]
            lids = [l for (_, l) in ck]
            with self._conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (tids, lids, start_ts, int(limit_per_key)))
                out.extend(list(cur.fetchall() or []))

        return out


    def fetch_project_updates_since_for_many(
        self,
        *,
        keys: List[Tuple[str, str]],  # [(tenant_id, legacy_id), ...]
        start_ts: datetime,
        limit_per_key: int = 400,
        chunk_keys: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Fetch project updates for MANY (tenant_id, legacy_id) pairs.

        IMPORTANT: Uses created_at for windowing to avoid updated_at backfills/upserts polluting last-N-days.
        """
        keys = [
            (str(t or "").strip(), str(l or "").strip())
            for (t, l) in (keys or [])
            if str(t or "").strip() and str(l or "").strip()
        ]
        if not keys:
            return []

        # Do NOT select updated_at here (column may not exist in some DBs).
        sql = """
        WITH wanted(tenant_id, legacy_id) AS (
        SELECT * FROM unnest(%s::text[], %s::text[])
        ),
        ranked AS (
        SELECT
            v.update_message,
            v.project_name,
            v.part_number,
            v.legacy_id,
            v.created_at,
            v.tenant_id,
            row_number() OVER (PARTITION BY v.tenant_id, v.legacy_id ORDER BY v.created_at DESC) AS rn
        FROM dashboard_vectors v
        JOIN wanted w
            ON w.tenant_id=v.tenant_id AND w.legacy_id=v.legacy_id
        WHERE v.created_at >= %s
        )
        SELECT
        update_message, project_name, part_number, legacy_id, created_at, tenant_id
        FROM ranked
        WHERE rn <= %s
        ORDER BY created_at DESC
        """

        out: List[Dict[str, Any]] = []

        def chunks(xs: List[Tuple[str, str]], n: int) -> List[List[Tuple[str, str]]]:
            if n <= 0:
                n = 200
            return [xs[i : i + n] for i in range(0, len(xs), n)]

        for ck in chunks(keys, chunk_keys):
            tids = [t for (t, _) in ck]
            lids = [l for (_, l) in ck]
            with self._conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (tids, lids, start_ts, int(limit_per_key)))
                out.extend(list(cur.fetchall() or []))

        return out
    # -------------------------
    # Low visibility (strict)
    # -------------------------

    @staticmethod
    def compute_low_visibility(
        *,
        assemblies: List[Assembly],
        checkins: List[Dict[str, Any]],
        updates: List[Dict[str, Any]],
        mode: Literal["window", "today"] = "window",
        today_start_ist: Optional[datetime] = None,
        now_ist: Optional[datetime] = None,
    ) -> List[Assembly]:
        """
        Low visibility parity with prompt set logic:

        LOW = ALL_ASSEMBLIES − (VISIBLE_BY_CHECKINS ∪ VISIBLE_BY_UPDATES)

        Visibility rule (strict keying):
        - If assembly has part_number -> match by part_number ONLY (primary).
        - Else -> fallback match by part_name.

        Mode:
        - "window": assumes caller already passed window-filtered events (default).
        - "today": further filters events by their created_at timestamps within today IST window.
            This requires events to carry a "created_at" field (datetime) or an ISO string.
        """

        def _dt(v: object) -> Optional[datetime]:
            if v is None:
                return None
            if isinstance(v, datetime):
                return v
            s = str(v).strip()
            if not s:
                return None
            # best-effort parse (supports ISO / TIMESTAMPTZ-ish strings)
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return None

        # If mode == today, we need a (start, end) IST window.
        if mode == "today":
            if today_start_ist is None or now_ist is None:
                today_start_ist, now_ist = CXOReportTool.today_window_ist(now_ist=now_ist)

            def _in_today(ev: Dict[str, Any]) -> bool:
                ts = _dt(ev.get("created_at"))
                if ts is None:
                    # Fail-closed: if event has no timestamp, it should NOT make something "visible today".
                    return False
                try:
                    return ts >= today_start_ist and ts <= now_ist
                except Exception:
                    return False

            checkins = [c for c in (checkins or []) if _in_today(c)]
            updates = [u for u in (updates or []) if _in_today(u)]

        # Build visibility sets for strict keying
        visible_pn: set[str] = set()
        visible_nm: set[str] = set()

        def add_visible(ev: Dict[str, Any]) -> None:
            pn = _cf(ev.get("part_number"))
            if pn:
                visible_pn.add(pn)
            nm = _cf(ev.get("part_name"))
            if nm:
                visible_nm.add(nm)

        for c in checkins or []:
            add_visible(c)
        for u in updates or []:
            add_visible(u)

        low: List[Assembly] = []

        for a in assemblies or []:
            pn = _cf(a.part_number)
            nm = _cf(a.part_name)

            # strict keying:
            # if pn exists -> ONLY pn decides visibility
            # else fallback to name
            if pn:
                is_visible = pn in visible_pn
            else:
                is_visible = bool(nm and nm in visible_nm)

            if not is_visible:
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
    
    # -------------------------
    # Prompt inputs formatting (GLOBAL: key = (tenant_id, legacy_id))
    # -------------------------

    @staticmethod
    def db_checkins_to_prompt_json_global(
        rows: List[Dict[str, Any]],
        assemblies_by_key: Dict[Tuple[str, str], Assembly],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in rows or []:
            tid = _clean(r.get("tenant_id"))
            lid = _clean(r.get("legacy_id"))
            a = assemblies_by_key.get((tid, lid))
            out.append(
                {
                    "checkin_id": _clean(r.get("checkin_id")),
                    "project_name": _clean(r.get("project_name")) or (a.project_name if a else ""),
                    "part_number": _clean(r.get("part_number")) or (a.part_number if a else ""),
                    "part_name": (a.part_name if a else ""),
                    "status": _clean(r.get("status")),
                    "vector_type": _clean(r.get("vector_type")),
                    "description": _clean(r.get("summary_text")),
                    "created_at": r.get("created_at"),  # used only for low-visibility (today mode)
                }
            )
        return out


    @staticmethod
    def db_updates_to_prompt_json_global(
        rows: List[Dict[str, Any]],
        assemblies_by_key: Dict[Tuple[str, str], Assembly],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in rows or []:
            tid = _clean(r.get("tenant_id"))
            lid = _clean(r.get("legacy_id"))
            a = assemblies_by_key.get((tid, lid))
            out.append(
                {
                    "project_name": _clean(r.get("project_name")) or (a.project_name if a else ""),
                    "part_number": _clean(r.get("part_number")) or (a.part_number if a else ""),
                    "part_name": (a.part_name if a else ""),
                    "description": _clean(r.get("update_message")),
                    "added_by": "",
                    "created_at": r.get("created_at"),  # used only for low-visibility (today mode)
                }
            )
        return out