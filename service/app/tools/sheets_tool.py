from __future__ import annotations

import re
import time
from typing import Dict, Any, List, Optional, Tuple

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ..config import Settings, parse_service_account_info
from .mapping_tool import load_sheet_mapping, SheetMapping

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _norm_header(x: object) -> str:
    """
    Normalize sheet header cells:
    - replace NBSP
    - collapse whitespace
    - strip
    """
    s = str(x or "").replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _norm_value(x: object) -> str:
    """
    Normalize cell values for matching:
    - replace NBSP
    - collapse whitespace
    - strip
    - convert '123.0' -> '123' (Google Sheets numeric formatting)
    """
    s = str(x or "").replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    return s


def _key(x: object) -> str:
    """Case-insensitive key used for robust dict lookups and joins."""
    return _norm_value(x).casefold()


class SheetsTool:
    """
    Mapping-driven Google Sheets access.

    - Normalizes headers and uses case-insensitive matching to avoid issues with:
      spaces/case/NBSP differences.
    - Caches full-tab scans to avoid quota spam.
    - Retries on common transient errors (429/500/503).
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.map: SheetMapping = load_sheet_mapping()

        info = parse_service_account_info(settings.google_service_account_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        self._svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        self._sheet_id = settings.spreadsheet_id

        # tab_key -> {"tab_name","headers","keys","idx","rows"}
        self._cache: Dict[str, Dict[str, Any]] = {}

    # ---------- Cache helpers ----------

    def refresh_cache(self, tab_key: Optional[str] = None) -> None:
        """Clear cached sheet data (useful if you edit sheet while server runs)."""
        if tab_key is None:
            self._cache.clear()
        else:
            self._cache.pop(tab_key, None)

    # ---------- Low-level helpers ----------

    def _retryable_execute(self, fn, *, max_attempts: int = 4, base_sleep: float = 1.0):
        for attempt in range(max_attempts):
            try:
                return fn()
            except HttpError as e:
                status = getattr(e.resp, "status", None)
                if status in (429, 500, 503) and attempt < max_attempts - 1:
                    time.sleep(base_sleep * (2**attempt))
                    continue
                raise

    def _get_values(self, range_a1: str) -> List[List[Any]]:
        resp = self._retryable_execute(
            lambda: (
                self._svc.spreadsheets()
                .values()
                .get(spreadsheetId=self._sheet_id, range=range_a1)
                .execute()
            )
        )
        return resp.get("values", [])

    def _append_values(self, range_a1: str, rows: List[List[Any]]) -> None:
        self._retryable_execute(
            lambda: (
                self._svc.spreadsheets()
                .values()
                .append(
                    spreadsheetId=self._sheet_id,
                    range=range_a1,
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body={"values": rows},
                )
                .execute()
            )
        )

    def _table(self, tab_key: str) -> Dict[str, Any]:
        """
        Load and cache table scan for a tab_key.
        Returns a dict with:
          - headers: normalized headers
          - keys: casefold keys
          - idx: key->colindex
          - rows: remaining values
        """
        if tab_key in self._cache:
            return self._cache[tab_key]

        tab_name = self.map.tab(tab_key)
        values = self._get_values(f"{tab_name}!A:ZZ")

        if not values:
            t = {"tab_name": tab_name, "headers": [], "keys": [], "idx": {}, "rows": []}
            self._cache[tab_key] = t
            return t

        headers = [_norm_header(h) for h in values[0]]
        keys = [_key(h) for h in headers]
        idx = {keys[i]: i for i in range(len(keys)) if keys[i]}

        rows = values[1:]
        t = {"tab_name": tab_name, "headers": headers, "keys": keys, "idx": idx, "rows": rows}
        self._cache[tab_key] = t
        return t

    def _row_to_dict(self, table: Dict[str, Any], row: List[Any]) -> Dict[str, Any]:
        """
        Row dict keys are CASEFOLD-normalized header keys.
        Example: header "Project Name" => key "project name"
        """
        d: Dict[str, Any] = {}
        keys: List[str] = table.get("keys", [])
        for i, k in enumerate(keys):
            if not k:
                continue
            d[k] = row[i] if i < len(row) else ""
        return d

    def _idx(self, table: Dict[str, Any], col_name: str, tab_key: str) -> int:
        k = _key(col_name)
        idx = table.get("idx", {})
        if k not in idx:
            raise RuntimeError(f"Tab '{self.map.tab(tab_key)}' missing column: '{col_name}'")
        return int(idx[k])

    # ---------- Domain readers ----------

    def list_dashboard_updates(self) -> List[Dict[str, Any]]:
        """
        Returns all rows from Dashboard Updates tab as dicts keyed by casefold headers.
        """
        t = self._table("dashboard_update")
        if not t["headers"]:
            return []
        return [self._row_to_dict(t, r) for r in t["rows"]]

    def get_checkin_by_id(self, checkin_id: str) -> Optional[Dict[str, Any]]:
        t = self._table("checkin")
        if not t["headers"]:
            return None

        key_col = self.map.col("checkin", "checkin_id")
        k = self._idx(t, key_col, "checkin")
        want = _key(checkin_id)

        for r in t["rows"]:
            if k < len(r) and _key(r[k]) == want:
                return self._row_to_dict(t, r)
        return None

    def list_checkins(self) -> List[Dict[str, Any]]:
        t = self._table("checkin")
        if not t["headers"]:
            return []
        return [self._row_to_dict(t, r) for r in t["rows"]]

    def list_projects(self) -> List[Dict[str, Any]]:
        t = self._table("project")
        if not t["headers"]:
            return []
        return [self._row_to_dict(t, r) for r in t["rows"]]

    def get_project_row(self, project_name: str, part_number: str, legacy_id: str) -> Optional[Dict[str, Any]]:
        """
        Slow path (scan).
        Prefer building an index once in ingestion (recommended).
        """
        t = self._table("project")
        if not t["headers"]:
            return None

        col_project = self.map.col("project", "project_name")
        col_part = self.map.col("project", "part_number")
        col_id = self.map.col("project", "legacy_id")

        ip = self._idx(t, col_project, "project")
        inum = self._idx(t, col_part, "project")
        iid = self._idx(t, col_id, "project")

        want_p = _key(project_name)
        want_n = _key(part_number)
        want_i = _key(legacy_id)

        for r in t["rows"]:
            if ip < len(r) and inum < len(r) and iid < len(r):
                if _key(r[ip]) == want_p and _key(r[inum]) == want_n and _key(r[iid]) == want_i:
                    return self._row_to_dict(t, r)
        return None

    def list_ccp(self) -> List[Dict[str, Any]]:
        t = self._table("ccp")
        if not t["headers"]:
            return []
        return [self._row_to_dict(t, r) for r in t["rows"]]

    def get_conversations_for_checkin(self, checkin_id: str) -> List[Dict[str, Any]]:
        t = self._table("conversation")
        if not t["headers"]:
            return []

        col_checkin_id = self.map.col("conversation", "checkin_id")
        ck = self._idx(t, col_checkin_id, "conversation")
        want = _key(checkin_id)

        out: List[Dict[str, Any]] = []
        for r in t["rows"]:
            if ck < len(r) and _key(r[ck]) == want:
                out.append(self._row_to_dict(t, r))
        return out

    # ---------- Writeback ----------

    def append_conversation_ai_comment(self, checkin_id: str, remark: str, status: str, photos: str) -> None:
        """
        Adds a new row into Conversation tab using mapping.writeback.ai_comment
        """
        wb = self.map.writeback.get("ai_comment", {})
        tab_name = wb.get("tab", self.map.tab("conversation"))
        set_cols = wb.get("set_columns", {})

        t = self._table("conversation")
        headers: List[str] = t.get("headers", [])
        idx: Dict[str, int] = t.get("idx", {})
        if not headers:
            raise RuntimeError("Conversation tab has no header row")

        prefix = wb.get("remark_prefix", "")
        row: List[Any] = [""] * len(headers)

        def set_if_exists(mapped_col_key: str, val: str):
            col_name = set_cols.get(mapped_col_key)
            if not col_name:
                return
            k = _key(col_name)
            if k in idx:
                row[idx[k]] = val

        set_if_exists("checkin_id", str(checkin_id))
        set_if_exists("photos", str(photos or ""))
        set_if_exists("remark", f"{prefix}{remark}")
        set_if_exists("status", str(status or ""))

        self._append_values(f"{tab_name}!A:ZZ", [row])
        self.refresh_cache("conversation")
