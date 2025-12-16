from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from ..config import Settings, parse_service_account_info
from .mapping_tool import load_sheet_mapping, SheetMapping

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class SheetsTool:
    """
    Mapping-driven Google Sheets access.
    MVP uses table scans (A:ZZ) which is fine for small/medium data.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.map: SheetMapping = load_sheet_mapping()

        info = parse_service_account_info(settings.google_service_account_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        self._svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        self._sheet_id = settings.spreadsheet_id

    # ---------- Low-level helpers ----------

    def _get_values(self, range_a1: str) -> List[List[Any]]:
        resp = (
            self._svc.spreadsheets()
            .values()
            .get(spreadsheetId=self._sheet_id, range=range_a1)
            .execute()
        )
        return resp.get("values", [])

    def _append_values(self, range_a1: str, rows: List[List[Any]]) -> None:
        (
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

    def _load_table(self, tab_key: str) -> Tuple[List[str], List[List[Any]]]:
        tab_name = self.map.tab(tab_key)
        values = self._get_values(f"{tab_name}!A:ZZ")
        if not values:
            return [], []
        headers = [str(h).strip() for h in values[0]]
        rows = values[1:]
        return headers, rows

    def _row_to_dict(self, headers: List[str], row: List[Any]) -> Dict[str, Any]:
        d: Dict[str, Any] = {}
        for i, h in enumerate(headers):
            d[h] = row[i] if i < len(row) else ""
        return d

    def _idx(self, headers: List[str], col_name: str, tab_key: str) -> int:
        if col_name not in headers:
            raise RuntimeError(f"Tab '{self.map.tab(tab_key)}' missing column: '{col_name}'")
        return headers.index(col_name)

    # ---------- Domain readers ----------

    def get_checkin_by_id(self, checkin_id: str) -> Optional[Dict[str, Any]]:
        headers, rows = self._load_table("checkin")
        if not headers:
            return None
        key_col = self.map.col("checkin", "checkin_id")
        k = self._idx(headers, key_col, "checkin")

        for r in rows:
            if k < len(r) and str(r[k]).strip() == str(checkin_id).strip():
                return self._row_to_dict(headers, r)
        return None

    def list_checkins(self) -> List[Dict[str, Any]]:
        headers, rows = self._load_table("checkin")
        if not headers:
            return []
        return [self._row_to_dict(headers, r) for r in rows]

    def get_project_row(self, project_name: str, part_number: str, legacy_id: str) -> Optional[Dict[str, Any]]:
        headers, rows = self._load_table("project")
        if not headers:
            return None

        col_project = self.map.col("project", "project_name")
        col_part = self.map.col("project", "part_number")
        col_id = self.map.col("project", "legacy_id")

        ip = self._idx(headers, col_project, "project")
        inum = self._idx(headers, col_part, "project")
        iid = self._idx(headers, col_id, "project")

        for r in rows:
            if (
                str(r[ip]).strip() == str(project_name).strip()
                and str(r[inum]).strip() == str(part_number).strip()
                and str(r[iid]).strip() == str(legacy_id).strip()
            ):
                return self._row_to_dict(headers, r)
        return None

    def list_ccp(self) -> List[Dict[str, Any]]:
        headers, rows = self._load_table("ccp")
        if not headers:
            return []
        return [self._row_to_dict(headers, r) for r in rows]

    def get_conversations_for_checkin(self, checkin_id: str) -> List[Dict[str, Any]]:
        headers, rows = self._load_table("conversation")
        if not headers:
            return []

        col_checkin_id = self.map.col("conversation", "checkin_id")
        ck = self._idx(headers, col_checkin_id, "conversation")

        out: List[Dict[str, Any]] = []
        for r in rows:
            if ck < len(r) and str(r[ck]).strip() == str(checkin_id).strip():
                out.append(self._row_to_dict(headers, r))
        return out

    # ---------- Writeback ----------

    def append_conversation_ai_comment(self, checkin_id: str, remark: str, status: str, photos: str) -> None:
        """
        Adds a new row into Conversation tab using mapping.writeback.ai_comment
        """
        wb = self.map.writeback.get("ai_comment", {})
        tab_name = wb.get("tab", self.map.tab("conversation"))
        set_cols = wb.get("set_columns", {})

        headers, _ = self._load_table("conversation")
        if not headers:
            raise RuntimeError("Conversation tab has no header row")

        prefix = wb.get("remark_prefix", "")
        row = [""] * len(headers)

        def set_if_exists(mapped_col_key: str, val: str):
            col_name = set_cols.get(mapped_col_key)
            if col_name and col_name in headers:
                row[headers.index(col_name)] = val

        set_if_exists("checkin_id", str(checkin_id))
        set_if_exists("photos", str(photos or ""))
        set_if_exists("remark", f"{prefix}{remark}")
        set_if_exists("status", str(status or ""))

        self._append_values(f"{tab_name}!A:ZZ", [row])
