from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import requests

from ..config import Settings


@dataclass
class CompanyProfile:
    row_id: str
    name: str
    description: str
    raw: Dict[str, Any]


class GlideClient:
    """
    Uses Glide Tables API (Advanced) queryTables endpoint.
    Docs: https://www.glideapps.com/docs/using-glide-tables-api

    Requirements (env):
      GLIDE_API_KEY
      GLIDE_APP_ID
      GLIDE_COMPANY_TABLE   (e.g. native-table-XXXX)
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def enabled(self) -> bool:
        s = self.settings
        return bool((s.glide_api_key or "").strip() and (s.glide_app_id or "").strip() and (s.glide_company_table or "").strip())

    def get_company_by_row_id(self, row_id: str, *, timeout: int = 30) -> Optional[CompanyProfile]:
        if not self.enabled():
            return None

        row_id = (row_id or "").strip()
        if not row_id:
            return None

        s = self.settings
        url = f"{s.glide_base_url}/api/function/queryTables"

        table = s.glide_company_table
        col_rowid = s.glide_company_rowid_column or "Row ID"

        sql = f'SELECT * FROM "{table}" WHERE "{col_rowid}" = $1 LIMIT 1'
        payload = {
            "appID": s.glide_app_id,
            "queries": [
                {
                    "sql": sql,
                    "params": [row_id],
                }
            ],
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {s.glide_api_key}",
        }

        r = requests.post(url, json=payload, headers=headers, timeout=timeout)
        if not r.ok:
            raise RuntimeError(f"Glide queryTables failed: {r.status_code} {r.text}")

        data = r.json()
        # response is array: one entry per query
        arr = data if isinstance(data, list) else data.get("data") or data.get("results") or None
        if not arr or not isinstance(arr, list):
            return None

        rows = (arr[0] or {}).get("rows") or []
        if not rows:
            return None

        row = rows[0] if isinstance(rows[0], dict) else None
        if not row:
            return None

        name_col = s.glide_company_name_column or "Name"
        desc_col = s.glide_company_desc_column or "Short client description"

        name = str(row.get(name_col, "") or "").strip()
        desc = str(row.get(desc_col, "") or "").strip()

        return CompanyProfile(
            row_id=row_id,
            name=name,
            description=desc,
            raw=row,
        )

    def list_company_rows(self, *, timeout: int = 60) -> list[dict]:
        """
        Fetches all rows from Glide company table using tableName query (no SQL).
        Handles pagination if Glide returns 'next'.
        """
        if not self.enabled():
            return []

        s = self.settings
        url = f"{s.glide_base_url}/api/function/queryTables"

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {s.glide_api_key}",
        }

        out: list[dict] = []
        start_at: str | None = None

        while True:
            q: Dict[str, Any] = {"tableName": s.glide_company_table, "utc": True}
            if start_at:
                q["startAt"] = start_at

            payload = {"appID": s.glide_app_id, "queries": [q]}
            r = requests.post(url, json=payload, headers=headers, timeout=timeout)
            if not r.ok:
                raise RuntimeError(f"Glide queryTables(list) failed: {r.status_code} {r.text}")

            data = r.json()
            arr = data if isinstance(data, list) else (data.get("data") or data.get("results") or [])
            if not arr or not isinstance(arr, list):
                break

            block = arr[0] if isinstance(arr[0], dict) else {}
            rows = block.get("rows") or []
            if isinstance(rows, list):
                out.extend([x for x in rows if isinstance(x, dict)])

            nxt = block.get("next")
            if not nxt:
                break
            start_at = str(nxt)

        return out
