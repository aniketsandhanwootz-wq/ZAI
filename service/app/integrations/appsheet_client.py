# service/app/integrations/appsheet_client.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

from ..config import Settings


@dataclass(frozen=True)
class AppSheetCuesConfig:
    app_id: str
    access_key: str
    table_name: str
    base_url: str

    col_cue: str
    col_cue_id: str
    col_id: str
    col_generated_at: str


class AppSheetClient:
    """
    Minimal AppSheet REST caller for adding rows to a table.

    Endpoint format (Add):
      POST https://www.appsheet.com/api/v2/apps/{APP_ID}/tables/{TABLE}/Action?applicationAccessKey={KEY}
      body: {"Action":"Add","Properties":{"Locale":"en-US"},"Rows":[{...}]}
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def enabled(self) -> bool:
        s = self.settings
        return bool(
            (s.appsheet_app_id or "").strip()
            and (s.appsheet_access_key or "").strip()
            and (s.appsheet_cues_table or "").strip()
        )

    def cues_config(self) -> AppSheetCuesConfig:
        s = self.settings
        return AppSheetCuesConfig(
            app_id=(s.appsheet_app_id or "").strip(),
            access_key=(s.appsheet_access_key or "").strip(),
            table_name=(s.appsheet_cues_table or "").strip(),
            base_url=(s.appsheet_base_url or "https://www.appsheet.com").rstrip("/"),

            col_cue=(s.appsheet_cues_col_cue or "Cue").strip(),
            col_cue_id=(s.appsheet_cues_col_cue_id or "Cue ID").strip(),
            col_id=(s.appsheet_cues_col_id or "ID").strip(),
            col_generated_at=(s.appsheet_cues_col_generated_at or "Date").strip(),
        )

    def add_rows(self, *, table_name: str, rows: List[Dict[str, Any]], locale: str = "en-US", timeout: int = 30) -> Any:
        if not self.enabled():
            return None

        cfg = self.cues_config()
        if not table_name:
            table_name = cfg.table_name

        url = (
            f"{cfg.base_url}/api/v2/apps/{cfg.app_id}/tables/{quote(table_name)}/Action"
            f"?applicationAccessKey={cfg.access_key}"
        )

        payload = {
            "Action": "Add",
            "Properties": {"Locale": locale},
            "Rows": rows or [],
        }

        r = requests.post(url, json=payload, timeout=timeout)
        if not r.ok:
            raise RuntimeError(f"AppSheet Add failed: {r.status_code} {r.text}")
        return r.json()

    def add_cues_rows(
        self,
        *,
        legacy_id: str,
        cue_items: List[Dict[str, str]],
        generated_at: str,
        timeout: int = 30,
    ) -> Any:
        """
        cue_items: [{ "cue_id": "...", "cue": "..." }, ...]
        """
        if not self.enabled():
            return None

        cfg = self.cues_config()

        rows: List[Dict[str, Any]] = []
        for it in cue_items or []:
            cue_id = (it.get("cue_id") or "").strip()
            cue = (it.get("cue") or "").strip()
            if not cue_id or not cue:
                continue
            rows.append(
                {
                    cfg.col_cue: cue,
                    cfg.col_cue_id: cue_id,
                    cfg.col_id: (legacy_id or "").strip(),          # AppSheet "ID" column == Project.ID (legacy_id)
                    cfg.col_generated_at: (generated_at or "").strip(),
                }
            )

        if not rows:
            return None

        return self.add_rows(table_name=cfg.table_name, rows=rows, timeout=timeout)