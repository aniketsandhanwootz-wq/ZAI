# service/app/integrations/appsheet_client.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List
from urllib.parse import quote

import requests

from ..config import Settings
import time

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

    Recommended endpoint (Add):
      POST https://api.appsheet.com/api/v2/apps/{APP_ID}/tables/{TABLE}/Action

    Header:
      ApplicationAccessKey: <KEY>

    Body:
      {"Action":"Add","Properties":{"Locale":"en-US"},"Rows":[{...}]}
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._session = requests.Session()

    def enabled(self) -> bool:
        s = self.settings
        return bool(
            (s.appsheet_app_id or "").strip()
            and (s.appsheet_access_key or "").strip()
            and (s.appsheet_cues_table or "").strip()
        )

    def cues_config(self) -> AppSheetCuesConfig:
        s = self.settings

        base = (s.appsheet_base_url or "https://api.appsheet.com").strip().rstrip("/")
        # Safety: if user sets www host, swap to api host (AppSheet API expects api.appsheet.com)
        if "www.appsheet.com" in base:
            base = "https://api.appsheet.com"

        return AppSheetCuesConfig(
            app_id=(s.appsheet_app_id or "").strip(),
            access_key=(s.appsheet_access_key or "").strip(),
            table_name=(s.appsheet_cues_table or "").strip(),
            base_url=base,

            col_cue=(s.appsheet_cues_col_cue or "Cue").strip(),
            col_cue_id=(s.appsheet_cues_col_cue_id or "Cue ID").strip(),
            col_id=(s.appsheet_cues_col_id or "ID").strip(),
            col_generated_at=(s.appsheet_cues_col_generated_at or "Date").strip(),
        )
    def _raise_if_appsheet_errors(self, data: Any, *, action: str) -> None:
        """
        AppSheet can return HTTP 200 but still fail row inserts.
        Detect common error shapes and raise.
        """
        if data is None:
            return
        if isinstance(data, dict):
            # Common: {"Errors":[...]} or {"error": "..."} or {"success": false}
            errs = data.get("Errors") or data.get("errors")
            if errs:
                raise RuntimeError(f"AppSheet {action} row errors: {errs}")
            if data.get("error"):
                raise RuntimeError(f"AppSheet {action} error: {data.get('error')}")
            if data.get("success") is False:
                raise RuntimeError(f"AppSheet {action} success=false: {data}")
        # Sometimes AppSheet returns list of per-row results; keep best-effort
        if isinstance(data, list):
            # If any item contains Errors, fail
            for it in data:
                if isinstance(it, dict) and (it.get("Errors") or it.get("error")):
                    raise RuntimeError(f"AppSheet {action} row errors: {it}")
                
    def add_rows(
        self,
        *,
        table_name: str,
        rows: List[Dict[str, Any]],
        locale: str = "en-US",
        timezone: str = "Asia/Kolkata",
        timeout: int = 30,
    ) -> Any:
        if not self.enabled():
            return None

        cfg = self.cues_config()
        table_name = (table_name or cfg.table_name).strip()
        if not table_name:
            raise RuntimeError("AppSheet table_name missing (APPSHEET_CUES_TABLE)")

        url = f"{cfg.base_url}/api/v2/apps/{cfg.app_id}/tables/{quote(table_name)}/Action"

        payload = {
            "Action": "Add",
            "Properties": {"Locale": locale, "Timezone": timezone},
            "Rows": rows or [],
        }

        headers = {
            "ApplicationAccessKey": cfg.access_key,
            "Content-Type": "application/json",
        }

        last_err: Exception | None = None

        # Tiny retry for transient 429/5xx
        for attempt in range(1, 4):
            try:
                r = self._session.post(url, headers=headers, json=payload, timeout=timeout)

                if not r.ok:
                    # fallback: some setups accept query param; try once if header-based fails
                    url2 = url + f"?applicationAccessKey={cfg.access_key}"
                    r2 = self._session.post(url2, json=payload, timeout=timeout)
                    if not r2.ok:
                        raise RuntimeError(
                            f"AppSheet Add failed: {r.status_code} {r.text} | fallback: {r2.status_code} {r2.text}"
                        )
                    data = r2.json()
                    self._raise_if_appsheet_errors(data, action="Add")
                    return data

                data = r.json()
                self._raise_if_appsheet_errors(data, action="Add")
                return data

            except Exception as e:
                last_err = e
                # retry only on likely transient http errors
                msg = str(e)
                if ("429" in msg) or ("502" in msg) or ("503" in msg) or ("504" in msg):
                    time.sleep(0.4 * attempt)
                    continue
                raise

        raise RuntimeError(f"AppSheet Add failed after retries: {last_err}")
    
    def action_rows(
        self,
        *,
        table_name: str,
        action: str,
        rows: List[Dict[str, Any]],
        locale: str = "en-US",
        timezone: str = "Asia/Kolkata",
        timeout: int = 30,
    ) -> Any:
        if not self.enabled():
            return None

        cfg = self.cues_config()
        table_name = (table_name or cfg.table_name).strip()
        if not table_name:
            raise RuntimeError("AppSheet table_name missing (APPSHEET_CUES_TABLE)")

        url = f"{cfg.base_url}/api/v2/apps/{cfg.app_id}/tables/{quote(table_name)}/Action"

        payload = {
            "Action": (action or "").strip(),
            "Properties": {"Locale": locale, "Timezone": timezone},
            "Rows": rows or [],
        }

        headers = {
            "ApplicationAccessKey": cfg.access_key,
            "Content-Type": "application/json",
        }

        last_err: Exception | None = None

        for attempt in range(1, 4):
            try:
                r = self._session.post(url, headers=headers, json=payload, timeout=timeout)

                if not r.ok:
                    url2 = url + f"?applicationAccessKey={cfg.access_key}"
                    r2 = self._session.post(url2, json=payload, timeout=timeout)
                    if not r2.ok:
                        raise RuntimeError(
                            f"AppSheet {action} failed: {r.status_code} {r.text} | fallback: {r2.status_code} {r2.text}"
                        )
                    data = r2.json()
                    self._raise_if_appsheet_errors(data, action=action)
                    return data

                data = r.json()
                self._raise_if_appsheet_errors(data, action=action)
                return data

            except Exception as e:
                last_err = e
                msg = str(e)
                if ("429" in msg) or ("502" in msg) or ("503" in msg) or ("504" in msg):
                    time.sleep(0.4 * attempt)
                    continue
                raise

        raise RuntimeError(f"AppSheet {action} failed after retries: {last_err}")
    
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
                    cfg.col_id: (legacy_id or "").strip(),
                    cfg.col_generated_at: (generated_at or "").strip(),
                }
            )

        if not rows:
            return None

        return self.add_rows(table_name=cfg.table_name, rows=rows, timeout=timeout)
    
    def upsert_cues_rows(
        self,
        *,
        legacy_id: str,
        cue_items: List[Dict[str, str]],
        generated_at: str,
        timeout: int = 30,
    ) -> Any:
        """
        Upsert semantics:
          - Try Add first
          - If Add fails (most commonly "key already exists"), fallback to Edit
        Assumption: AppSheet table Key column is cfg.col_cue_id ("Cue ID").
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
                    cfg.col_id: (legacy_id or "").strip(),
                    cfg.col_generated_at: (generated_at or "").strip(),
                }
            )

        if not rows:
            return None

        # 1) Try Add
        try:
            return self.action_rows(
                table_name=cfg.table_name,
                action="Add",
                rows=rows,
                timeout=timeout,
            )
        except Exception:
            # 2) Fallback Edit (idempotent update when key exists)
            return self.action_rows(
                table_name=cfg.table_name,
                action="Edit",
                rows=rows,
                timeout=timeout,
            )