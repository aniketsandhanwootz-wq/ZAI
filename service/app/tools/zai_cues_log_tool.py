from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..config import Settings
from .sheets_tool import SheetsTool


def _now_ist_str() -> str:
    # Match your sheet style like: 01/07/26 12:49 PM
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.now(ZoneInfo("Asia/Kolkata"))
    except Exception:
        dt = datetime.now()
    return dt.strftime("%m/%d/%y %I:%M %p")


def _as_bool_str(x: Any) -> str:
    return "TRUE" if bool(x) else "FALSE"


@dataclass(frozen=True)
class ZaiCuesLogRow:
    timestamp_ist: str
    event_type: str
    run_id: str
    primary_id: str
    idempotency_primary_id: str
    tenant_id: str
    legacy_id: str
    status_assembly: str
    skipped: bool
    skip_reason: str
    rerank_used: bool
    cues10_json: str
    chips: str

    def to_values_row(self) -> List[Any]:
        return [
            self.timestamp_ist,
            self.event_type,
            self.run_id,
            self.primary_id,
            self.idempotency_primary_id,
            self.tenant_id,
            self.legacy_id,
            self.status_assembly,
            _as_bool_str(self.skipped),
            self.skip_reason,
            _as_bool_str(self.rerank_used),
            self.cues10_json,
            self.chips,
        ]


class ZaiCuesLogTool:
    """
    Appends a single row to a dedicated "ZAI_CUES_LOG" tab in a NEW spreadsheet.

    - Best-effort: caller should wrap to avoid breaking main pipeline.
    - Requires the tab to exist with headers already created.
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def enabled(self) -> bool:
        return bool(self.settings.zai_cues_log_enabled) and bool((self.settings.zai_cues_log_spreadsheet_id or "").strip())

    def append_row(self, row: ZaiCuesLogRow) -> None:
        if not self.enabled():
            return

        sheet_id = (self.settings.zai_cues_log_spreadsheet_id or "").strip()
        tab = (self.settings.zai_cues_log_tab_name or "ZAI_CUES_LOG").strip() or "ZAI_CUES_LOG"

        # use dedicated spreadsheet_id override
        sheets = SheetsTool(self.settings, spreadsheet_id=sheet_id)

        # append at A:ZZ (expects header already exists)
        a1 = f"{tab}!A:ZZ"
        sheets._append_values(a1, [row.to_values_row()])  # intentional: internal helper