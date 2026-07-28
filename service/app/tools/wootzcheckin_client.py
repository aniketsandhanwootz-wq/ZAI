# service/app/tools/wootzcheckin_client.py
"""
Thin HTTP client for wootzcheckin's `/internal/*` CQTS API — the only way
this pipeline reads or writes wootzcheckin data (never direct DB access; see
the CQTS plan's "Critical design constraint" section for why).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

import requests

from ..config import Settings

logger = logging.getLogger("zai.wootzcheckin_client")


class WootzCheckinClientError(RuntimeError):
    pass


class WootzCheckinClient:
    def __init__(self, settings: Settings, *, timeout_s: float = 30.0):
        if not settings.wootzcheckin_api_url:
            raise WootzCheckinClientError("WOOTZCHECKIN_API_URL is not configured")
        if not settings.wootzcheckin_api_secret:
            raise WootzCheckinClientError("WOOTZCHECKIN_API_SECRET is not configured")

        self._base_url = settings.wootzcheckin_api_url.rstrip("/")
        self._headers = {"x-cqts-secret": settings.wootzcheckin_api_secret}
        self._timeout_s = timeout_s

    def _get(self, path: str) -> Dict[str, Any]:
        url = f"{self._base_url}{path}"
        resp = requests.get(url, headers=self._headers, timeout=self._timeout_s)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self._base_url}{path}"
        resp = requests.post(url, headers=self._headers, json=payload, timeout=self._timeout_s)
        resp.raise_for_status()
        return resp.json()

    def list_checkins_needing_classification(self) -> List[str]:
        data = self._get("/internal/checkins/needs-cqts-classification")
        return list(data.get("checkinIds") or [])

    def get_checkin_context(self, checkin_id: str) -> Dict[str, Any]:
        return self._get(f"/internal/checkins/{checkin_id}/cqts-context")

    def get_ccp(self, ccp_id: str) -> Dict[str, Any]:
        return self._get(f"/internal/ccps/{ccp_id}")

    def post_classification(self, checkin_id: str, classification: Dict[str, Any]) -> None:
        self._post(f"/internal/checkins/{checkin_id}/cqts-classification", classification)

    def iter_all_checkins(self, *, page_size: int = 200):
        """Cursor-paginated over every checkin (used only by the one-time vector backfill)."""
        cursor = None
        while True:
            path = f"/internal/checkins/all?limit={page_size}"
            if cursor:
                path += f"&cursor={cursor}"
            page = self._get(path)
            for row in page.get("checkins") or []:
                yield row
            cursor = page.get("nextCursor")
            if not cursor:
                return
