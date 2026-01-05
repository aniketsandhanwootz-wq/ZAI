from __future__ import annotations

from typing import Any, Dict, Optional
import requests


class TeamsClient:
    """
    Generic POST JSON to a Teams workflow/webhook URL.
    We keep payload simple so you can wire Power Automate later.
    """

    def __init__(self, webhook_url: str):
        self.webhook_url = (webhook_url or "").strip()

    def enabled(self) -> bool:
        return bool(self.webhook_url)

    def post_message(self, payload: Dict[str, Any], *, timeout: int = 20) -> Optional[str]:
        if not self.webhook_url:
            return None

        headers = {"Content-Type": "application/json"}
        r = requests.post(self.webhook_url, json=payload, headers=headers, timeout=timeout)

        if not r.ok:
            raise RuntimeError(f"Webhook POST failed: {r.status_code} {r.text}")

        return r.text
