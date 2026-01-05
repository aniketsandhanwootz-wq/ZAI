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

        last_err: Optional[Exception] = None
        # Small exponential backoff: 0.5s, 1s, 2s
        backoffs = [0.5, 1.0, 2.0]

        for attempt, sleep_s in enumerate([0.0] + backoffs):
            try:
                if sleep_s > 0:
                    import time
                    time.sleep(sleep_s)

                r = requests.post(self.webhook_url, json=payload, headers=headers, timeout=timeout)

                # Retry transient status codes
                if r.status_code in (429, 500, 502, 503, 504):
                    raise RuntimeError(f"Transient webhook error: {r.status_code} {r.text}")

                if not r.ok:
                    raise RuntimeError(f"Webhook POST failed: {r.status_code} {r.text}")

                return r.text

            except Exception as e:
                last_err = e

        raise RuntimeError(f"Webhook POST failed after retries: {last_err}")

