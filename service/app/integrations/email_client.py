from __future__ import annotations

import smtplib
from dataclasses import dataclass
from email.mime.text import MIMEText
from email.utils import formatdate
import logging
import time
import random
import re
from typing import List

logger = logging.getLogger("zai.email")
def _parse_recipients(raw: str) -> List[str]:
    """
    Supports comma/semicolon separated lists:
      "a@x.com, b@y.com; c@z.com"
    Returns a de-duplicated, cleaned list.
    """
    s = (raw or "").strip()
    if not s:
        return []
    parts = re.split(r"[;,]+", s)
    out: List[str] = []
    seen = set()
    for p in parts:
        e = (p or "").strip()
        if not e:
            continue
        # minimal sanity
        if "@" not in e:
            continue
        key = e.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out
@dataclass(frozen=True)
class EmailMessage:
    subject: str
    html_body: str
    to_email: str
    from_email: str


class EmailClient:
    """
    SMTP client (Gmail App Password supported).
    """

    def __init__(self, *, host: str, port: int, username: str, password: str, use_starttls: bool = True):
        self.host = (host or "").strip()
        self.port = int(port or 0)
        self.username = (username or "").strip()
        self.password = (password or "").strip()
        self.use_starttls = bool(use_starttls)

        if not self.host:
            raise RuntimeError("SMTP host missing")
        if not self.port:
            raise RuntimeError("SMTP port missing")
        if not self.username:
            raise RuntimeError("SMTP username missing")
        if not self.password:
            raise RuntimeError("SMTP password missing")

    def send_html(self, msg: EmailMessage) -> None:
        recipients = _parse_recipients(msg.to_email)
        if not recipients:
            raise RuntimeError("No valid recipient emails in to_email")

        m = MIMEText(msg.html_body or "", "html", "utf-8")
        m["Subject"] = msg.subject
        m["From"] = msg.from_email
        m["To"] = ", ".join(recipients)
        m["Date"] = formatdate(localtime=True)

        max_attempts = 3
        base_sleep = 1.0

        last_err: Exception | None = None
        for attempt in range(max_attempts):
            try:
                with smtplib.SMTP(self.host, self.port, timeout=60) as smtp:
                    if self.use_starttls:
                        smtp.ehlo()
                        smtp.starttls()
                        smtp.ehlo()
                    smtp.login(self.username, self.password)

                    # IMPORTANT: pass the full recipient list
                    smtp.sendmail(msg.from_email, recipients, m.as_string())

                logger.info("Email sent recipients=%d to=%s subject=%s", len(recipients), ",".join(recipients), msg.subject)
                return

            except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError, smtplib.SMTPHeloError, smtplib.SMTPDataError, TimeoutError) as e:
                last_err = e
                if attempt < max_attempts - 1:
                    d = min(10.0, base_sleep * (2 ** attempt)) * (0.8 + random.random() * 0.4)
                    logger.warning("SMTP retryable error=%s attempt=%d/%d sleep=%.2fs", type(e).__name__, attempt + 1, max_attempts, d)
                    time.sleep(d)
                    continue
                raise

            except Exception as e:
                last_err = e
                logger.exception("SMTP send failed (non-retryable): %s", type(e).__name__)
                raise

        if last_err:
            raise last_err
