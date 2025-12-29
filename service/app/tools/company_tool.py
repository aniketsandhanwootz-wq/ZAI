from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import re

from ..config import Settings
from ..integrations.glide_client import GlideClient, CompanyProfile


def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or ""


@dataclass
class CompanyContext:
    tenant_row_id: str
    company_key: str          # stable routing key for Teams channel naming
    company_name: str
    company_description: str


class CompanyTool:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.glide = GlideClient(settings)

    def get_company_context(self, tenant_row_id: str) -> Optional[CompanyContext]:
        tenant_row_id = (tenant_row_id or "").strip()
        if not tenant_row_id:
            return None

        prof: CompanyProfile | None = None
        try:
            prof = self.glide.get_company_by_row_id(tenant_row_id)
        except Exception:
            prof = None

        name = (prof.name if prof else "").strip()
        desc = (prof.description if prof else "").strip()

        # Routing key preference:
        # 1) slug(company_name) if available
        # 2) tenant_row_id fallback (still stable, but not pretty)
        key = _slug(name) or tenant_row_id

        return CompanyContext(
            tenant_row_id=tenant_row_id,
            company_key=key,
            company_name=name,
            company_description=desc,
        )
