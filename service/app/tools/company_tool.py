from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple
import re

from ..config import Settings
from ..integrations.glide_client import GlideClient, CompanyProfile
from .company_cache_tool import CompanyCacheTool

def _slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or ""

def normalize_company_name(name: str) -> str:
    """
    Normalizes a company name for routing/grouping.
    Example: "Unnati 123" -> "Unnati"
    """
    s = (name or "").strip()
    if not s:
        return ""
    # Reuse same normalization rule as project-name derivation:
    # splits left of separators and strips trailing numeric token
    norm = derive_company_name_from_project_name(s)
    return norm or s


def normalize_company_key(name: str, *, fallback: str = "") -> str:
    """
    Normalizes routing key for Teams channel.
    Example: "Unnati 123" -> "unnati"
    """
    norm_name = normalize_company_name(name)
    key = _slug(norm_name)
    return key or (fallback or "")

# Split project name on common separators, keep left as "company-ish"
_SPLIT_RE = re.compile(r"\s*[-–—|]\s*")  # -, –, —, |
_TRAILING_NUM_RE = re.compile(r"\s*(#?\d+)\s*$")  # " 114" or " #114"


def derive_company_name_from_project_name(project_name: str) -> str:
    """
    Examples:
      "Unnati 114 - sdfggg" -> "Unnati"
      "Gilbert 1 - fgtrbbrt" -> "Gilbert"
      "ACME Corp 12 – something" -> "ACME Corp"
      "Unnati - abc" -> "Unnati"
      "Unnati 114" -> "Unnati"
    """
    s = (project_name or "").strip()
    if not s:
        return ""

    # Take left side of split (company-ish prefix)
    left = _SPLIT_RE.split(s, maxsplit=1)[0].strip()
    left = re.sub(r"\s{2,}", " ", left).strip()

    # Remove trailing numeric token (job number / sequence)
    # "Unnati 114" -> "Unnati"
    left2 = _TRAILING_NUM_RE.sub("", left).strip()

    # If we accidentally removed everything (rare), keep original left
    name = left2 or left
    name = re.sub(r"\s{2,}", " ", name).strip()

    # Guardrails
    if len(name) < 2:
        return ""
    return name


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
        self.cache = CompanyCacheTool(settings)

    def from_project_name(self, project_name: str, *, tenant_row_id: str = "") -> Optional[CompanyContext]:
        """
        Fallback: derive company from Project name.
        This is the routing source of truth if Glide isn't configured or doesn't return a name.
        """
        name = derive_company_name_from_project_name(project_name)
        if not name:
            return None

        key = _slug(name) or _slug(project_name) or (tenant_row_id or "")
        if not key:
            # last resort: make a stable key from the name itself
            key = _slug(name)

        return CompanyContext(
            tenant_row_id=(tenant_row_id or "").strip(),
            company_key=key,
            company_name=name,
            company_description="",
        )

    def get_company_context(self, tenant_row_id: str) -> Optional[CompanyContext]:
        """
        Primary: cached DB (company_profiles).
        Fallback: Glide (if configured), then warm-cache.
        """
        tenant_row_id = (tenant_row_id or "").strip()
        if not tenant_row_id:
            return None

        # 1) cache-first
        cached = self.cache.get(tenant_row_id)
        if cached:
            name = (cached.get("company_name") or "").strip()
            desc = (cached.get("company_description") or "").strip()
            if name or desc:
                key = normalize_company_key(name, fallback=tenant_row_id)
                return CompanyContext(
                    tenant_row_id=tenant_row_id,
                    company_key=key,
                    company_name=name,
                    company_description=desc,
                )

        # 2) fallback: Glide
        prof: CompanyProfile | None = None
        try:
            prof = self.glide.get_company_by_row_id(tenant_row_id)
        except Exception:
            prof = None

        name = (prof.name if prof else "").strip()
        desc = (prof.description if prof else "").strip()

        if not name and not desc:
            return None

        # warm cache best-effort
        try:
            self.cache.upsert(
                tenant_row_id=tenant_row_id,
                company_name=name,
                company_description=desc,
                raw=(prof.raw if prof else {}),
                source="glide",
            )
        except Exception:
            pass

        key = normalize_company_key(name, fallback=tenant_row_id)
        return CompanyContext(
            tenant_row_id=tenant_row_id,
            company_key=key,
            company_name=name,
            company_description=desc,
        )