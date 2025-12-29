from __future__ import annotations

from typing import Any, Dict
from pathlib import Path

from ...config import Settings
from ...tools.llm_tool import LLMTool


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _load_prompt_template() -> str:
    p = _repo_root() / "packages" / "prompts" / "checkin_reply.md"
    return p.read_text(encoding="utf-8")


def _render_template_safe(template: str, vars: Dict[str, str]) -> str:
    """
    Safe renderer: ONLY replaces our known placeholders like {snapshot}, {ctx}, etc.
    Leaves any other braces (e.g., JSON examples) untouched.
    """
    out = template
    for k, v in vars.items():
        out = out.replace("{" + k + "}", v or "")
    return out


def generate_ai_reply(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    tenant_id = (state.get("tenant_id") or "").strip()
    snapshot = (state.get("thread_snapshot_text") or "").strip()

    if not tenant_id:
        state["ai_reply"] = (
            "I couldn't map this check-in to a customer/company (missing Company row id). "
            "Please fill Project → Company row id for this ID and re-trigger. "
            "Also share: measurement method, stage/process, and 1 clear inspection photo."
        )
        state.setdefault("logs", []).append("Generated SAFE reply (missing tenant)")
        return state

    ctx = (state.get("packed_context") or "").strip()
    closure_notes = (state.get("closure_notes") or "").strip()

    company_name = (state.get("company_name") or "").strip()
    company_desc = (state.get("company_description") or "").strip()

    company_context = ""
    if company_name or company_desc:
        company_context = (
            f"COMPANY CONTEXT:\n"
            f"- Company: {company_name or '(unknown)'}\n"
            f"- What matters to them / constraints (from Glide): {company_desc or '(not provided)'}\n"
        ).strip()

    template = _load_prompt_template()

    prompt = _render_template_safe(
        template,
        {
            "snapshot": snapshot,
            "ctx": ctx,
            "closure_notes": closure_notes,
            "company_context": company_context,
        },
    )

    llm = LLMTool(settings)
    state["ai_reply"] = llm.generate_text(prompt).strip()
    state.setdefault("logs", []).append("Generated AI reply (safe-template)")
    return state
