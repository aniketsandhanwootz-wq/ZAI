from __future__ import annotations

from typing import Any, Dict
from pathlib import Path

from ...config import Settings
from ...tools.llm_tool import LLMTool


def _repo_root() -> Path:
    # service/app/pipeline/nodes -> repo root
    return Path(__file__).resolve().parents[4]


def _load_prompt_template() -> str:
    p = _repo_root() / "packages" / "prompts" / "checkin_reply.md"
    return p.read_text(encoding="utf-8")


def generate_ai_reply(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    tenant_id = (state.get("tenant_id") or "").strip()
    snapshot = (state.get("thread_snapshot_text") or "").strip()

    # Hard safety: no tenant => do not guess across customers
    if not tenant_id:
        state["ai_reply"] = (
            "I couldn't map this check-in to a customer/company (missing Company row id). "
            "Please fill Project → Company row id for this ID and re-trigger. "
            "Also share: measurement method, stage/process, and 1 clear inspection photo."
        )
        (state.get("logs") or []).append("Generated SAFE reply (missing tenant)")
        return state

    ctx = (state.get("packed_context") or "").strip()
    closure_notes = (state.get("closure_notes") or "").strip()

    template = _load_prompt_template()

    prompt = template.format(
        snapshot=snapshot,
        ctx=ctx,
        closure_notes=closure_notes,
    )

    llm = LLMTool(settings)
    state["ai_reply"] = llm.generate_text(prompt).strip()
    (state.get("logs") or []).append("Generated AI reply (standardized)")
    return state
