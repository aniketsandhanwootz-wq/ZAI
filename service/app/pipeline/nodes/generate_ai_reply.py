# service/app/pipeline/nodes/generate_ai_reply.py
from __future__ import annotations

from typing import Any, Dict, List

from ...config import Settings
from ...tools.llm_tool import LLMTool


def _format_context(state: Dict[str, Any]) -> str:
    parts: List[str] = []

    inc = state.get("similar_incidents") or []
    if inc:
        parts.append("Similar past incidents:")
        for i, item in enumerate(inc, start=1):
            s = (item.get("summary") or "").strip()
            if s:
                parts.append(f"{i}. {s}")

    ccp = state.get("relevant_ccp_chunks") or []
    if ccp:
        parts.append("\nRelevant CCP guidance:")
        for i, item in enumerate(ccp, start=1):
            t = (item.get("text") or "").strip()
            name = (item.get("ccp_name") or "").strip()
            line = f"{i}. {name}: {t}".strip() if name else f"{i}. {t}"
            if t:
                parts.append(line)

    return "\n".join(parts).strip()


def generate_ai_reply(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    tenant_id = (state.get("tenant_id") or "").strip()
    snapshot = (state.get("thread_snapshot_text") or "").strip()

    # ✅ Hard safety: no tenant => do not “guess”
    if not tenant_id:
        state["ai_reply"] = (
            "I couldn't map this check-in to a customer/company (missing Company row id). "
            "Please ensure the Project tab has Company row id filled for this ID, and re-trigger. "
            "Also share: clear inspection photo, measurement reference, and what stage/process this is at."
        )
        (state.get("logs") or []).append("Generated SAFE reply (missing tenant)")
        return state

    llm = LLMTool(settings)
    ctx = _format_context(state)

    prompt = f"""
You are a manufacturing quality assistant.
Given a new check-in and context, write a short actionable reply for the team.

Rules:
- Be practical, step-by-step.
- Mention precautions to avoid repeat issues.
- Ask for missing evidence if needed.
- Do NOT ask for or mention assembly drawings.
- Keep it concise.
- If unsure, propose verification steps not guesses.

CHECKIN:
{snapshot}

CONTEXT:
{ctx}
""".strip()

    state["ai_reply"] = llm.generate_text(prompt).strip()
    (state.get("logs") or []).append("Generated AI reply")
    return state
