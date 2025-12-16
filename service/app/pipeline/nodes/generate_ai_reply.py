from ..state import GraphState
from ...config import Settings
from ...tools.llm_tool import LLMTool


def _format_context(state: GraphState) -> str:
    parts = []

    if state.similar_incidents:
        parts.append("Similar past incidents:")
        for i, item in enumerate(state.similar_incidents, start=1):
            parts.append(f"{i}. {item.get('summary','')}".strip())

    if state.relevant_ccp_chunks:
        parts.append("\nRelevant CCP guidance:")
        for i, item in enumerate(state.relevant_ccp_chunks, start=1):
            parts.append(f"{i}. {item.get('text','')}".strip())

    return "\n".join([p for p in parts if p]).strip()


def generate_ai_reply(state: GraphState, config):
    settings: Settings = config["settings"]
    llm = LLMTool(settings)

    snapshot = state.thread_snapshot_text or ""
    ctx = _format_context(state)

    prompt = f"""
You are a manufacturing quality assistant.
Given a new check-in and context, write a short actionable reply for the team.

Rules:
- Be practical, step-by-step.
- Mention precautions to avoid repeat issues.
- Ask for missing evidence if needed.
- Do NOT mention assembly drawings (not provided).
- Keep it concise.

CHECKIN:
{snapshot}

CONTEXT:
{ctx}
""".strip()

    state.ai_reply = llm.generate_text(prompt)
    state.logs.append("Generated AI reply")
    return state
