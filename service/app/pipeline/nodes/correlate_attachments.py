# service/app/pipeline/nodes/correlate_attachments.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from ...config import Settings
from ..lc_runtime import lc_registry, lc_invoke


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _load_prompt_template() -> str:
    p = _repo_root() / "packages" / "prompts" / "attachment_correlation.md"
    return p.read_text(encoding="utf-8")


def _render_template_safe(template: str, vars: Dict[str, str]) -> str:
    out = template
    for k, v in vars.items():
        out = out.replace("{" + k + "}", v or "")
    return out


def _norm(s: str) -> str:
    return (s or "").strip()


def _make_checkin_context(state: Dict[str, Any]) -> str:
    # Keep consistent with analyze_attachments.py
    lines = []
    lines.append(f"tenant_id: {_norm(str(state.get('tenant_id') or ''))}")
    lines.append(f"checkin_id: {_norm(str(state.get('checkin_id') or ''))}")
    lines.append(f"project_name: {_norm(str(state.get('project_name') or ''))}")
    lines.append(f"part_number: {_norm(str(state.get('part_number') or ''))}")
    lines.append(f"legacy_id: {_norm(str(state.get('legacy_id') or ''))}")
    lines.append(f"status: {_norm(str(state.get('checkin_status') or ''))}")
    desc = _norm(str(state.get("checkin_description") or ""))
    if desc:
        lines.append("checkin_description:")
        lines.append(desc)
    snap = _norm(str(state.get("thread_snapshot_text") or ""))
    if snap:
        lines.append("\nthread_snapshot:")
        lines.append(snap[:6000])
    return "\n".join(lines).strip()


def _fallback_compose(items: List[Dict[str, Any]]) -> str:
    # Deterministic fallback if LLM text tool is unavailable / errors
    out: List[str] = []
    out.append("ATTACHMENT_EVIDENCE:")
    for it in items[:6]:
        fn = (it.get("filename") or "").strip()
        doc_type = (it.get("doc_type") or "").strip()
        a = it.get("analysis") if isinstance(it.get("analysis"), dict) else {}
        summ = str(a.get("summary") or "").strip()
        ids = a.get("identifiers") if isinstance(a.get("identifiers"), list) else []
        dates = a.get("dates") if isinstance(a.get("dates"), list) else []
        out.append(f"- {fn} (type={doc_type})")
        if ids:
            out.append("  identifiers: " + "; ".join([str(x).strip() for x in ids if str(x).strip()][:6]))
        if dates:
            out.append("  dates: " + "; ".join([str(x).strip() for x in dates if str(x).strip()][:6]))
        if summ:
            out.append("  summary: " + summ)

    out.append("\nCROSS_FILE_MATCH:\nUnclear (fallback mode).")
    out.append("\nCONFLICTS:\nunclear")
    out.append("\nMISSING_PAGES_OR_DOCS:\nunclear")
    out.append("\nUSE_IN_REPLY_HINT:\nAttachment evidence captured above; verify identifiers/dates before action.")
    return "\n".join(out).strip()


def correlate_attachments(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produces a single combined evidence block across all attachments and overwrites:
      state["attachment_context"]  -> correlated dense evidence (preferred by reply prompt)
    Also stores:
      state["attachment_evidence"] -> same block (explicit)
    """
    reg = lc_registry(settings, state)

    analyzed = state.get("attachments_analyzed") or []
    if not isinstance(analyzed, list) or not analyzed:
        state.setdefault("logs", []).append("correlate_attachments: skip (no attachments_analyzed)")
        state["attachment_evidence"] = ""
        return state

    # Keep only successful ones if possible
    items: List[Dict[str, Any]] = []
    for it in analyzed:
        if not isinstance(it, dict):
            continue
        if it.get("ok") is False:
            continue
        items.append(
            {
                "filename": it.get("filename") or "",
                "doc_type": it.get("doc_type") or "",
                "ref": it.get("ref") or "",
                "analysis": it.get("analysis") if isinstance(it.get("analysis"), dict) else {},
            }
        )

    if not items:
        state.setdefault("logs", []).append("correlate_attachments: skip (no ok items)")
        state["attachment_evidence"] = ""
        return state

    checkin_ctx = _make_checkin_context(state)
    prompt_t = _load_prompt_template()

    # Give the model the per-file analyses (not raw extracted text again)
    files_json = json.dumps(items[:6], ensure_ascii=False)

    prompt = _render_template_safe(
        prompt_t,
        {
            "checkin_context": checkin_ctx,
            "files_json": files_json,
        },
    )

    # Use existing JSON-capable LLM tool (already present in your system).
    # We ask it to return {"text": "<plain-text evidence block>"} so we can extract safely.
    analysis = lc_invoke(
        reg,
        "llm_generate_json_with_images",
        {
            "prompt": (
                prompt
                + "\n\n"
                + "RETURN STRICT JSON ONLY:\n"
                + '{ "text": "..." }\n'
                + "Where text is exactly the required plain-text sections output."
            ),
            "images": [],
            "temperature": 0.0,
        },
        state,
        default=None,
    )

    evidence = ""
    if isinstance(analysis, dict):
        evidence = str(analysis.get("text") or "").strip()
    if not evidence:
        evidence = _fallback_compose(items)

    # This is what checkin_reply.md already consumes as {attachment_context}
    state["attachment_evidence"] = evidence
    state["attachment_context"] = evidence

    state.setdefault("logs", []).append(f"correlate_attachments: ok_files={len(items)}")
    return state