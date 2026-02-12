# service/app/pipeline/nodes/generate_ai_reply.py
from __future__ import annotations

from typing import Any, Dict, List
from pathlib import Path

from ...config import Settings
from ..lc_runtime import lc_registry, lc_invoke
import base64


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _load_prompt_template() -> str:
    p = _repo_root() / "packages" / "prompts" / "checkin_reply.md"
    return p.read_text(encoding="utf-8")


def _render_template_safe(template: str, vars: Dict[str, str]) -> str:
    out = template
    for k, v in vars.items():
        out = out.replace("{" + k + "}", v or "")
    return out


def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y")


def _normalize_images_defects(raw: Any, image_count: int) -> List[Dict[str, Any]]:
    seen: Dict[int, Dict[str, Any]] = {}
    if isinstance(raw, list):
        for it in raw:
            if not isinstance(it, dict):
                continue
            try:
                idx = int(it.get("image_index"))
            except Exception:
                continue
            if idx < 0 or idx >= image_count:
                continue
            defects = it.get("defects") or []
            if not isinstance(defects, list):
                defects = []
            seen[idx] = {"image_index": idx, "defects": defects}

    out: List[Dict[str, Any]] = []
    for i in range(max(0, int(image_count))):
        out.append(seen.get(i) or {"image_index": i, "defects": []})
    return out


def _b64(b: bytes) -> str:
    return base64.b64encode(b or b"").decode("utf-8")


def generate_ai_reply(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    reg = lc_registry(settings, state)

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

    attachment_context = (state.get("attachment_context") or "").strip()
    if attachment_context:
        attachment_context = "ATTACHMENT CONTEXT (from Files):\n" + attachment_context.strip()

    template = _load_prompt_template()
    prompt = _render_template_safe(
        template,
        {
            "snapshot": snapshot,
            "ctx": ctx,
            "closure_notes": closure_notes,
            "company_context": company_context,
            "attachment_context": attachment_context,
        },
    )

    if attachment_context and "{attachment_context}" not in template:
        prompt = (prompt.strip() + "\n\n" + attachment_context.strip()).strip()

    images = state.get("media_images") or []
    if not isinstance(images, list):
        images = []

    tool_images = []
    for it in images:
        b = it.get("image_bytes")
        if not isinstance(b, (bytes, bytearray)) or not b:
            continue
        tool_images.append(
            {
                "image_index": int(it.get("image_index") or 0),
                "mime_type": str(it.get("mime_type") or "image/jpeg"),
                "image_b64": _b64(bytes(b)),
            }
        )

    out = lc_invoke(
        reg,
        "llm_generate_json_with_images",
        {"prompt": prompt, "images": tool_images, "temperature": 0.0},
        state,
        default=None,
    )

    if not isinstance(out, dict):
        # fallback to text
        txt = lc_invoke(reg, "llm_generate_text", {"prompt": prompt}, state, fatal=True)
        state["ai_reply"] = str(txt or "").strip()
        state["defects_by_image"] = []
        return state

    technical = (out.get("technical_advice") or "").strip()
    if not technical:
        txt = lc_invoke(reg, "llm_generate_text", {"prompt": prompt}, state, fatal=True)
        technical = str(txt or "").strip()

    state["is_critical"] = _to_bool(out.get("is_critical"))
    state["ai_reply"] = technical
    state["defects_by_image"] = _normalize_images_defects(out.get("images"), len(images))

    state.setdefault("logs", []).append("Generated AI reply + defects (single prompt, multimodal via LC tools)")
    return state