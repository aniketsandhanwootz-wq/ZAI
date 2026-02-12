# service/app/pipeline/nodes/analyze_attachments.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import base64

from ...config import Settings
from ..lc_runtime import lc_registry, lc_invoke


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _load_prompt_template() -> str:
    p = _repo_root() / "packages" / "prompts" / "attachment_analysis.md"
    return p.read_text(encoding="utf-8")


def _render_template_safe(template: str, vars: Dict[str, str]) -> str:
    out = template
    for k, v in vars.items():
        out = out.replace("{" + k + "}", v or "")
    return out


def _norm(s: str) -> str:
    return (s or "").strip()


def _norm_header(s: str) -> str:
    import re
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _find_files_cell(checkin_row: Dict[str, Any]) -> str:
    if not checkin_row:
        return ""
    for k in checkin_row.keys():
        if _norm_header(k) == "files":
            v = str(checkin_row.get(k) or "").strip()
            if v:
                return v

    candidates = {"files", "file", "attachments", "attachment", "documents", "docs"}
    for k in checkin_row.keys():
        if _norm_header(k) in candidates:
            v = str(checkin_row.get(k) or "").strip()
            if v:
                return v
    return ""


def _make_checkin_context(state: Dict[str, Any]) -> str:
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


def analyze_attachments(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    tenant_id = _norm(str(state.get("tenant_id") or ""))
    checkin_id = _norm(str(state.get("checkin_id") or ""))

    if not tenant_id or not checkin_id:
        state.setdefault("logs", []).append("analyze_attachments: skip (missing tenant_id/checkin_id)")
        return state

    reg = lc_registry(settings, state)

    meta = state.get("meta") or {}
    attachments_only = str(meta.get("attachments_only") or "").strip().lower() in ("1", "true", "yes", "y", "on")

    checkin_row = state.get("checkin_row") or {}
    files_cell = ""

    override = meta.get("checkin_files")
    if isinstance(override, list):
        files_cell = "\n".join([str(x).strip() for x in override if str(x).strip()])
    elif isinstance(override, str):
        files_cell = override.strip()

    if not files_cell:
        files_cell = _find_files_cell(checkin_row)

    if not files_cell:
        state.setdefault("logs", []).append("analyze_attachments: no Files found on checkin")
        state["attachment_context"] = ""
        state["attachments_analyzed"] = []
        return state

    refs = lc_invoke(reg, "attachment_split_cell_refs", {"cell": files_cell}, state, default=[]) or []
    if not refs:
        state.setdefault("logs", []).append("analyze_attachments: Files present but no refs parsed")
        state["attachment_context"] = ""
        state["attachments_analyzed"] = []
        return state

    prompt_t = _load_prompt_template()
    checkin_ctx = _make_checkin_context(state)

    analyzed: List[Dict[str, Any]] = []
    ctx_lines: List[str] = []
    processed = 0
    skipped = 0

    max_files = int(meta.get("max_files") or 6)
    max_bytes = int(meta.get("max_bytes") or 15_000_000)

    for ref in refs[:max_files]:
        att = lc_invoke(reg, "attachment_resolve", {"ref": ref}, state, default=None)
        if not isinstance(att, dict) or not att:
            skipped += 1
            continue

        fetch = lc_invoke(
            reg,
            "attachment_fetch_bytes",
            {
                "source_ref": att.get("source_ref") or "",
                "kind": att.get("kind") or "",
                "name": att.get("name") or "",
                "mime_type": att.get("mime_type") or "",
                "is_pdf": bool(att.get("is_pdf")),
                "is_image": bool(att.get("is_image")),
                "drive_file_id": att.get("drive_file_id"),
                "direct_url": att.get("direct_url"),
                "rel_path": att.get("rel_path"),
                "timeout": 40,
                "max_bytes": max_bytes,
            },
            state,
            default=None,
        )

        if not isinstance(fetch, dict) or not fetch.get("content_b64"):
            sh = lc_invoke(reg, "file_sha256_text", {"text": att.get("source_ref") or ""}, state, default="") or ""
            lc_invoke(
                reg,
                "db_upsert_checkin_file_artifact",
                {
                    "tenant_id": tenant_id,
                    "checkin_id": checkin_id,
                    "source_hash": str(sh),
                    "source_ref": att.get("source_ref") or "",
                    "filename": att.get("name") or "",
                    "mime_type": att.get("mime_type") or "",
                    "byte_size": 0,
                    "drive_file_id": att.get("drive_file_id") or "",
                    "direct_url": att.get("direct_url") or "",
                    "content_hash": "",
                    "extracted_text": "(Download failed.)",
                    "extracted_json": {"download_failed": True},
                    "analysis_json": {"matches_checkin": False, "summary": "Download failed.", "confidence": 0.0},
                },
                state,
                default=None,
            )
            analyzed.append({"ref": att.get("source_ref"), "filename": att.get("name"), "ok": False, "reason": "download_failed"})
            skipped += 1
            continue

        content_b64 = fetch.get("content_b64") or ""
        if not content_b64:
            skipped += 1
            continue

        content_hash = lc_invoke(reg, "file_sha256_bytes", {"content_b64": content_b64}, state, default="") or ""
        source_hash = str(content_hash)

        exists = lc_invoke(
            reg,
            "db_checkin_file_artifact_exists",
            {
                "tenant_id": tenant_id,
                "checkin_id": checkin_id,
                "source_hash": source_hash,
                "content_hash": str(content_hash),
            },
            state,
            default=False,
        )
        if bool(exists):
            analyzed.append({"ref": att.get("source_ref"), "filename": att.get("name"), "ok": True, "skipped": True})
            skipped += 1
            continue

        mime = lc_invoke(
            reg,
            "file_sniff_mime",
            {
                "filename": att.get("name") or "",
                "declared_mime": att.get("mime_type") or "",
                "content_b64": content_b64,
            },
            state,
            default="",
        ) or ""

        ex = lc_invoke(
            reg,
            "file_extract_any",
            {
                "filename": att.get("name") or "",
                "mime_type": str(mime),
                "content_b64": content_b64,
                "context_hint": checkin_ctx,
                "enable_vision_caption": True,
            },
            state,
            default=None,
        )

        if not isinstance(ex, dict):
            ex = {"doc_type": "unknown", "extracted_text": "", "extracted_json": {}, "meta": {}}

        attachment_text = (ex.get("extracted_text") or "").strip()
        attachment_text_for_prompt = attachment_text[:20000] + "\n\n[TRUNCATED]" if len(attachment_text) > 20000 else attachment_text

        attachment_meta = {
            "filename": att.get("name") or "",
            "mime_type": str(mime),
            "byte_size": int(fetch.get("byte_size") or 0),
            "source_ref": att.get("source_ref") or "",
            "drive_file_id": att.get("drive_file_id") or "",
            "direct_url": att.get("direct_url") or "",
            "doc_type": ex.get("doc_type") or "",
            "extract_meta": ex.get("meta") or {},
        }

        prompt = _render_template_safe(
            prompt_t,
            {
                "checkin_context": checkin_ctx,
                "attachment_meta": str(attachment_meta),
                "attachment_text": attachment_text_for_prompt,
            },
        )

        analysis = lc_invoke(
            reg,
            "llm_generate_json_with_images",
            {"prompt": prompt, "images": [], "temperature": 0.0},
            state,
            default=None,
        )
        if not isinstance(analysis, dict):
            analysis = {
                "doc_type": ex.get("doc_type") or "unknown",
                "summary": "(LLM analysis failed.)",
                "matches_checkin": False,
                "mismatches": [],
                "key_findings": [],
                "measurements": [],
                "actions": [],
                "questions": [],
                "confidence": 0.0,
            }

        lc_invoke(
            reg,
            "db_upsert_checkin_file_artifact",
            {
                "tenant_id": tenant_id,
                "checkin_id": checkin_id,
                "source_hash": source_hash,
                "source_ref": att.get("source_ref") or "",
                "filename": att.get("name") or "",
                "mime_type": str(mime),
                "byte_size": int(fetch.get("byte_size") or 0),
                "drive_file_id": att.get("drive_file_id") or "",
                "direct_url": att.get("direct_url") or "",
                "content_hash": str(content_hash),
                "extracted_text": attachment_text[:120000],
                "extracted_json": {"doc_type": ex.get("doc_type") or "", "meta": ex.get("meta") or {}, **(ex.get("extracted_json") or {})},
                "analysis_json": analysis or {},
            },
            state,
            default=None,
        )

        summ = str((analysis or {}).get("summary") or "").strip()
        matches = bool((analysis or {}).get("matches_checkin") is True)
        conf = (analysis or {}).get("confidence", None)
        mism = (analysis or {}).get("mismatches") or []
        if not isinstance(mism, list):
            mism = []
        mism = [str(x).strip() for x in mism if str(x).strip()][:4]

        line = f"- File: {att.get('name') or ''} | type={ex.get('doc_type') or ''} | matches={matches}"
        if conf is not None:
            try:
                line += f" | confidence={float(conf):.2f}"
            except Exception:
                pass
        if summ:
            line += f"\n  Summary: {summ}"
        if mism:
            line += "\n  Mismatches: " + "; ".join(mism)

        ctx_lines.append(line)

        analyzed.append({"ref": att.get("source_ref"), "filename": att.get("name"), "ok": True, "doc_type": ex.get("doc_type") or ""})
        processed += 1

    state["attachments_analyzed"] = analyzed
    state["attachment_context"] = "\n".join(ctx_lines).strip()

    state.setdefault("logs", []).append(f"analyze_attachments: processed={processed} skipped={skipped} max_files={max_files}")

    if attachments_only:
        state.setdefault("logs", []).append("analyze_attachments: attachments_only mode enabled")

    return state