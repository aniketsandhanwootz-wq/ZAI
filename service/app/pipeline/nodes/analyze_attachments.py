# service/app/pipeline/nodes/analyze_attachments.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from ...config import Settings
from ..lc_runtime import lc_registry, lc_invoke


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _load_prompt_template() -> str:
    p = _repo_root() / "packages" / "prompts" / "attachment_analysis.md"
    return p.read_text(encoding="utf-8")


def _load_correlation_template() -> str:
    p = _repo_root() / "packages" / "prompts" / "attachment_correlation.md"
    return p.read_text(encoding="utf-8")


def _render_template_safe(template: str, vars: Dict[str, str]) -> str:
    out = template or ""
    for k, v in (vars or {}).items():
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


def _safe_list(x: Any, *, cap: int = 8) -> List[str]:
    if not isinstance(x, list):
        return []
    out = []
    for it in x:
        s = str(it or "").strip()
        if s:
            out.append(s)
        if len(out) >= cap:
            break
    return out


def _compact_measurements(measurements: Any, *, cap: int = 10) -> List[str]:
    """
    Accepts either:
      - list[str]
      - list[dict] with common keys (name/value/unit/spec/pass_fail)
    Returns: list[str] compact lines.
    """
    if not isinstance(measurements, list):
        return []
    out: List[str] = []
    for it in measurements:
        if isinstance(it, str):
            s = it.strip()
            if s:
                out.append(s)
        elif isinstance(it, dict):
            name = str(it.get("name") or it.get("test") or it.get("parameter") or "").strip()
            val = str(it.get("value") or "").strip()
            unit = str(it.get("unit") or "").strip()
            spec = str(it.get("spec") or it.get("limit") or "").strip()
            pf = str(it.get("pass_fail") or it.get("result") or "").strip()

            seg = ""
            if name:
                seg += name
            if val:
                seg += (": " if seg else "") + val
            if unit:
                seg += unit if val else (" " + unit)
            if spec:
                seg += f" (Spec: {spec})"
            if pf:
                seg += f" [{pf}]"
            seg = seg.strip()
            if seg:
                out.append(seg)
        if len(out) >= cap:
            break
    return out


def _analysis_brief(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize analysis_json into predictable keys even if prompt evolves.
    """
    a = analysis or {}
    return {
        "doc_type": str(a.get("doc_type") or a.get("type") or "unknown").strip(),
        "summary": str(a.get("summary") or "").strip(),
        "matches_checkin": bool(a.get("matches_checkin") is True),
        "confidence": a.get("confidence", None),
        "mismatches": _safe_list(a.get("mismatches") or [], cap=6),
        "key_findings": _safe_list(a.get("key_findings") or a.get("findings") or [], cap=8),
        "measurements": _compact_measurements(a.get("measurements") or a.get("tests") or [], cap=12),
        "identifiers": _safe_list(a.get("identifiers") or a.get("ids") or [], cap=8),
        "dates": _safe_list(a.get("dates") or [], cap=6),
        "actions": _safe_list(a.get("actions") or [], cap=6),
        "questions": _safe_list(a.get("questions") or [], cap=6),
    }


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

    # Pull existing briefs so "exists=True" doesn't delete context from prompt.
    existing_briefs = lc_invoke(
        reg,
        "db_get_checkin_file_briefs",
        {"tenant_id": tenant_id, "checkin_id": checkin_id, "max_items": 20},
        state,
        default=[],
    ) or []
    if not isinstance(existing_briefs, list):
        existing_briefs = []

    briefs_by_hash: Dict[str, Dict[str, Any]] = {}
    for b in existing_briefs:
        if isinstance(b, dict):
            h = str(b.get("source_hash") or b.get("content_hash") or "").strip()
            if h:
                briefs_by_hash[h] = b

    prompt_t = _load_prompt_template()
    corr_t = _load_correlation_template()
    checkin_ctx = _make_checkin_context(state)

    analyzed: List[Dict[str, Any]] = []
    ctx_lines: List[str] = []
    per_file_for_correlation: List[Dict[str, Any]] = []

    processed = 0
    skipped = 0

    max_files = int(meta.get("max_files") or 6)
    max_bytes = int(meta.get("max_bytes") or 15_000_000)

    PROMPT_TEXT_CAP = int(meta.get("prompt_text_cap") or 60000)

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
            # Record failure into DB (idempotent), but also keep context line.
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
            ctx_lines.append(f"- File: {att.get('name') or ''} | download_failed")
            analyzed.append({"ref": att.get("source_ref"), "filename": att.get("name"), "ok": False, "reason": "download_failed"})
            skipped += 1
            continue

        content_b64 = fetch.get("content_b64") or ""
        content_hash = lc_invoke(reg, "file_sha256_bytes", {"content_b64": content_b64}, state, default="") or ""
        source_hash = str(content_hash)

        # If exists, DO NOT skip context. Pull brief from DB and include.
        exists = lc_invoke(
            reg,
            "db_checkin_file_artifact_exists",
            {"tenant_id": tenant_id, "checkin_id": checkin_id, "source_hash": source_hash, "content_hash": str(content_hash)},
            state,
            default=False,
        )
        if bool(exists):
            b = briefs_by_hash.get(source_hash) or {}
            analysis_prev = b.get("analysis_json") if isinstance(b, dict) else {}
            if not isinstance(analysis_prev, dict):
                analysis_prev = {}
            brief = _analysis_brief(analysis_prev)

            line = f"- File: {att.get('name') or ''} | type={brief['doc_type']} | cached=True | matches={brief['matches_checkin']}"
            conf = brief.get("confidence", None)
            if conf is not None:
                try:
                    line += f" | confidence={float(conf):.2f}"
                except Exception:
                    pass
            if brief["identifiers"]:
                line += "\n  IDs: " + "; ".join(brief["identifiers"][:6])
            if brief["dates"]:
                line += "\n  Dates: " + "; ".join(brief["dates"][:4])
            if brief["measurements"]:
                line += "\n  Measurements: " + "; ".join(brief["measurements"][:8])
            if brief["summary"]:
                line += f"\n  Summary: {brief['summary']}"
            if brief["mismatches"]:
                line += "\n  Mismatches: " + "; ".join(brief["mismatches"][:4])

            ctx_lines.append(line)
            per_file_for_correlation.append(
                {
                    "filename": att.get("name") or "",
                    "doc_type": brief["doc_type"],
                    "identifiers": brief["identifiers"],
                    "dates": brief["dates"],
                    "measurements": brief["measurements"],
                    "key_findings": brief["key_findings"],
                    "mismatches": brief["mismatches"],
                    "summary": brief["summary"],
                    "confidence": brief.get("confidence", None),
                    "cached": True,
                }
            )
            analyzed.append({"ref": att.get("source_ref"), "filename": att.get("name"), "ok": True, "cached": True})
            skipped += 1
            continue

        mime = lc_invoke(
            reg,
            "file_sniff_mime",
            {"filename": att.get("name") or "", "declared_mime": att.get("mime_type") or "", "content_b64": content_b64},
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
                "enable_vision_caption": True,  # uses OCR_MODE for scanned PDF pages (from pdf_extractor)
            },
            state,
            default=None,
        )
        if not isinstance(ex, dict):
            ex = {"doc_type": "unknown", "extracted_text": "", "extracted_json": {}, "meta": {}}

        attachment_text = (ex.get("extracted_text") or "").strip()
        attachment_text_for_prompt = (
            attachment_text[:PROMPT_TEXT_CAP] + "\n\n[TRUNCATED]"
            if len(attachment_text) > PROMPT_TEXT_CAP
            else attachment_text
        )

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

        # Persist full extraction + analysis
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
                "extracted_json": {
                    "doc_type": ex.get("doc_type") or "",
                    "meta": ex.get("meta") or {},
                    **(ex.get("extracted_json") or {}),
                },
                "analysis_json": analysis or {},
            },
            state,
            default=None,
        )

        brief = _analysis_brief(analysis)

        line = f"- File: {att.get('name') or ''} | type={brief['doc_type']} | matches={brief['matches_checkin']}"
        conf = brief.get("confidence", None)
        if conf is not None:
            try:
                line += f" | confidence={float(conf):.2f}"
            except Exception:
                pass

        if brief["identifiers"]:
            line += "\n  IDs: " + "; ".join(brief["identifiers"][:6])
        if brief["dates"]:
            line += "\n  Dates: " + "; ".join(brief["dates"][:4])
        if brief["measurements"]:
            line += "\n  Measurements: " + "; ".join(brief["measurements"][:8])
        if brief["key_findings"]:
            line += "\n  KeyFindings: " + "; ".join(brief["key_findings"][:6])
        if brief["summary"]:
            line += f"\n  Summary: {brief['summary']}"
        if brief["mismatches"]:
            line += "\n  Mismatches: " + "; ".join(brief["mismatches"][:4])

        ctx_lines.append(line)
        per_file_for_correlation.append(
            {
                "filename": att.get("name") or "",
                "doc_type": brief["doc_type"],
                "identifiers": brief["identifiers"],
                "dates": brief["dates"],
                "measurements": brief["measurements"],
                "key_findings": brief["key_findings"],
                "mismatches": brief["mismatches"],
                "summary": brief["summary"],
                "confidence": brief.get("confidence", None),
                "cached": False,
            }
        )

        analyzed.append(
            {
                "ref": att.get("source_ref"),
                "filename": att.get("name"),
                "ok": True,
                "doc_type": ex.get("doc_type") or "",
                "analysis": analysis if isinstance(analysis, dict) else {},
            }
        )
        processed += 1

    # Build the attachment_context (this is what checkin_reply.md receives)
    base_ctx = "\n".join(ctx_lines).strip()
    state["attachments_analyzed"] = analyzed
    state["attachment_context"] = base_ctx

    # Cross-file correlation step (merges evidence + flags conflicts)
    corr_block = ""
    if per_file_for_correlation and corr_t:
        corr_prompt = _render_template_safe(
            corr_t,
            {
                "checkin_context": checkin_ctx,
                "attachment_meta": "",  # not used here
                "attachment_text": "",  # not used here
            },
        )
        corr_prompt = (corr_prompt.strip() + "\n\nCHECKIN_CONTEXT:\n" + checkin_ctx + "\n\nFILES:\n" + str(per_file_for_correlation)).strip()

        corr_txt = lc_invoke(reg, "llm_generate_text", {"prompt": corr_prompt}, state, default="") or ""
        corr_block = str(corr_txt or "").strip()

    if corr_block:
        merged = ("ATTACHMENT CONTEXT (from Files):\n" + base_ctx + "\n\n" + corr_block).strip()
        state["attachment_context"] = merged

    state.setdefault("logs", []).append(f"analyze_attachments: processed={processed} skipped={skipped} max_files={max_files}")
    if attachments_only:
        state.setdefault("logs", []).append("analyze_attachments: attachments_only mode enabled")

    return state