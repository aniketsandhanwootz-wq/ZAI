# service/app/pipeline/nodes/analyze_attachments.py
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional

from ...config import Settings
from ...tools.attachment_tool import AttachmentResolver, split_cell_refs
from ...tools.drive_tool import DriveTool
from ...tools.db_tool import DBTool
from ...tools.llm_tool import LLMTool
from ...tools.vision_tool import VisionTool
from ...tools.file_extractors.router import extract_any, sniff_mime, sha256_text, sha256_bytes

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
    """
    Find Files column even if mapping doesn't include it.
    """
    if not checkin_row:
        return ""
    # exact match first
    for k in checkin_row.keys():
        if _norm_header(k) == "files":
            v = str(checkin_row.get(k) or "").strip()
            if v:
                return v

    # fallback set
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
    """
    Ingest + extract + analyze "Files" column attachments for the checkin.
    - Idempotent (DB primary key)
    - No reply side effects; reply node uses `state['attachment_context']` if present.
    """
    tenant_id = _norm(str(state.get("tenant_id") or ""))
    checkin_id = _norm(str(state.get("checkin_id") or ""))

    # If we cannot associate to tenant/checkin, skip cleanly
    if not tenant_id or not checkin_id:
        state.setdefault("logs", []).append("analyze_attachments: skip (missing tenant_id/checkin_id)")
        return state

    meta = state.get("meta") or {}
    attachments_only = str(meta.get("attachments_only") or "").strip().lower() in ("1", "true", "yes", "y", "on")

    checkin_row = state.get("checkin_row") or {}
    files_cell = ""

    # Allow explicit override (Swagger calls can pass meta.checkin_files)
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

    refs = split_cell_refs(files_cell)
    if not refs:
        state.setdefault("logs", []).append("analyze_attachments: Files present but no refs parsed")
        state["attachment_context"] = ""
        state["attachments_analyzed"] = []
        return state

    drive = DriveTool(settings)
    resolver = AttachmentResolver(drive)
    db = DBTool(settings.database_url)

    llm = LLMTool(settings)
    vision = VisionTool(settings)

    prompt_t = _load_prompt_template()
    checkin_ctx = _make_checkin_context(state)

    analyzed: List[Dict[str, Any]] = []
    ctx_lines: List[str] = []
    processed = 0
    skipped = 0

    # Hard limit to prevent abuse / runaway
    max_files = int(meta.get("max_files") or 6)

    for ref in refs[:max_files]:
        att = resolver.resolve(ref)
        if not att:
            skipped += 1
            continue
        max_bytes = int(meta.get("max_bytes") or 15_000_000)
        b = resolver.fetch_bytes(att, timeout=40, max_bytes=max_bytes)
        if not b:
            # store minimal metadata anyway (so we know it failed)
            sh = sha256_text(att.source_ref)
            db.upsert_checkin_file_artifact(
                tenant_id=tenant_id,
                checkin_id=checkin_id,
                source_hash=sh,
                source_ref=att.source_ref,
                filename=att.name,
                mime_type=att.mime_type or "",
                byte_size=0,
                drive_file_id=att.drive_file_id or "",
                direct_url=att.direct_url or "",
                content_hash="",
                extracted_text="(Download failed.)",
                extracted_json={"download_failed": True},
                analysis_json={"matches_checkin": False, "summary": "Download failed.", "confidence": 0.0},
            )
            analyzed.append({"ref": att.source_ref, "filename": att.name, "ok": False, "reason": "download_failed"})
            skipped += 1
            continue

        # Strong idempotency: content hash
        content_hash = sha256_bytes(b)
        # Prefer content-based source_hash so same file doesn't duplicate
        source_hash = content_hash

        if db.checkin_file_artifact_exists(tenant_id=tenant_id, checkin_id=checkin_id, source_hash=source_hash, content_hash=content_hash):
            analyzed.append({"ref": att.source_ref, "filename": att.name, "ok": True, "skipped": True})
            skipped += 1
            continue

        mime = sniff_mime(att.name, att.mime_type or "", b)

        # Extract
        ex = extract_any(
            filename=att.name,
            mime_type=mime,
            data=b,
            vision_caption_fn=lambda image_bytes, mime_type, context="": vision.caption_for_retrieval(
                image_bytes=image_bytes, mime_type=mime_type, context_hint=context
            ),
        )

        # LLM analysis (use extracted_text, may be long -> truncate for prompt)
        attachment_text = (ex.extracted_text or "").strip()
        if len(attachment_text) > 20000:
            attachment_text_for_prompt = attachment_text[:20000] + "\n\n[TRUNCATED]"
        else:
            attachment_text_for_prompt = attachment_text

        attachment_meta = {
            "filename": att.name,
            "mime_type": mime,
            "byte_size": len(b),
            "source_ref": att.source_ref,
            "drive_file_id": att.drive_file_id or "",
            "direct_url": att.direct_url or "",
            "doc_type": ex.doc_type,
            "extract_meta": ex.meta,
        }

        prompt = _render_template_safe(
            prompt_t,
            {
                "checkin_context": checkin_ctx,
                "attachment_meta": str(attachment_meta),
                "attachment_text": attachment_text_for_prompt,
            },
        )

        try:
            analysis = llm.generate_json_with_images(prompt=prompt, images=[], temperature=0.0)
        except Exception as e:
            analysis = {
                "doc_type": ex.doc_type,
                "summary": "(LLM analysis failed.)",
                "matches_checkin": False,
                "match_reason": str(e)[:160],
                "mismatches": [],
                "key_findings": [],
                "measurements": [],
                "actions": [],
                "questions": [],
                "confidence": 0.0,
            }

        # Persist
        db.upsert_checkin_file_artifact(
            tenant_id=tenant_id,
            checkin_id=checkin_id,
            source_hash=source_hash,
            source_ref=att.source_ref,
            filename=att.name,
            mime_type=mime,
            byte_size=len(b),
            drive_file_id=att.drive_file_id or "",
            direct_url=att.direct_url or "",
            content_hash=content_hash,
            extracted_text=attachment_text[:120000],  # keep it bounded in DB
            extracted_json={"doc_type": ex.doc_type, "meta": ex.meta, **(ex.extracted_json or {})},
            analysis_json=analysis or {},
        )

        # Build reply-context snippet
        summ = str((analysis or {}).get("summary") or "").strip()
        matches = bool((analysis or {}).get("matches_checkin") is True)
        conf = (analysis or {}).get("confidence", None)
        mism = (analysis or {}).get("mismatches") or []
        if not isinstance(mism, list):
            mism = []
        mism = [str(x).strip() for x in mism if str(x).strip()][:4]

        line = f"- File: {att.name} | type={ex.doc_type} | matches={matches}"
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

        analyzed.append({"ref": att.source_ref, "filename": att.name, "ok": True, "doc_type": ex.doc_type})
        processed += 1

    state["attachments_analyzed"] = analyzed
    state["attachment_context"] = "\n".join(ctx_lines).strip()

    state.setdefault("logs", []).append(
        f"analyze_attachments: processed={processed} skipped={skipped} max_files={max_files}"
    )

    # If this run is attachments-only backfill, we still want the pipeline to continue normally
    # but reply/writeback already controlled by graph event_type rules.
    if attachments_only:
        state.setdefault("logs", []).append("analyze_attachments: attachments_only mode enabled")

    return state