# service/app/pipeline/nodes/analyze_media.py
from __future__ import annotations

from typing import Any, Dict, List
import hashlib
import re
import base64

from ...config import Settings
from ..lc_runtime import lc_registry, lc_invoke


_IMG_EXT_RX = re.compile(r"\.(png|jpe?g|webp|bmp|tiff?)$", re.IGNORECASE)


def _norm_value(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x.strip()
    return str(x).strip()


def _key(s: Any) -> str:
    return re.sub(r"\s+", " ", _norm_value(s)).strip().lower()


def _sha256(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return h.hexdigest()


def _looks_like_media_ref(s: str) -> bool:
    ss = (s or "").strip()
    if not ss:
        return False
    if ss.lower().startswith("http://") or ss.lower().startswith("https://"):
        return True
    if "/" in ss:
        return True
    return bool(_IMG_EXT_RX.search(ss)) or ss.lower().endswith(".pdf")


def _sniff_mime(data: bytes) -> str:
    if not data:
        return ""
    b = data
    if b.startswith(b"%PDF"):
        return "application/pdf"
    if b[:3] == b"\xFF\xD8\xFF":
        return "image/jpeg"
    if b.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if b[:4] == b"RIFF" and b[8:12] == b"WEBP":
        return "image/webp"
    return ""


def _is_image_mime(m: str) -> bool:
    return (m or "").startswith("image/")


def _b64(b: bytes) -> str:
    return base64.b64encode(b or b"").decode("utf-8")


def _collect_photo_cells_from_additional_rows(reg, rows: List[Dict[str, Any]], state: Dict[str, Any]) -> List[str]:
    refs: List[str] = []
    for r in rows or []:
        for k, v in (r or {}).items():
            kk = (k or "").strip()
            if not kk:
                continue
            if not kk.lower().strip().startswith("photo"):
                continue
            cell = _norm_value(v)
            if not cell:
                continue

            parts = lc_invoke(reg, "attachment_split_cell_refs", {"cell": cell}, state, default=[]) or []
            for ref in parts:
                if _looks_like_media_ref(ref):
                    refs.append(ref)
    return refs


def analyze_media(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {"CHECKIN_CREATED", "CHECKIN_UPDATED", "CONVERSATION_ADDED"}
    if (state.get("event_type") or "") not in allowed:
        state.setdefault("logs", []).append(f"analyze_media: skipped (event_type not in {sorted(allowed)})")
        return state

    tenant_id = (state.get("tenant_id") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()
    run_id = (state.get("run_id") or "").strip()
    if not tenant_id or not checkin_id or not run_id:
        state.setdefault("logs", []).append("analyze_media: skipped (missing tenant/checkin/run_id)")
        state["media_images"] = []
        return state

    reg = lc_registry(settings, state)

    existing_captions_by_hash = lc_invoke(
        reg,
        "db_image_captions_by_hash",
        {"tenant_id": tenant_id, "checkin_id": checkin_id},
        state,
        default={},
    ) or {}
    if not isinstance(existing_captions_by_hash, dict):
        existing_captions_by_hash = {}
    existing_caption_hashes = set(existing_captions_by_hash.keys())

    existing_pdf_hashes = set(
        (lc_invoke(
            reg,
            "db_existing_artifact_source_hashes",
            {"tenant_id": tenant_id, "checkin_id": checkin_id, "artifact_type": "PDF_ATTACHMENT"},
            state,
            default={"hashes": []},
        ) or {}).get("hashes", []) or []
    )
    existing_image_source_hashes = set(
        (lc_invoke(
            reg,
            "db_existing_artifact_source_hashes",
            {"tenant_id": tenant_id, "checkin_id": checkin_id, "artifact_type": "IMAGE_SOURCE"},
            state,
            default={"hashes": []},
        ) or {}).get("hashes", []) or []
    )

    checkin_row = state.get("checkin_row") or {}
    col_img = lc_invoke(reg, "sheets_map_col", {"table": "checkin", "field": "inspection_image_url"}, state, default="")
    img_cell = _norm_value(checkin_row.get(_key(col_img), "")) if col_img else ""

    main_refs = lc_invoke(reg, "attachment_split_cell_refs", {"cell": img_cell or ""}, state, default=[]) or []
    main_refs = [r for r in main_refs if _looks_like_media_ref(r)]

    convo_refs: List[str] = []
    try:
        col_convo_photo = lc_invoke(reg, "sheets_map_col", {"table": "conversation", "field": "photos"}, state, default="")
        k_convo_photo = _key(col_convo_photo) if col_convo_photo else ""
        for cr in (state.get("conversation_rows") or [])[-50:]:
            cell = _norm_value((cr or {}).get(k_convo_photo, "")) if k_convo_photo else ""
            parts = lc_invoke(reg, "attachment_split_cell_refs", {"cell": cell or ""}, state, default=[]) or []
            for ref in parts:
                if _looks_like_media_ref(ref):
                    convo_refs.append(ref)
    except Exception as e:
        state.setdefault("logs", []).append(f"analyze_media: conversation photo parse failed (non-fatal): {e}")

    add_refs: List[str] = []
    try:
        add_tab = (getattr(settings, "additional_photos_tab_name", "Checkin Additional photos") or "").strip()
        add_rows = lc_invoke(
            reg,
            "sheets_list_additional_photos_for_checkin",
            {"checkin_id": checkin_id, "tab_name": add_tab},
            state,
            default=[],
        ) or []
        add_refs = _collect_photo_cells_from_additional_rows(reg, add_rows, state)
        state.setdefault("logs", []).append(
            f"analyze_media: additional_photos rows={len(add_rows or [])} refs={len(add_refs)} tab='{add_tab}'"
        )
    except Exception as e:
        state.setdefault("logs", []).append(f"analyze_media: additional photos read failed (non-fatal): {e}")
        add_refs = []

    refs: List[str] = []
    seen = set()
    for r in (main_refs or []) + (convo_refs or []) + (add_refs or []):
        rr = (r or "").strip()
        if rr and rr not in seen:
            refs.append(rr)
            seen.add(rr)

    if not refs:
        state.setdefault("logs", []).append("analyze_media: no media refs found")
        state["media_images"] = []
        return state

    refs = refs[:12]

    media_notes: List[str] = []
    new_captions: List[str] = []
    media_images: List[Dict[str, Any]] = []

    context_hint = (
        f"Project={state.get('project_name') or ''} | Part={state.get('part_number') or ''} | "
        f"Checkin={checkin_id} | Status={state.get('checkin_status') or ''}\n"
        f"Description: {state.get('checkin_description') or ''}"
    ).strip()

    for ref in refs:
        att = lc_invoke(reg, "attachment_resolve", {"ref": ref}, state, default=None)
        if not isinstance(att, dict) or not att:
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
                "max_bytes": 15_000_000,
            },
            state,
            default=None,
        )
        if not isinstance(fetch, dict) or not fetch.get("content_b64"):
            continue

        try:
            data = base64.b64decode(fetch.get("content_b64") or "")
        except Exception:
            continue
        if not data:
            continue

        source_hash = _sha256(data)
        mime = (att.get("mime_type") or "").strip() or _sniff_mime(data) or "application/octet-stream"
        is_pdf = (mime == "application/pdf") or str(att.get("name") or "").lower().endswith(".pdf")
        is_img = _is_image_mime(mime)

        if is_img and source_hash not in existing_image_source_hashes:
            ok = lc_invoke(
                reg,
                "db_insert_artifact_no_fail",
                {
                    "run_id": run_id,
                    "artifact_type": "IMAGE_SOURCE",
                    "url": (att.get("source_ref") or att.get("rel_path") or "unknown"),
                    "meta": {
                        "tenant_id": tenant_id,
                        "checkin_id": checkin_id,
                        "source_ref": att.get("source_ref"),
                        "source_hash": source_hash,
                        "file_name": att.get("name"),
                        "mime_type": mime,
                    },
                },
                state,
                default=False,
            )
            if ok:
                existing_image_source_hashes.add(source_hash)

        if is_pdf:
            if source_hash in existing_pdf_hashes:
                continue

            lc_invoke(
                reg,
                "db_insert_artifact_no_fail",
                {
                    "run_id": run_id,
                    "artifact_type": "PDF_ATTACHMENT",
                    "url": (att.get("source_ref") or att.get("rel_path") or "unknown"),
                    "meta": {
                        "tenant_id": tenant_id,
                        "checkin_id": checkin_id,
                        "source_ref": att.get("source_ref"),
                        "source_hash": source_hash,
                        "file_name": att.get("name"),
                        "mime_type": mime,
                    },
                },
                state,
                default=False,
            )

            existing_pdf_hashes.add(source_hash)
            pdf_line = f"PDF: {att.get('name') or 'attachment'} (no text extracted)"
            new_captions.append(pdf_line)
            media_notes.append(f"- Doc: {pdf_line}")
            continue

        if not is_img:
            continue

        img_index = len(media_images)
        media_images.append(
            {
                "image_index": img_index,
                "mime_type": mime if mime.startswith("image/") else "image/jpeg",
                "image_bytes": data,
                "source_ref": att.get("source_ref") or att.get("rel_path") or "unknown",
                "file_name": att.get("name") or "",
                "source_hash": source_hash,
            }
        )

        caption = (existing_captions_by_hash.get(source_hash) or "").strip()

        if not caption:
            cap = lc_invoke(
                reg,
                "vision_caption_for_retrieval",
                {
                    "image_b64": _b64(data),
                    "mime_type": mime if mime.startswith("image/") else "image/jpeg",
                    "context_hint": context_hint,
                    "model": None,
                },
                state,
                default="",
            )
            caption = str(cap or "").strip()

            if caption and source_hash not in existing_caption_hashes:
                ok = lc_invoke(
                    reg,
                    "db_insert_artifact_no_fail",
                    {
                        "run_id": run_id,
                        "artifact_type": "IMAGE_CAPTION",
                        "url": (att.get("source_ref") or att.get("rel_path") or "unknown"),
                        "meta": {
                            "tenant_id": tenant_id,
                            "checkin_id": checkin_id,
                            "source_ref": att.get("source_ref"),
                            "source_hash": source_hash,
                            "file_name": att.get("name"),
                            "mime_type": mime,
                            "caption": caption,
                            "vision_model": getattr(settings, "vision_model", ""),
                        },
                    },
                    state,
                    default=False,
                )
                if ok:
                    existing_caption_hashes.add(source_hash)
                    existing_captions_by_hash[source_hash] = caption
                    new_captions.append(caption)

        if caption:
            media_notes.append(f"- Image: {caption}")
        else:
            media_notes.append(f"- Image: {(att.get('name') or 'image').strip()}")

    state["media_images"] = media_images

    all_caps: List[str] = []
    seen_caps = set()

    for c in (new_captions or []):
        cc = str(c or "").strip()
        if cc and cc not in seen_caps:
            all_caps.append(cc)
            seen_caps.add(cc)

    for _, c in (existing_captions_by_hash or {}).items():
        cc = str(c or "").strip()
        if cc and cc not in seen_caps:
            all_caps.append(cc)
            seen_caps.add(cc)

    if all_caps:
        state["image_captions"] = all_caps

    if media_notes:
        note_block = "MEDIA OBSERVATIONS:\n" + "\n".join(media_notes)
        state["media_notes"] = note_block
        snap = (state.get("thread_snapshot_text") or "").strip()
        state["thread_snapshot_text"] = (snap + "\n\n" + note_block).strip()

    state.setdefault("logs", []).append(
        f"analyze_media: done refs={len(refs)} images={len(media_images)} captions_total={len(state.get('image_captions') or [])}"
    )
    return state