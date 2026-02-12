from __future__ import annotations

from typing import Any, Dict
import json
import hashlib
from datetime import datetime

from ...config import Settings
from ..lc_runtime import lc_registry, lc_invoke


def _payload_hash(payload: dict) -> str:
    b = json.dumps(payload or {}, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def _now_timestamp_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.now(ZoneInfo("Asia/Kolkata"))
    except Exception:
        dt = datetime.now()
    return dt.strftime("%m/%d/%y %I:%M %p")


def _new_conversation_id(checkin_id: str) -> str:
    base = f"{checkin_id}|{_now_timestamp_str()}|zai"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]


def writeback(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    reg = lc_registry(settings, state)

    reply = (state.get("ai_reply") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()

    if not reply or not checkin_id:
        (state.get("logs") or []).append("Skipping writeback (missing ai_reply/checkin_id)")
        return state

    annotated_urls = state.get("annotated_image_urls") or []

    reply_clean = reply

    photos_cell = ""
    if isinstance(annotated_urls, list) and annotated_urls:
        photos_cell = str(annotated_urls[0]).strip()
        reply_for_sheet = reply_clean + "\n\nAnnotated images:\n" + "\n".join([f"- {u}" for u in annotated_urls[:3]])
    else:
        reply_for_sheet = reply_clean

    is_critical = bool(state.get("is_critical"))

    conversation_id = (state.get("conversation_id") or "").strip()
    if not conversation_id:
        conversation_id = _new_conversation_id(checkin_id)

    ts = _now_timestamp_str()
    added_by = "zai@wootz.work"

    s = settings
    table = (getattr(s, "appsheet_conversation_table", "") or "Conversation").strip()
    key_col = (getattr(s, "appsheet_conversation_key_col", "") or "Conversation ID").strip()
    critical_col = (getattr(s, "appsheet_conversation_critical_col", "") or "Critical").strip()

    row = {
        key_col: conversation_id,
        "CheckIn ID": checkin_id,
        "Photo": photos_cell,
        "Remarks": "[ZAI] " + reply_for_sheet,
        "Status": state.get("checkin_status") or "",
        "Added by": added_by,
        "Timestamp": ts,
        critical_col: bool(is_critical),
    }

    lc_invoke(
        reg,
        "appsheet_action_rows",
        {"table_name": table, "action": "Add", "rows": [row], "timeout": 30},
        state,
        fatal=True,
    )

    state["conversation_id"] = conversation_id
    state["writeback_done"] = True
    (state.get("logs") or []).append("Wrote Conversation via AppSheet (Critical flag used for bot trigger)")

    # Teams + n8n only for CHECKIN_CREATED
    if (state.get("event_type") or "") != "CHECKIN_CREATED":
        return state

    run_id = (state.get("run_id") or "").strip()
    tenant_row_id = (state.get("tenant_id") or "").strip()
    project_name = (state.get("project_name") or "").strip()

    company_name_raw = (state.get("company_name") or "").strip()
    company_desc = (state.get("company_description") or "").strip()
    company_key_in_state = (state.get("company_key") or "").strip()

    # Fallback company derivation via tools (no direct CompanyTool usage)
    if (not company_name_raw or not company_key_in_state) and (tenant_row_id or project_name):
        try:
            ctx = None
            if tenant_row_id:
                ctx = lc_invoke(reg, "company_get_company_context", {"tenant_row_id": tenant_row_id}, state, default=None)
            if not ctx and project_name:
                ctx = lc_invoke(
                    reg,
                    "company_from_project_name",
                    {"project_name": project_name, "tenant_row_id": tenant_row_id},
                    state,
                    default=None,
                )
            if isinstance(ctx, dict) and ctx:
                company_name_raw = company_name_raw or (ctx.get("company_name") or "")
                company_desc = company_desc or (ctx.get("company_description") or "")
                company_key_in_state = company_key_in_state or (ctx.get("company_key") or "")
        except Exception:
            pass

    company_key_norm = (company_key_in_state or tenant_row_id or project_name or "unknown").strip()

    payload = {
        "type": "checkin_ai_reply",
        "tenant_row_id": tenant_row_id,
        "company_key_normalized": company_key_norm,
        "company_key": company_key_norm,
        "company_name": company_name_raw,
        "company_description": company_desc,
        "checkin_id": checkin_id,
        "project_name": project_name,
        "part_number": state.get("part_number") or "",
        "status": state.get("checkin_status") or "",
        "ai_reply": reply_clean,
        "annotated_images": annotated_urls[:3] if isinstance(annotated_urls, list) else [],
        "checkin_text": state.get("checkin_description") or "",
        "created_by": state.get("checkin_created_by") or "",
        "item_id": state.get("checkin_item_id") or "",
        "checkin_images": state.get("checkin_image_urls") or [],
    }

    # Idempotency (Teams post)
    try:
        if run_id:
            h = _payload_hash(payload)
            existing = lc_invoke(
                reg,
                "db_existing_artifact_source_hashes",
                {"tenant_id": tenant_row_id or company_key_norm or "unknown", "checkin_id": checkin_id, "artifact_type": "TEAMS_POST"},
                state,
                default={"hashes": []},
            ) or {"hashes": []}
            hashes = set((existing.get("hashes") or []) if isinstance(existing, dict) else [])
            if h in hashes:
                (state.get("logs") or []).append("Teams post skipped (idempotency hit: already posted)")
            else:
                lc_invoke(reg, "teams_post_message", {"payload": payload, "webhook_url": ""}, state, default=None)
                (state.get("logs") or []).append("Posted summary to Teams (company-routed payload)")

                lc_invoke(
                    reg,
                    "db_insert_artifact_no_fail",
                    {
                        "run_id": run_id,
                        "artifact_type": "TEAMS_POST",
                        "url": "power_automate_webhook",
                        "meta": {
                            "tenant_id": tenant_row_id or company_key_norm or "unknown",
                            "checkin_id": checkin_id,
                            "source_hash": h,
                            "company_key_normalized": company_key_norm,
                        },
                    },
                    state,
                    default=False,
                )
        else:
            lc_invoke(reg, "teams_post_message", {"payload": payload, "webhook_url": ""}, state, default=None)
            (state.get("logs") or []).append("Posted summary to Teams (no run_id; idempotency disabled)")
    except Exception as e:
        (state.get("logs") or []).append(f"Teams post failed: {e}")

    # n8n WhatsApp Trigger (via http tool + idempotency in db)
    try:
        n8n_url = (getattr(settings, "n8n_whatsapp_webhook_url", "") or "").strip()
        if n8n_url:
            n8n_payload = {
                "type": "checkin_created",
                "tenant_id": tenant_row_id,
                "checkin_id": checkin_id,
                "project_name": state.get("project_name") or "",
                "company_name": state.get("company_name") or "",
                "ai_reply": reply_clean,
                "part_number": state.get("part_number") or "",
                "status": state.get("checkin_status") or "",
                "checkin_text": state.get("checkin_description") or "",
                "created_by": state.get("checkin_created_by") or "",
            }

            if run_id:
                h = _payload_hash(n8n_payload)
                existing = lc_invoke(
                    reg,
                    "db_existing_artifact_source_hashes",
                    {"tenant_id": tenant_row_id or "unknown", "checkin_id": checkin_id, "artifact_type": "N8N_WEBHOOK"},
                    state,
                    default={"hashes": []},
                ) or {"hashes": []}
                hashes = set((existing.get("hashes") or []) if isinstance(existing, dict) else [])
                if h in hashes:
                    (state.get("logs") or []).append("n8n webhook skipped (idempotency hit)")
                else:
                    resp = lc_invoke(
                        reg,
                        "http_post_json",
                        {"url": n8n_url, "payload": n8n_payload, "timeout": 30},
                        state,
                        fatal=True,
                    )
                    sc = int((resp or {}).get("status_code") or 0) if isinstance(resp, dict) else 0
                    if sc >= 400:
                        raise RuntimeError(f"n8n webhook failed: {sc} {(resp or {}).get('text') if isinstance(resp, dict) else ''}")

                    (state.get("logs") or []).append("Posted payload to n8n webhook")
                    lc_invoke(
                        reg,
                        "db_insert_artifact_no_fail",
                        {
                            "run_id": run_id,
                            "artifact_type": "N8N_WEBHOOK",
                            "url": n8n_url,
                            "meta": {"tenant_id": tenant_row_id or "unknown", "checkin_id": checkin_id, "source_hash": h},
                        },
                        state,
                        default=False,
                    )
            else:
                lc_invoke(reg, "http_post_json", {"url": n8n_url, "payload": n8n_payload, "timeout": 30}, state, default=None)
                (state.get("logs") or []).append("Posted payload to n8n webhook (no run_id; idempotency disabled)")
    except Exception as e:
        (state.get("logs") or []).append(f"n8n webhook failed: {e}")

    return state