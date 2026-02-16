# service/app/pipeline/nodes/writeback.py
from __future__ import annotations

from typing import Any, Dict, List
import json
import hashlib
import requests

from ...config import Settings
from ...integrations.teams_client import TeamsClient
from ...tools.company_tool import CompanyTool, normalize_company_key, normalize_company_name
from ...tools.db_tool import DBTool
from ...integrations.appsheet_client import AppSheetClient


def _payload_hash(payload: dict) -> str:
    b = json.dumps(payload or {}, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def _as_list(x: Any) -> List[Any]:
    return x if isinstance(x, list) else []


def _as_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _extract_grounding_from_state(state: Dict[str, Any]) -> tuple[list[dict], list[dict]]:
    """
    Pull citations + edge refs from state in a backward-compatible way.

    Preferred: state["ai_reply_json"] = full LLM JSON output from checkin_reply.md
    Fallbacks: state["citations"], state["edge_tab_refs"]
    """
    citations: list[dict] = []
    edge_refs: list[dict] = []

    # Best: full structured output saved by generate_ai_reply
    ai_reply_json = _as_dict(state.get("ai_reply_json"))
    if ai_reply_json:
        citations = [c for c in _as_list(ai_reply_json.get("citations")) if isinstance(c, dict)]
        edge_refs = [e for e in _as_list(ai_reply_json.get("edge_tab_refs")) if isinstance(e, dict)]

    # Back-compat: direct keys
    if not citations:
        citations = [c for c in _as_list(state.get("citations")) if isinstance(c, dict)]
    if not edge_refs:
        edge_refs = [e for e in _as_list(state.get("edge_tab_refs")) if isinstance(e, dict)]

    return citations, edge_refs


def _format_grounding_block(citations: list[dict], edge_refs: list[dict]) -> str:
    """
    Human-readable block to append to Conversation.Remarks.
    """
    lines: list[str] = []

    if citations:
        lines.append("EVIDENCE (citations):")
        for i, c in enumerate(citations[:12], start=1):
            st = str(c.get("source_type") or "").strip()
            loc = str(c.get("locator") or "").strip()
            why = str(c.get("why_used") or "").strip()
            tag = f"[{st}]" if st else "[src]"
            if loc and why:
                lines.append(f"{i}. {tag} {loc} — {why}")
            elif loc:
                lines.append(f"{i}. {tag} {loc}")
            elif why:
                lines.append(f"{i}. {tag} {why}")

    # Edge refs MUST be attachment-only per your prompt; we still enforce display-limiting here.
    if edge_refs:
        lines.append("\nEDGE TAB REFS (attachments-only):")
        for i, e in enumerate(edge_refs[:6], start=1):
            loc = str(e.get("locator") or "").strip()
            note = str(e.get("note") or "").strip()
            if loc and note:
                lines.append(f"{i}. {loc} — {note}")
            elif loc:
                lines.append(f"{i}. {loc}")
            elif note:
                lines.append(f"{i}. {note}")

    return "\n".join([x for x in lines if str(x).strip()]).strip()


def writeback(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    reply = (state.get("ai_reply") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()

    if not reply or not checkin_id:
        (state.get("logs") or []).append("Skipping writeback (missing ai_reply/checkin_id)")
        return state

    annotated_urls = state.get("annotated_image_urls") or []

    # ---- Grounding: citations + edge refs ----
    citations, edge_refs = _extract_grounding_from_state(state)
    grounding_block = _format_grounding_block(citations, edge_refs)

    # Keep a clean reply for Teams formatting
    reply_clean = reply

    # AppSheet Image column expects a single URL (best: direct image URL)
    photos_cell = ""
    if isinstance(annotated_urls, list) and annotated_urls:
        photos_cell = str(annotated_urls[0]).strip()  # only 1 for Photo column
        reply_for_sheet = reply_clean + "\n\nAnnotated images:\n" + "\n".join([f"- {u}" for u in annotated_urls[:3]])
    else:
        reply_for_sheet = reply_clean

    # Append grounding block to Remarks (this is your “persistence” in sheet)
    if grounding_block:
        reply_for_sheet = (reply_for_sheet.strip() + "\n\n" + grounding_block).strip()

    # ----------------------------
    # Conversation writeback (ALWAYS via AppSheet)
    # Trigger uses ONLY "Critical"
    # ----------------------------
    is_critical = bool(state.get("is_critical"))
    apps = AppSheetClient(settings)

    conversation_id = (state.get("conversation_id") or "").strip()
    if not conversation_id:
        from ...tools.sheets_tool import _rand_conversation_id
        conversation_id = _rand_conversation_id()

    added_by = "zai@wootz.work"
    from ...tools.sheets_tool import _now_timestamp_str
    ts = _now_timestamp_str()

    if not apps.enabled():
        raise RuntimeError(
            "AppSheet not enabled (missing APPSHEET_APP_ID / APPSHEET_ACCESS_KEY). "
            "Conversation writeback requires AppSheet."
        )

    s = settings
    table = (s.appsheet_conversation_table or "Conversation").strip()
    key_col = (s.appsheet_conversation_key_col or "Conversation ID").strip()
    critical_col = (s.appsheet_conversation_critical_col or "Critical").strip()

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

    apps.action_rows(table_name=table, action="Add", rows=[row], timeout=30)

    state["conversation_id"] = conversation_id
    state["writeback_done"] = True
    (state.get("logs") or []).append("Wrote Conversation via AppSheet (Critical flag used for bot trigger)")

    # Persist grounding artifact (DB) for audit/idempotency (never fail pipeline)
    try:
        run_id = (state.get("run_id") or "").strip()
        tenant_row_id = (state.get("tenant_id") or "").strip()
        if run_id and settings.database_url:
            db = DBTool(settings.database_url)
            db.insert_artifact_no_fail(
                run_id=run_id,
                artifact_type="AI_REPLY_GROUNDED",
                url="appsheet:conversation",
                meta={
                    "tenant_id": tenant_row_id or "unknown",
                    "checkin_id": checkin_id,
                    "conversation_id": conversation_id,
                    "source_hash": _payload_hash(
                        {
                            "checkin_id": checkin_id,
                            "conversation_id": conversation_id,
                            "ai_reply": reply_clean,
                            "citations": citations,
                            "edge_tab_refs": edge_refs,
                        }
                    ),
                    "citations": citations,
                    "edge_tab_refs": edge_refs,
                },
            )
    except Exception:
        pass

    # Teams post (only for new checkins)
    if (state.get("event_type") or "") == "CHECKIN_CREATED":
        try:
            webhook_url = (
                getattr(settings, "power_automate_webhook_url", "")
                or getattr(settings, "teams_webhook_url", "")
            )
            client = TeamsClient(webhook_url)
            if client.enabled():
                tenant_row_id = (state.get("tenant_id") or "").strip()
                project_name = (state.get("project_name") or "").strip()

                company_name_raw = (state.get("company_name") or "").strip()
                company_desc = (state.get("company_description") or "").strip()
                company_key_in_state = (state.get("company_key") or "").strip()

                # Fallback: if upstream didn't populate company_* reliably, derive now
                if not company_name_raw or not company_key_in_state:
                    try:
                        ctool = CompanyTool(settings)
                        ctx = None
                        if tenant_row_id:
                            ctx = ctool.get_company_context(tenant_row_id)
                        if not ctx and project_name:
                            ctx = ctool.from_project_name(project_name, tenant_row_id=tenant_row_id)

                        if ctx:
                            company_name_raw = company_name_raw or (ctx.company_name or "")
                            company_desc = company_desc or (ctx.company_description or "")
                            company_key_in_state = company_key_in_state or (ctx.company_key or "")
                    except Exception:
                        pass

                company_name_norm = normalize_company_name(company_name_raw or project_name)
                company_key_norm = company_key_in_state or normalize_company_key(
                    company_name_raw or project_name,
                    fallback=tenant_row_id,
                )

                payload = {
                    "type": "checkin_ai_reply",

                    # routing inputs
                    "tenant_row_id": tenant_row_id,
                    "company_key_normalized": company_key_norm,
                    "company_key": company_key_norm,
                    "company_name": company_name_raw or company_name_norm,
                    "company_description": company_desc,

                    # checkin info
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

                    # NEW: grounding payload for Edge tab writer
                    "citations": citations,
                    "edge_tab_refs": edge_refs,
                }

                # ---- Idempotency: avoid duplicate external posts ----
                run_id = (state.get("run_id") or "").strip()
                if run_id:
                    db = DBTool(settings.database_url)
                    h = _payload_hash(payload)

                    existing = db.existing_artifact_source_hashes(
                        tenant_id=tenant_row_id or company_key_norm or "unknown",
                        checkin_id=checkin_id,
                        artifact_type="TEAMS_POST",
                    )

                    if h in existing:
                        (state.get("logs") or []).append("Teams post skipped (idempotency hit: already posted)")
                        return state

                client.post_message(payload)
                (state.get("logs") or []).append("Posted summary to Teams (company-routed payload)")

                # Mark as posted (idempotency record)
                try:
                    if run_id:
                        db.insert_artifact_no_fail(
                            run_id=run_id,
                            artifact_type="TEAMS_POST",
                            url="power_automate_webhook",
                            meta={
                                "tenant_id": tenant_row_id or company_key_norm or "unknown",
                                "checkin_id": checkin_id,
                                "source_hash": _payload_hash(payload),
                                "company_key_normalized": company_key_norm,
                            },
                        )
                except Exception:
                    pass

        except Exception as e:
            (state.get("logs") or []).append(f"Teams post failed: {e}")

        try:
            # n8n WhatsApp Trigger
            n8n_url = getattr(settings, "n8n_whatsapp_webhook_url", "").strip()
            if n8n_url:
                run_id = (state.get("run_id") or "").strip()
                tenant_row_id = (state.get("tenant_id") or "").strip()

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

                    # NEW: grounding for downstream automation
                    "citations": citations,
                    "edge_tab_refs": edge_refs,
                }

                # Idempotency check
                if run_id:
                    db = DBTool(settings.database_url)
                    h = _payload_hash(n8n_payload)

                    existing = db.existing_artifact_source_hashes(
                        tenant_id=tenant_row_id or "unknown",
                        checkin_id=checkin_id,
                        artifact_type="N8N_WEBHOOK",
                    )

                    if h in existing:
                        (state.get("logs") or []).append("n8n webhook skipped (idempotency hit)")
                    else:
                        response = requests.post(n8n_url, json=n8n_payload, timeout=30)
                        if response.status_code >= 400:
                            raise RuntimeError(f"n8n webhook failed: {response.status_code} {response.text}")

                        (state.get("logs") or []).append("Posted payload to n8n webhook")

                        db.insert_artifact_no_fail(
                            run_id=run_id,
                            artifact_type="N8N_WEBHOOK",
                            url=n8n_url,
                            meta={
                                "tenant_id": tenant_row_id or "unknown",
                                "checkin_id": checkin_id,
                                "source_hash": h,
                            },
                        )
        except Exception as e:
            (state.get("logs") or []).append(f"n8n webhook failed: {e}")

    return state