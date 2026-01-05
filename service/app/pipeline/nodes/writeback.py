from __future__ import annotations

from typing import Any, Dict

from ...config import Settings
from ...tools.sheets_tool import SheetsTool
from ...integrations.teams_client import TeamsClient
from ...tools.company_tool import CompanyTool, normalize_company_key, normalize_company_name


def writeback(settings: Settings, state: Dict[str, Any]) -> Dict[str, Any]:
    reply = (state.get("ai_reply") or "").strip()
    checkin_id = (state.get("checkin_id") or "").strip()

    if not reply or not checkin_id:
        (state.get("logs") or []).append("Skipping writeback (missing ai_reply/checkin_id)")
        return state

    annotated_urls = state.get("annotated_image_urls") or []
    photos_cell = ""
    if isinstance(annotated_urls, list) and annotated_urls:
        photos_cell = "\n".join([str(u).strip() for u in annotated_urls[:3] if str(u).strip()])
        reply = reply + "\n\nAnnotated images:\n" + "\n".join([f"- {u}" for u in annotated_urls[:3]])

    sheets = SheetsTool(settings)
    sheets.append_conversation_ai_comment(
        checkin_id=checkin_id,
        remark=reply,
        status=state.get("checkin_status") or "",
        photos=photos_cell,
    )

    state["writeback_done"] = True
    (state.get("logs") or []).append("Wrote back AI comment to Conversation")

    # Teams post (only for new checkins)
    if (state.get("event_type") or "") == "CHECKIN_CREATED":
        try:
            webhook_url = (
                getattr(settings, "power_automate_webhook_url", "")  # new env
                or getattr(settings, "teams_webhook_url", "")        # fallback
            )
            client = TeamsClient(webhook_url)           
            if client.enabled():
                tenant_row_id = (state.get("tenant_id") or "").strip()
                project_name = (state.get("project_name") or "").strip()

                company_name_raw = (state.get("company_name") or "").strip()
                company_desc = (state.get("company_description") or "").strip()
                company_key_in_state = (state.get("company_key") or "").strip()

                # ✅ Fallback: if upstream didn't populate company_* fields reliably, derive now
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

                    # routing inputs (Power Automate filters on this)
                    "tenant_row_id": tenant_row_id,
                    "company_key_normalized": company_key_norm,
                    "company_key": company_key_norm,
                    "company_name": company_name_raw,
                    "company_description": company_desc,

                    # checkin info
                    "checkin_id": checkin_id,
                    "project_name": project_name,
                    "part_number": state.get("part_number") or "",
                    "status": state.get("checkin_status") or "",
                    "ai_reply": reply,  # IMPORTANT: send final reply with annotated links appended
                    "annotated_images": annotated_urls[:3] if isinstance(annotated_urls, list) else [],
                }


                client.post_message(payload)
                (state.get("logs") or []).append("Posted summary to Teams (company-routed payload)")
        except Exception as e:
            (state.get("logs") or []).append(f"Teams post failed: {e}")

    return state
