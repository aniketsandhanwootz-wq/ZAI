# service/scripts/backfill_appsheet_cues.py
from __future__ import annotations

import re
import json
import hashlib
import secrets
import string
from datetime import datetime

from service.app.config import load_settings
from service.app.tools.sheets_tool import SheetsTool, _key, _norm_value
from service.app.tools.llm_tool import LLMTool
from service.app.tools.db_tool import DBTool
from service.app.integrations.appsheet_client import AppSheetClient

_ALPHANUM = string.ascii_letters + string.digits

def _rand_cue_id(n: int = 10) -> str:
    return "".join(secrets.choice(_ALPHANUM) for _ in range(n))

def _now_timestamp_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.now(ZoneInfo("Asia/Kolkata"))
    except Exception:
        dt = datetime.now()
    return dt.strftime("%m/%d/%y %I:%M %p")

def _payload_hash(payload: dict) -> str:
    b = json.dumps(payload or {}, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b).hexdigest()

def _split_lines_10(text: str) -> list[str]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    out: list[str] = []
    for ln in lines:
        ln = re.sub(r"^\-\s*\[\s*\]\s*", "", ln).strip()
        ln = re.sub(r"^\-\s*", "", ln).strip()
        ln = re.sub(r"^\•\s*", "", ln).strip()
        ln = re.sub(r"^\d+[\).]\s*", "", ln).strip()
        if ln:
            out.append(ln)
        if len(out) >= 10:
            break
    # dedup
    seen = set()
    dedup: list[str] = []
    for x in out:
        k = re.sub(r"\s+", " ", x).strip().lower()
        if k and k not in seen:
            dedup.append(x)
            seen.add(k)
    return dedup[:10]

def main():
    settings = load_settings()
    sheets = SheetsTool(settings)
    llm = LLMTool(settings)
    db = DBTool(settings.database_url)
    client = AppSheetClient(settings)

    if not client.enabled():
        raise SystemExit("AppSheet not enabled. Set APPSHEET_APP_ID / APPSHEET_ACCESS_KEY / APPSHEET_CUES_TABLE")

    projects = sheets.list_projects()

    k_legacy = _key(sheets.map.col("project", "legacy_id"))
    k_status = _key(sheets.map.col("project", "status_assembly"))
    k_pname = _key(sheets.map.col("project", "project_name"))
    k_part  = _key(sheets.map.col("project", "part_number"))
    k_tenant = _key(sheets.map.col("project", "company_row_id"))

    generated_at = _now_timestamp_str()

    for pr in projects:
        legacy_id = _norm_value(pr.get(k_legacy, ""))
        status = _norm_value(pr.get(k_status, ""))
        if (status or "").strip().lower() != "mfg":
            continue
        if not legacy_id:
            continue

        tenant_id = _norm_value(pr.get(k_tenant, ""))
        if not tenant_id:
            continue

        project_name = _norm_value(pr.get(k_pname, ""))
        part_number = _norm_value(pr.get(k_part, ""))

        # idempotency scope: tenant_id + legacy_id
        existing = db.existing_artifact_source_hashes(
            tenant_id=tenant_id,
            checkin_id=legacy_id,
            artifact_type="APPSHEET_CUE",
        )

        prompt = f"""
You are a senior manufacturing quality inspector.
Generate EXACTLY 10 short micro-inspection cues for shopfloor team.

Constraints:
- Each cue max 8 words.
- No numbering, no bullets.
- One cue per line.
- Make cues specific and practical.

Project: {project_name or "(unknown)"}
Part: {part_number or "(unknown)"}
Project ID (legacy_id): {legacy_id}
""".strip()

        raw = llm.generate_text(prompt).strip()
        cues = _split_lines_10(raw)
        if not cues:
            continue

        cue_items = []
        for cue in cues:
            h = _payload_hash({"legacy_id": legacy_id, "cue": cue})
            if h in existing:
                continue
            cue_items.append({"cue_id": _rand_cue_id(), "cue": cue})

        if not cue_items:
            continue

        client.add_cues_rows(
            legacy_id=legacy_id,
            cue_items=cue_items,
            generated_at=generated_at,
        )

        # record as artifacts (run_id not available; store run_id as "backfill")
        for cue in cue_items:
            h = _payload_hash({"legacy_id": legacy_id, "cue": cue["cue"]})
            db.insert_artifact(
                run_id="backfill_appsheet_cues",
                artifact_type="APPSHEET_CUE",
                url="appsheet_cues",
                meta={
                    "tenant_id": tenant_id,
                    "checkin_id": legacy_id,
                    "source_hash": h,
                    "legacy_id": legacy_id,
                    "cue_id": cue["cue_id"],
                },
            )

        print(f"[OK] legacy_id={legacy_id} appended={len(cue_items)}")

if __name__ == "__main__":
    main()