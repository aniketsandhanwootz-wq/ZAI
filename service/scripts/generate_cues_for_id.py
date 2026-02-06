# service/scripts/generate_cues_for_id.py
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import time
from datetime import date
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

import requests

from service.app.config import load_settings
from service.app.tools.sheets_tool import SheetsTool, _key, _norm_value
from service.app.tools.embed_tool import EmbedTool
from service.app.tools.vector_tool import VectorTool
from service.app.tools.llm_tool import LLMTool
from service.app.pipeline.nodes.rerank_context import rerank_context
from service.app.pipeline.nodes import generate_assembly_todo as gat

try:
    from service.app.tools.db_tool import DBTool
except Exception:
    DBTool = None  # type: ignore


# =============================================================================
# SECRETS (hardcode OK since not going to GitHub)
# =============================================================================

GOOGLE_SERVICE_ACCOUNT_FILE = "/Users/aniketsandhan/Desktop/ZAI/service/gcp_key.json"
GOOGLE_SHEET_ID = "1Vl5XKHGscj0XwO4-97zkxapMFGFDTSoauPgPOz1oVD4"

# IMPORTANT:
# If your LLMTool is Gemini-based, set GOOGLE_API_KEY (AIza...).
# If your LLMTool is OpenAI-based, set OPENAI_API_KEY (sk-...).
GOOGLE_API_KEY = "AIzaSyD3YaYMCXSSoVcHd_9hlwhRE6057RC2vfw"
OPENAI_API_KEY = ""  # keep empty if not used

DATABASE_URL = "postgresql://zai_u2sh_user:pQfl5NyYKOH4L1Oqm5RGbccjXDwecKG0@dpg-d50gvebe5dus73deqbcg-a.oregon-postgres.render.com/zai_u2sh?sslmode=require"

APPSHEET_APP_ID = "b9fe56b5-5bb2-40ca-a376-0557a85bd2c4"
APPSHEET_ACCESS_KEY = "V2-anLhb-XzPgN-K0c8E-hwzJJ-lPTwi-vmfrj-FJCEY-cHK22"
APPSHEET_TABLE = "Recommended checkins"

APPSHEET_COL_CUE = "Cue"
APPSHEET_COL_CUE_ID = "Cue ID"     # Key column -> must be unique
APPSHEET_COL_ID = "ID"
APPSHEET_COL_DATE = "Date"
APPSHEET_COL_CONTEXT = "Context"


# =============================================================================
# AppSheet API
# =============================================================================

def _post_json_with_retries(
    url: str,
    *,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    timeout: int = 60,
    max_attempts: int = 4,
    base_sleep: float = 0.8,
) -> requests.Response:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=timeout)
            return r
        except Exception as e:
            last_err = e
            if attempt >= max_attempts:
                break
            time.sleep(base_sleep * (2 ** (attempt - 1)) + random.random() * 0.2)
    raise RuntimeError(f"POST failed after {max_attempts} attempts: {last_err}")


def _appsheet_add_rows(*, rows: List[Dict[str, Any]], debug: bool = False) -> Dict[str, Any]:
    """
    AppSheet can return HTTP 200 even when row insert fails.
    So we MUST parse JSON and check for error fields.
    """
    app_id = quote(APPSHEET_APP_ID, safe="")
    table = quote(APPSHEET_TABLE, safe="")  # handles spaces
    url = f"https://api.appsheet.com/api/v2/apps/{app_id}/tables/{table}/Action"
    headers = {
        "ApplicationAccessKey": APPSHEET_ACCESS_KEY,
        "Content-Type": "application/json",
    }
    body = {
        "Action": "Add",
        "Properties": {
            "Locale": "en-US",
            "Timezone": "Asia/Kolkata",
        },
        "Rows": rows,
    }

    r = _post_json_with_retries(url, headers=headers, payload=body, timeout=60)

    text = r.text or ""
    if debug:
        print(f"[appsheet] status={r.status_code}")
        print(f"[appsheet] resp={text[:2000]}")

    if r.status_code >= 300:
        raise RuntimeError(f"AppSheet HTTP error: {r.status_code} {text[:2000]}")

    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"AppSheet non-JSON response (status={r.status_code}): {text[:2000]}")

    if isinstance(data, dict):
        if data.get("Errors"):
            raise RuntimeError(f"AppSheet row errors: {json.dumps(data.get('Errors'), ensure_ascii=False)[:2000]}")
        if data.get("error"):
            raise RuntimeError(f"AppSheet error: {json.dumps(data, ensure_ascii=False)[:2000]}")
        if data.get("success") is False:
            raise RuntimeError(f"AppSheet success=false: {json.dumps(data, ensure_ascii=False)[:2000]}")
    return data


# =============================================================================
# Helpers
# =============================================================================

def _apply_hardcoded_secrets_to_env() -> None:
    os.environ.setdefault("GOOGLE_SERVICE_ACCOUNT_FILE", GOOGLE_SERVICE_ACCOUNT_FILE)
    os.environ.setdefault("GOOGLE_SHEET_ID", GOOGLE_SHEET_ID)
    os.environ.setdefault("DATABASE_URL", DATABASE_URL)

    # Set BOTH; whichever LLMTool needs will be picked up.
    if GOOGLE_API_KEY:
        os.environ.setdefault("GOOGLE_API_KEY", GOOGLE_API_KEY)
    if OPENAI_API_KEY:
        os.environ.setdefault("OPENAI_API_KEY", OPENAI_API_KEY)


def _iso_date() -> str:
    return date.today().isoformat()  # "YYYY-MM-DD"


def _payload_hash(payload: dict) -> str:
    b = json.dumps(payload or {}, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def _stable_numeric_key(s: str, digits: int = 9) -> int:
    """
    Deterministic numeric ID for AppSheet Key column.
    Avoids collisions vs random IDs and supports idempotent reruns.
    """
    h = hashlib.sha256((s or "").encode("utf-8")).hexdigest()
    n = int(h[:16], 16)  # big int
    mod = 10 ** max(1, int(digits))
    return n % mod


def _clip_lines(text: str, max_lines: int) -> str:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    return "\n".join(lines[:max_lines]).strip()


def _build_appsheet_context(
    *,
    legacy_id: str,
    project_name: str,
    part_number: str,
    status_assembly: str,
    dispatch_date_str: str,
    stage: str,
    process_material: str,
    recent_activity: str,
    vector_risks: str,
    target_count: int,
) -> str:
    ra = _clip_lines(recent_activity, 8)
    vr = _clip_lines(vector_risks, 20)

    ctx = (
        f"legacy_id: {legacy_id}\n"
        f"project: {project_name}\n"
        f"part: {part_number}\n"
        f"status_assembly: {status_assembly}\n"
        f"dispatch_date: {dispatch_date_str}\n"
        f"stage: {stage}\n"
        f"target_count: {target_count}\n"
        f"\nprocess_material:\n{process_material}\n"
        f"\nrecent_activity:\n{ra}\n"
        f"\nvector_risks:\n{vr}\n"
    ).strip()
    return ctx[:4000]


def _split_lines(text: str) -> List[str]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    out: List[str] = []
    for ln in lines:
        ln = re.sub(r"^\-\s*\[\s*\]\s*", "", ln).strip()
        ln = re.sub(r"^\-\s*", "", ln).strip()
        ln = re.sub(r"^\•\s*", "", ln).strip()
        ln = re.sub(r"^\d+[\).]\s*", "", ln).strip()
        if ln:
            out.append(ln)
    seen = set()
    dedup: List[str] = []
    for x in out:
        k = re.sub(r"\s+", " ", x).strip().lower()
        if k and k not in seen:
            dedup.append(x)
            seen.add(k)
    return dedup


def _clamp_words(line: str, *, min_w: int = 6, max_w: int = 8) -> str:
    words = [w for w in re.split(r"\s+", (line or "").strip()) if w]
    if len(words) > max_w:
        words = words[:max_w]
    while len(words) < min_w:
        words.append("(unknown)")
    return " ".join(words).strip()


def _normalize_cues(text: str, *, count: int) -> List[str]:
    cand = _split_lines(text)
    out: List[str] = []
    for ln in cand:
        out.append(_clamp_words(ln))
        if len(out) >= count:
            break
    return out


def _build_prompt(
    *,
    stage: str,
    vector_risks: str,
    process_material: str,
    recent_activity: str,
    previous_chips: str,
    target_count: int,
) -> str:
    base = gat._load_prompt()
    filled = (
        base.replace("{{stage}}", stage or "(unknown)")
        .replace("{{vector_risks}}", vector_risks or "(none found)")
        .replace("{{process_material}}", process_material or "(unknown)")
        .replace("{{recent_activity}}", recent_activity or "(unknown)")
        .replace("{{previous_chips}}", (previous_chips or "").strip())
    )
    override = f"""
STRICT OUTPUT OVERRIDE (for this run):
- Output EXACTLY {int(target_count)} lines.
- Each line MUST be 6-8 words max.
- Each line must be actionable & specific.
- No headings, no extra text, no blank lines.
"""
    return (filled.strip() + "\n\n" + override.strip()).strip()


def _list_projects_rows(sheets: SheetsTool) -> List[Dict[str, Any]]:
    for name in ("list_projects", "list_project_rows", "get_projects", "projects"):
        fn = getattr(sheets, name, None)
        if callable(fn):
            return list(fn() or [])
    raise RuntimeError("SheetsTool does not expose project listing. Add list_projects() method.")


def _llm_generate_with_retries(llm: LLMTool, prompt: str, *, max_attempts: int = 3) -> str:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            return llm.generate_text(prompt)
        except Exception as e:
            last_err = e
            if attempt >= max_attempts:
                break
            time.sleep(0.8 * (2 ** (attempt - 1)) + random.random() * 0.2)
    raise RuntimeError(f"LLM failed after {max_attempts} attempts: {last_err}")


def _generate_for_one_legacy_id(
    *,
    settings: Any,
    sheets: SheetsTool,
    legacy_id: str,
    target_count: int,
    allow_non_mfg: bool,
    print_context: bool,
) -> Tuple[List[str], str, Dict[str, str]]:
    legacy_id = _norm_value(legacy_id)
    target_count = max(1, int(target_count))

    pr = sheets.get_project_by_legacy_id(legacy_id)
    if not pr:
        raise RuntimeError(f"Project row not found for legacy_id={legacy_id}")

    k_status = _key(sheets.map.col("project", "status_assembly"))
    k_pname = _key(sheets.map.col("project", "project_name"))
    k_part = _key(sheets.map.col("project", "part_number"))
    k_tenant = _key(sheets.map.col("project", "company_row_id"))
    k_prev = _key(sheets.map.col("project", "ai_critcal_point"))

    try:
        k_dispatch = _key(sheets.map.col("project", "dispatch_date"))
    except Exception:
        k_dispatch = ""

    status_val = _norm_value(pr.get(k_status, ""))
    if (not allow_non_mfg) and (status_val.strip().lower() != "mfg"):
        raise RuntimeError(f"Status gate: status_assembly='{status_val}' != 'mfg'")

    project_name = _norm_value(pr.get(k_pname, ""))
    part_number = _norm_value(pr.get(k_part, ""))
    tenant_id = _norm_value(pr.get(k_tenant, ""))
    previous_chips = _norm_value(pr.get(k_prev, ""))
    dispatch_date_str = _norm_value(pr.get(k_dispatch, "")) if k_dispatch else ""

    if not tenant_id:
        raise RuntimeError(f"Missing tenant_id for legacy_id={legacy_id}")

    query_text = (
        f"Micro inspection cues to avoid blind spots during manufacturing.\n"
        f"PROJECT_NAME: {project_name or '(unknown)'}\n"
        f"PART_NUMBER: {part_number or '(unknown)'}\n"
        f"LEGACY_ID: {legacy_id}\n"
        f"Output should be {target_count} quick micro-checks."
    ).strip()

    embedder = EmbedTool(settings)
    vector_db = VectorTool(settings)
    q = embedder.embed_query(query_text)

    problems = vector_db.search_incidents(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=80,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=legacy_id,
        vector_type="PROBLEM",
    )
    resolutions = vector_db.search_incidents(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=80,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=legacy_id,
        vector_type="RESOLUTION",
    )
    media = vector_db.search_incidents(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=60,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=legacy_id,
        vector_type="MEDIA",
    )
    ccp = vector_db.search_ccp_chunks(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=60,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=legacy_id,
    )
    dash = vector_db.search_dashboard_updates(
        tenant_id=tenant_id,
        query_embedding=q,
        top_k=30,
        project_name=project_name or None,
        part_number=part_number or None,
        legacy_id=legacy_id,
    )

    company_profile_text = ""
    try:
        row = vector_db.get_company_profile_by_tenant_row_id(tenant_row_id=tenant_id)
        if row:
            company_profile_text = (
                f"Company: {row.get('company_name','')}\n"
                f"Client description: {row.get('company_description','')}"
            ).strip()
    except Exception:
        pass

    tmp_state = {
        "thread_snapshot_text": query_text,
        "similar_problems": problems,
        "similar_resolutions": resolutions,
        "similar_media": media,
        "relevant_ccp_chunks": ccp,
        "relevant_dashboard_updates": dash,
        "logs": [],
    }
    tmp_state = rerank_context(settings, tmp_state)
    packed_context = _norm_value(tmp_state.get("packed_context", ""))

    # Optional optimization: if SheetsTool has a filtered function, use it.
    related_checkins: List[Dict[str, Any]] = []
    try:
        fn = getattr(sheets, "list_checkins_for_legacy_id", None)
        if callable(fn):
            related_checkins = list(fn(legacy_id) or [])[-10:]
        else:
            k_ci_legacy = _key(sheets.map.col("checkin", "legacy_id"))
            all_checkins = sheets.list_checkins()
            related_checkins = [
                c for c in (all_checkins or [])
                if _key(_norm_value((c or {}).get(k_ci_legacy, ""))) == _key(legacy_id)
            ][-10:]
    except Exception:
        related_checkins = []

    recent_activity = gat._fmt_recent_activity(related_checkins=related_checkins, sheets=sheets)

    recent_blob = f"{dispatch_date_str}\n{recent_activity}\n{packed_context}"
    stage = gat._infer_stage(dispatch_date_str=dispatch_date_str, recent_text_blob=recent_blob)

    process_material = gat._fmt_process_material(
        project_name=project_name,
        part_number=part_number,
        company_profile_text=company_profile_text,
    )

    vector_risks = gat._fmt_vector_risks(
        problems=problems,
        resolutions=resolutions,
        ccp=ccp,
        dash=dash,
        packed_context=packed_context,
    )

    prompt = _build_prompt(
        stage=stage,
        vector_risks=vector_risks,
        process_material=process_material,
        recent_activity=recent_activity,
        previous_chips=previous_chips,
        target_count=target_count,
    )

    llm = LLMTool(settings)
    raw = _llm_generate_with_retries(llm, prompt)
    cues = _normalize_cues(raw, count=target_count)

    if len(cues) < target_count:
        missing = target_count - len(cues)
        raw2 = _llm_generate_with_retries(llm, prompt + f"\n\nNow output EXACTLY {missing} NEW additional lines only.")
        cues2 = _normalize_cues(raw2, count=missing)
        existing = {re.sub(r"\s+", " ", x).strip().lower() for x in cues}
        for x in cues2:
            k = re.sub(r"\s+", " ", x).strip().lower()
            if k and k not in existing:
                cues.append(x)
                existing.add(k)
            if len(cues) >= target_count:
                break
    cues = cues[:target_count]

    context_blob = _build_appsheet_context(
        legacy_id=legacy_id,
        project_name=project_name,
        part_number=part_number,
        status_assembly=status_val,
        dispatch_date_str=dispatch_date_str,
        stage=stage,
        process_material=process_material,
        recent_activity=recent_activity,
        vector_risks=vector_risks,
        target_count=target_count,
    )

    if print_context:
        print(f"[debug] legacy_id={legacy_id} tenant_id={tenant_id} status_assembly={status_val} stage={stage}")

    meta = {"tenant_id": tenant_id}
    return cues, context_blob, meta


def _write_cues_to_appsheet(
    *,
    settings: Any,
    tenant_id: str,
    legacy_id: str,
    cues: List[str],
    context_blob: str,
    no_appsheet: bool,
    appsheet_debug: bool,
    print_context: bool,
) -> int:
    if not cues:
        return 0

    existing: Set[str] = set()
    db = None
    database_url = getattr(settings, "database_url", "") or os.getenv("DATABASE_URL", "")
    if DBTool is not None and database_url:
        try:
            db = DBTool(database_url)
            existing = db.existing_artifact_source_hashes(
                tenant_id=tenant_id,
                checkin_id=legacy_id,
                artifact_type="APPSHEET_CUE",
            )
        except Exception:
            db = None
            existing = set()

    rows_to_add: List[Dict[str, Any]] = []
    hashes_added: List[str] = []

    for cue in cues:
        h = _payload_hash({"legacy_id": legacy_id, "cue": cue})
        if h in existing:
            continue

        cue_id = _stable_numeric_key(f"{legacy_id}||{cue}", digits=9)

        rows_to_add.append(
            {
                APPSHEET_COL_CUE: cue,
                APPSHEET_COL_CUE_ID: cue_id,      # deterministic unique numeric
                APPSHEET_COL_ID: legacy_id,
                APPSHEET_COL_DATE: _iso_date(),   # YYYY-MM-DD
                APPSHEET_COL_CONTEXT: context_blob,
            }
        )
        hashes_added.append(h)

    if not rows_to_add:
        if print_context:
            print(f"[appsheet] {legacy_id}: nothing new (idempotent)")
        return 0

    if no_appsheet:
        if print_context:
            print(f"[no-appsheet] would insert {len(rows_to_add)} rows for {legacy_id}")
        return len(rows_to_add)

    _appsheet_add_rows(rows=rows_to_add, debug=appsheet_debug)

    if db is not None:
        for h in hashes_added:
            db.insert_artifact_no_fail(
                run_id=f"script::{legacy_id}",
                artifact_type="APPSHEET_CUE",
                url="appsheet_cues",
                meta={
                    "tenant_id": tenant_id,
                    "checkin_id": legacy_id,
                    "source_hash": h,
                    "legacy_id": legacy_id,
                },
            )

    if print_context:
        print(f"[appsheet] {legacy_id}: inserted_rows={len(rows_to_add)}")
    return len(rows_to_add)


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate cues and write to AppSheet DB table.")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--legacy-id", help="Single legacy id")
    mode.add_argument("--all-mfg", action="store_true", help="All Project rows where Status_assembly == mfg")

    ap.add_argument("--count", type=int, default=5)
    ap.add_argument("--allow-non-mfg", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--no-appsheet", action="store_true", help="Do not write; only print")
    ap.add_argument("--appsheet-debug", action="store_true", help="Print AppSheet API response")
    ap.add_argument("--print-context", action="store_true")
    args = ap.parse_args()

    _apply_hardcoded_secrets_to_env()

    settings = load_settings()
    sheets = SheetsTool(settings)

    target_count = max(1, int(args.count))
    total_inserted = 0

    if args.legacy_id:
        legacy_id = _norm_value(args.legacy_id)
        cues, context_blob, meta = _generate_for_one_legacy_id(
            settings=settings,
            sheets=sheets,
            legacy_id=legacy_id,
            target_count=target_count,
            allow_non_mfg=bool(args.allow_non_mfg),
            print_context=bool(args.print_context),
        )

        print("\n".join(cues).strip())

        total_inserted += _write_cues_to_appsheet(
            settings=settings,
            tenant_id=meta["tenant_id"],
            legacy_id=legacy_id,
            cues=cues,
            context_blob=context_blob,
            no_appsheet=bool(args.no_appsheet),
            appsheet_debug=bool(args.appsheet_debug),
            print_context=bool(args.print_context),
        )

    else:
        proj_rows = _list_projects_rows(sheets)
        k_status = _key(sheets.map.col("project", "status_assembly"))
        k_legacy = _key(sheets.map.col("project", "legacy_id"))

        ids: List[str] = []
        seen: Set[str] = set()
        for r in (proj_rows or []):
            st = _norm_value((r or {}).get(k_status, "")).strip().lower()
            if st != "mfg":
                continue
            lid = _norm_value((r or {}).get(k_legacy, ""))
            kk = _key(lid)
            if lid and kk and kk not in seen:
                ids.append(lid)
                seen.add(kk)

        if args.limit and int(args.limit) > 0:
            ids = ids[: int(args.limit)]

        for i, legacy_id in enumerate(ids, start=1):
            try:
                cues, context_blob, meta = _generate_for_one_legacy_id(
                    settings=settings,
                    sheets=sheets,
                    legacy_id=legacy_id,
                    target_count=target_count,
                    allow_non_mfg=True,
                    print_context=bool(args.print_context),
                )
                print(f"\n=== {i}/{len(ids)} :: {legacy_id} ===")
                print("\n".join(cues).strip())

                total_inserted += _write_cues_to_appsheet(
                    settings=settings,
                    tenant_id=meta["tenant_id"],
                    legacy_id=legacy_id,
                    cues=cues,
                    context_blob=context_blob,
                    no_appsheet=bool(args.no_appsheet),
                    appsheet_debug=bool(args.appsheet_debug),
                    print_context=bool(args.print_context),
                )

            except Exception as e:
                print(f"[ERROR] {legacy_id}: {e}")

            if args.sleep and float(args.sleep) > 0:
                time.sleep(float(args.sleep))

    if args.print_context:
        print(f"[done] total_rows_inserted={total_inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())