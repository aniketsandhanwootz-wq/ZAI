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
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

import requests
from dotenv import load_dotenv

from service.app.config import load_settings
from service.app.pipeline.nodes import generate_assembly_todo as gat
from service.app.pipeline.nodes.rerank_context import rerank_context
from service.app.tools.embed_tool import EmbedTool
from service.app.tools.llm_tool import LLMTool
from service.app.tools.sheets_tool import SheetsTool, _key, _norm_value
from service.app.tools.vector_tool import VectorTool

try:
    from service.app.tools.db_tool import DBTool
except Exception:
    DBTool = None  # type: ignore


# =============================================================================
# SECRETS (hardcode OK since not going to GitHub)
# =============================================================================

GOOGLE_SERVICE_ACCOUNT_FILE = "/Users/aniketsandhan/Desktop/ZAI/service/gcp_key.json"
GOOGLE_SHEET_ID = "1Vl5XKHGscj0XwO4-97zkxapMFGFDTSoauPgPOz1oVD4"

GOOGLE_API_KEY = "AIzaSyD3YaYMCXSSoVcHd_9hlwhRE6057RC2vfw"
OPENAI_API_KEY = ""  # keep empty if not used

DATABASE_URL = "postgresql://zai_u2sh_user:pQfl5NyYKOH4L1Oqm5RGbccjXDwecKG0@dpg-d50gvebe5dus73deqbcg-a.oregon-postgres.render.com/zai_u2sh?sslmode=require"

APPSHEET_APP_ID = "b9fe56b5-5bb2-40ca-a376-0557a85bd2c4"
APPSHEET_ACCESS_KEY = "V2-gJCAK-OBzLT-RGNKK-i0wbj-PfG14-szpFf-D4cpx-FNpgm"
APPSHEET_TABLE = "Recommended checkins"

APPSHEET_COL_CUE = "Cue"
APPSHEET_COL_CUE_ID = "Cue ID"  # Key column -> must be unique
APPSHEET_COL_ID = "ID"
APPSHEET_COL_DATE = "Date"
APPSHEET_COL_CONTEXT = "Context"

# Context clamp (AppSheet quirks)
APPSHEET_CONTEXT_SAFE_CHARS = 1000

# Context sparsity: target ~30% cues get context (min 2)
CONTEXT_TARGET_RATIO = 0.30
CONTEXT_MIN_ITEMS = 2
CONTEXT_MAX_ITEMS = 4

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


def _appsheet_action_rows(*, action: str, rows: List[Dict[str, Any]], debug: bool = False) -> Dict[str, Any]:
    if action not in ("Add", "Edit"):
        raise ValueError(f"Unsupported AppSheet action: {action}")

    app_id = quote(APPSHEET_APP_ID, safe="")
    table = quote(APPSHEET_TABLE, safe="")  # handles spaces
    url = f"https://api.appsheet.com/api/v2/apps/{app_id}/tables/{table}/Action"
    headers = {"ApplicationAccessKey": APPSHEET_ACCESS_KEY, "Content-Type": "application/json"}
    body = {
        "Action": action,
        "Properties": {"Locale": "en-US", "Timezone": "Asia/Kolkata"},
        "Rows": rows,
    }

    r = _post_json_with_retries(url, headers=headers, payload=body, timeout=60)
    text = r.text or ""

    if debug:
        print(f"[appsheet] action={action} status={r.status_code}")
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

_ID_PATTERNS = [
    re.compile(r"\(checkin_id=[^)]+\)", re.IGNORECASE),
    re.compile(r"\bcheckin[_\s-]*id\s*=\s*[A-Za-z0-9._-]+\b", re.IGNORECASE),
    re.compile(r"\bCHECKIN\s+[A-Za-z0-9._-]+\b", re.IGNORECASE),
    re.compile(r"\bcheckin\s+[A-Za-z0-9._-]+\b", re.IGNORECASE),
]


def _scrub_ids(text: str) -> str:
    s = (text or "").strip()
    for rx in _ID_PATTERNS:
        s = rx.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _trim(text: str, max_chars: int) -> str:
    s = (text or "").strip()
    if len(s) <= max_chars:
        return s
    return s[:max_chars].rstrip()


def _load_env_file() -> None:
    service_dir = Path(__file__).resolve().parents[1]
    env_path = service_dir / ".env"
    if not env_path.exists():
        raise RuntimeError(f".env not found at {env_path}")
    load_dotenv(dotenv_path=env_path, override=False)


def _apply_hardcoded_secrets_to_env() -> None:
    os.environ.setdefault("GOOGLE_SERVICE_ACCOUNT_FILE", GOOGLE_SERVICE_ACCOUNT_FILE)
    os.environ.setdefault("GOOGLE_SHEET_ID", GOOGLE_SHEET_ID)
    os.environ.setdefault("DATABASE_URL", DATABASE_URL)

    if GOOGLE_API_KEY:
        os.environ.setdefault("GOOGLE_API_KEY", GOOGLE_API_KEY)
        os.environ.setdefault("LLM_API_KEY", GOOGLE_API_KEY)
        os.environ.setdefault("EMBEDDING_API_KEY", GOOGLE_API_KEY)
        os.environ.setdefault("VISION_API_KEY", GOOGLE_API_KEY)
    if OPENAI_API_KEY:
        os.environ.setdefault("OPENAI_API_KEY", OPENAI_API_KEY)
        os.environ.setdefault("LLM_API_KEY", OPENAI_API_KEY)
        os.environ.setdefault("EMBEDDING_API_KEY", OPENAI_API_KEY)

    if os.getenv("WEBHOOK_SECRET", "").strip() == "":
        os.environ["WEBHOOK_SECRET"] = os.getenv("APPSHEET_WEBHOOK_SECRET", "").strip()


def _iso_date() -> str:
    return date.today().isoformat()


def _payload_hash(payload: dict) -> str:
    b = json.dumps(payload or {}, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def _stable_numeric_key(s: str, digits: int = 9) -> int:
    h = hashlib.sha256((s or "").encode("utf-8")).hexdigest()
    n = int(h[:16], 16)
    mod = 10 ** max(1, int(digits))
    return n % mod


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


# ---- Project sheet write: 5 dot-bullets to Project ("ZAI Recommendations") ----

def _format_project_bullets(cues: List[str], *, max_items: int = 5) -> str:
    cues = [(_clamp_words(c) if c else "").strip() for c in (cues or [])]
    cues = [c for c in cues if c]
    picked = cues[:max_items]
    if not picked:
        return ""
    return "\n".join([f"• {c}" for c in picked]).strip()


def _write_cues_to_project_sheet(
    *,
    sheets: SheetsTool,
    legacy_id: str,
    cues: List[str],
    max_items: int = 5,
    print_context: bool = False,
) -> bool:
    legacy_id = _norm_value(legacy_id)
    value = _format_project_bullets(cues, max_items=max_items)
    col_out = sheets.map.col("project", "ai_critcal_point")
    ok = sheets.update_project_cell_by_legacy_id(legacy_id, column_name=col_out, value=value)
    if print_context:
        if ok:
            print(f"[project] wrote {min(max_items, len(cues or []))} bullets to '{col_out}' for legacy_id={legacy_id}")
        else:
            print(f"[project] row not found; skipped write for legacy_id={legacy_id}")
    return bool(ok)


def _cue_keywords(cue: str) -> List[str]:
    toks = re.findall(r"[A-Za-z0-9]+", (cue or "").lower())
    bad = {
        "the", "and", "or", "toh", "hai", "ya", "ke", "ka", "ki", "se", "mein", "me",
        "na", "nahi", "to", "do", "check", "quick", "verify", "ensure", "look"
    }
    out = []
    for t in toks:
        if len(t) < 3:
            continue
        if t in bad:
            continue
        out.append(t)
    seen = set()
    dedup = []
    for t in out:
        if t not in seen:
            dedup.append(t)
            seen.add(t)
    return dedup[:10]


def _extract_evidence_snippets(
    *,
    vector_risks: str,
    recent_activity: str,
    cue: str,
    max_snips: int = 2,
) -> List[str]:
    kws = _cue_keywords(cue)
    vr_lines = [ln.strip() for ln in (vector_risks or "").splitlines() if ln.strip()]
    ra_lines = [ln.strip("- ").strip() for ln in (recent_activity or "").splitlines() if ln.strip()]

    scored: List[Tuple[int, str]] = []

    for ln in vr_lines:
        low = ln.lower()
        score = sum(1 for k in kws if k in low)
        if any(x in low for x in ("resolution", "what worked", "closure", "defect", "rework", "reject", "scrap", "gauge")):
            score += 2
        if score > 0:
            scored.append((score, ln))

    for ln in ra_lines[-12:]:
        low = ln.lower()
        score = sum(1 for k in kws if k in low)
        if any(x in low for x in ("update", "rm", "raw", "spectro", "tc", "qc", "inspection", "rework", "hold", "dispatch")):
            score += 1
        if score > 0:
            scored.append((score, ln))

    scored.sort(key=lambda x: x[0], reverse=True)

    out: List[str] = []
    for _, ln in scored:
        s = _scrub_ids(ln)
        s = re.sub(r"\s+", " ", s).strip()
        s = re.sub(r"^RESOLUTIONS\s*\(.*?\)\s*:\s*", "", s, flags=re.IGNORECASE).strip()
        s = re.sub(r"^RESOLUTION\s*/\s*WHAT\s*WORKED\s*\(.*?\)\s*:\s*", "", s, flags=re.IGNORECASE).strip()
        s = re.sub(r"^Closure notes\s*\(.*?\)\s*:\s*", "", s, flags=re.IGNORECASE).strip()
        s = re.sub(r"^\d+\.\s*", "", s).strip()
        if s:
            out.append(_trim(s, 170))
        if len(out) >= max_snips:
            break

    if not out and vr_lines:
        out.append(_trim(_scrub_ids(vr_lines[0]), 170))

    seen = set()
    dedup = []
    for s in out:
        k = s.lower()
        if k not in seen:
            dedup.append(s)
            seen.add(k)
    return dedup[:max_snips]


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


# =============================================================================
# NEW: Context only for ~30% cues, and only when non-obvious
# =============================================================================

_OBVIOUS_HINTS = re.compile(
    r"\b(check|verify|ensure|confirm|clean|debur|burr|scratch|rust|crack|hole|thread|torque|fit|weld|spatter|paint|coat|finish|dimension|tolerance|marking|label)\b",
    re.IGNORECASE,
)


def _is_obvious_cue(cue: str) -> bool:
    """
    Heuristic: very common shop-floor checks are usually self-explanatory.
    """
    cue = (cue or "").strip()
    if not cue:
        return True
    # If cue is generic and matches common terms, treat as obvious.
    if _OBVIOUS_HINTS.search(cue):
        # BUT if it's highly specific (numbers/standards), may still need context
        if re.search(r"\b\d{2,}\b", cue) or re.search(r"\b(mm|micron|gsm|rc|hrc|iso|astm)\b", cue, re.I):
            return False
        return True
    return False


def _pick_cues_for_context(cues: List[str], vector_risks: str, recent_activity: str) -> Set[str]:
    """
    Choose ~30% cues to attach context, prioritizing ones that:
      - are NOT obvious by heuristic, OR
      - have strong matched evidence snippets
    """
    cues = list(cues or [])
    if not cues:
        return set()

    n = len(cues)
    target = int(round(n * CONTEXT_TARGET_RATIO))
    target = max(CONTEXT_MIN_ITEMS, target)
    target = min(CONTEXT_MAX_ITEMS, target, n)

    scored: List[Tuple[int, str]] = []

    for cue in cues:
        # base score: non-obvious higher
        score = 2 if not _is_obvious_cue(cue) else 0

        # evidence match score
        snips = _extract_evidence_snippets(
            vector_risks=vector_risks,
            recent_activity=recent_activity,
            cue=cue,
            max_snips=2,
        )
        # more snippets + longer content => higher value for context
        score += min(3, len(snips))
        if snips and any(len(s) > 80 for s in snips):
            score += 1

        scored.append((score, cue))

    # Prefer higher scores, stable tie-break by cue text
    scored.sort(key=lambda x: (-x[0], x[1].lower()))

    picked: List[str] = []
    for score, cue in scored:
        if score <= 0:
            continue
        picked.append(cue)
        if len(picked) >= target:
            break

    # If still not enough, fill from top regardless (but still avoid obvious if possible)
    if len(picked) < target:
        for _, cue in scored:
            if cue in picked:
                continue
            if _is_obvious_cue(cue) and len(picked) < target:
                # only take obvious if we must fill
                picked.append(cue)
            if len(picked) >= target:
                break

    return set(picked[:target])


def _build_context_lines_for_selected_cues(
    *,
    llm: LLMTool,
    cues: List[str],
    selected: Set[str],
    vector_risks: str,
    recent_activity: str,
) -> Dict[str, str]:
    """
    Generate 1-3 lines per selected cue:
      - why needed
      - what it refers to (checkin/update/process/raw_material/ccp)
      - summary of what happened
    For non-selected cues: empty string.
    """
    out: Dict[str, str] = {c: "" for c in cues}
    if not cues or not selected:
        return out

    # Prepare per-cue evidence blocks
    items: List[str] = []
    selected_list = [c for c in cues if c in selected]

    for i, cue in enumerate(selected_list, start=1):
        snips = _extract_evidence_snippets(vector_risks=vector_risks, recent_activity=recent_activity, cue=cue, max_snips=2)
        ev = "\n".join([f"- {s}" for s in snips if s.strip()])
        items.append(f"{i}. {cue}\nSignals:\n{ev}".strip())

    pack = "\n\n".join(items).strip()
    n = len(selected_list)

    prompt = f"""
You are writing short context notes for a shop-floor cue list.

For ONLY the cues below, write context when it helps understanding.
Each context should be 1 to 3 lines ONLY.
It must explain WHY it matters and WHAT it refers to:
- Reference type must be one of: checkin, dashboard update, CCP, raw material, process, boughtout, general
- Summarize what happened in that reference in short.

Rules:
- Output EXACTLY {n} blocks.
- Block format MUST be: i|LINE1\\nLINE2(optional)\\nLINE3(optional)
- No Stage, no Dispatch date, no IDs, no links.
- Keep each line <= 90 chars.

CUES + SIGNALS:
{pack}
""".strip()

    raw = _llm_generate_with_retries(llm, prompt)
    lines = [ln.rstrip() for ln in raw.splitlines() if ln.strip()]

    idx_to_text: Dict[int, str] = {}
    # Parse blocks: detect "i|..." then keep subsequent lines that are indented? (model returns \n escapes or real newlines)
    # We enforce by asking for literal \n in output; still be tolerant.
    buf_i: Optional[int] = None
    buf: List[str] = []

    def flush():
        nonlocal buf_i, buf
        if buf_i is None:
            return
        txt = "\n".join(buf).strip()
        txt = txt.replace("\\n", "\n")
        # keep 1-3 lines, scrub ids, clamp
        txt_lines = [re.sub(r"\s+", " ", _scrub_ids(x)).strip() for x in txt.splitlines() if x.strip()]
        txt_lines = txt_lines[:3]
        txt = "\n".join(txt_lines).strip()
        if txt:
            idx_to_text[buf_i] = _trim(txt, APPSHEET_CONTEXT_SAFE_CHARS)
        buf_i = None
        buf = []

    for ln in lines:
        if "|" in ln:
            # start of a new block
            flush()
            left, right = ln.split("|", 1)
            left = re.sub(r"[^\d]", "", left.strip())
            if not left:
                continue
            buf_i = int(left)
            buf = [right.strip()]
        else:
            # continuation (tolerate)
            if buf_i is not None:
                buf.append(ln.strip())

    flush()

    for i, cue in enumerate(selected_list, start=1):
        txt = idx_to_text.get(i, "")
        if txt:
            out[cue] = txt

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


def _generate_for_one_legacy_id(
    *,
    settings: Any,
    sheets: SheetsTool,
    legacy_id: str,
    target_count: int,
    allow_non_mfg: bool,
    print_context: bool,
) -> Tuple[List[str], Dict[str, str], Dict[str, str]]:
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

    tmp_state = {
        "thread_snapshot_text": query_text,
        "similar_problems": problems,
        "similar_resolutions": resolutions,
        "relevant_ccp_chunks": ccp,
        "relevant_dashboard_updates": dash,
        "logs": [],
    }
    tmp_state = rerank_context(settings, tmp_state)
    packed_context = _norm_value(tmp_state.get("packed_context", ""))

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

    # stage still used INSIDE prompt and why weighting, but not written into AppSheet context
    recent_blob = f"{dispatch_date_str}\n{recent_activity}\n{packed_context}"
    stage = gat._infer_stage(dispatch_date_str=dispatch_date_str, recent_text_blob=recent_blob)

    vector_risks = gat._fmt_vector_risks(
        problems=problems,
        resolutions=resolutions,
        ccp=ccp,
        dash=dash,
        packed_context=packed_context,
    )

    process_material = gat._fmt_process_material(
        project_name=project_name,
        part_number=part_number,
        company_profile_text="",  # not needed here
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

    # Select only some cues to have context
    selected = _pick_cues_for_context(cues, vector_risks=vector_risks, recent_activity=recent_activity)

    # Build context only for selected cues
    context_by_cue = _build_context_lines_for_selected_cues(
        llm=llm,
        cues=cues,
        selected=selected,
        vector_risks=vector_risks,
        recent_activity=recent_activity,
    )

    if print_context:
        print(f"[debug] legacy_id={legacy_id} tenant_id={tenant_id} status_assembly={status_val} stage={stage}")
        print(f"[debug] context_selected={len(selected)}/{len(cues)}")

    meta = {"tenant_id": tenant_id}
    return cues, context_by_cue, meta


def _write_cues_to_appsheet(
    *,
    settings: Any,
    tenant_id: str,
    legacy_id: str,
    cues: List[str],
    context_by_cue: Dict[str, str],
    no_appsheet: bool,
    appsheet_debug: bool,
    print_context: bool,
    force_update: bool,
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
    rows_to_edit: List[Dict[str, Any]] = []
    hashes_added: List[str] = []
    hashes_edited: List[str] = []

    for cue in cues:
        h = _payload_hash({"legacy_id": legacy_id, "cue": cue})
        cue_id = _stable_numeric_key(f"{legacy_id}||{cue}", digits=9)

        ctx = (context_by_cue.get(cue, "") or "").strip()
        ctx = _trim(ctx, APPSHEET_CONTEXT_SAFE_CHARS)

        row = {
            APPSHEET_COL_CUE: cue,
            APPSHEET_COL_CUE_ID: cue_id,  # key
            APPSHEET_COL_ID: legacy_id,
            APPSHEET_COL_DATE: _iso_date(),
            APPSHEET_COL_CONTEXT: ctx,  # empty for most cues
        }

        if h in existing:
            if force_update:
                rows_to_edit.append(row)
                hashes_edited.append(h)
            continue

        rows_to_add.append(row)
        hashes_added.append(h)

    if not rows_to_add and not rows_to_edit:
        if print_context:
            print(f"[appsheet] {legacy_id}: nothing to do")
        return 0

    if no_appsheet:
        if print_context:
            print(f"[no-appsheet] would add={len(rows_to_add)} edit={len(rows_to_edit)} rows for {legacy_id}")
        return len(rows_to_add) + len(rows_to_edit)

    if rows_to_add:
        _appsheet_action_rows(action="Add", rows=rows_to_add, debug=appsheet_debug)
    if rows_to_edit:
        _appsheet_action_rows(action="Edit", rows=rows_to_edit, debug=appsheet_debug)

    if db is not None:
        for h in hashes_added:
            db.insert_artifact_no_fail(
                run_id=f"script::{legacy_id}",
                artifact_type="APPSHEET_CUE",
                url="appsheet_cues",
                meta={"tenant_id": tenant_id, "checkin_id": legacy_id, "source_hash": h, "legacy_id": legacy_id},
            )
        for h in hashes_edited:
            db.insert_artifact_no_fail(
                run_id=f"script::{legacy_id}",
                artifact_type="APPSHEET_CUE_EDIT",
                url="appsheet_cues",
                meta={"tenant_id": tenant_id, "checkin_id": legacy_id, "source_hash": h, "legacy_id": legacy_id},
            )

    if print_context:
        print(f"[appsheet] {legacy_id}: added={len(rows_to_add)} edited={len(rows_to_edit)}")
    return len(rows_to_add) + len(rows_to_edit)


def main() -> int:
    _load_env_file()

    ap = argparse.ArgumentParser(description="Generate cues and write to AppSheet DB table + Project sheet.")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--legacy-id", help="Single legacy id")
    mode.add_argument("--all-mfg", action="store_true", help="All Project rows where Status_assembly == mfg")

    ap.add_argument("--count", type=int, default=10)
    ap.add_argument("--allow-non-mfg", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--no-appsheet", action="store_true", help="Do not write; only print")
    ap.add_argument("--no-project", action="store_true", help="Do not write 5 bullet cues to Project sheet")
    ap.add_argument("--appsheet-debug", action="store_true", help="Print AppSheet API response")
    ap.add_argument("--print-context", action="store_true")
    ap.add_argument("--force-update", action="store_true", help="Edit existing rows (by Cue ID) to update Context.")
    args = ap.parse_args()

    _apply_hardcoded_secrets_to_env()

    settings = load_settings()
    sheets = SheetsTool(settings)

    target_count = max(1, int(args.count))
    total_acted = 0

    if args.legacy_id:
        legacy_id = _norm_value(args.legacy_id)
        cues, context_by_cue, meta = _generate_for_one_legacy_id(
            settings=settings,
            sheets=sheets,
            legacy_id=legacy_id,
            target_count=target_count,
            allow_non_mfg=bool(args.allow_non_mfg),
            print_context=bool(args.print_context),
        )

        print("\n".join(cues).strip())

        if not bool(args.no_project):
            _write_cues_to_project_sheet(
                sheets=sheets,
                legacy_id=legacy_id,
                cues=cues,
                max_items=5,
                print_context=bool(args.print_context),
            )

        total_acted += _write_cues_to_appsheet(
            settings=settings,
            tenant_id=meta["tenant_id"],
            legacy_id=legacy_id,
            cues=cues,
            context_by_cue=context_by_cue,
            no_appsheet=bool(args.no_appsheet),
            appsheet_debug=bool(args.appsheet_debug),
            print_context=bool(args.print_context),
            force_update=bool(args.force_update),
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
                cues, context_by_cue, meta = _generate_for_one_legacy_id(
                    settings=settings,
                    sheets=sheets,
                    legacy_id=legacy_id,
                    target_count=target_count,
                    allow_non_mfg=True,
                    print_context=bool(args.print_context),
                )

                print(f"\n=== {i}/{len(ids)} :: {legacy_id} ===")
                print("\n".join(cues).strip())

                if not bool(args.no_project):
                    _write_cues_to_project_sheet(
                        sheets=sheets,
                        legacy_id=legacy_id,
                        cues=cues,
                        max_items=5,
                        print_context=bool(args.print_context),
                    )

                total_acted += _write_cues_to_appsheet(
                    settings=settings,
                    tenant_id=meta["tenant_id"],
                    legacy_id=legacy_id,
                    cues=cues,
                    context_by_cue=context_by_cue,
                    no_appsheet=bool(args.no_appsheet),
                    appsheet_debug=bool(args.appsheet_debug),
                    print_context=bool(args.print_context),
                    force_update=bool(args.force_update),
                )

            except Exception as e:
                print(f"[ERROR] {legacy_id}: {e}")

            if args.sleep and float(args.sleep) > 0:
                time.sleep(float(args.sleep))

    if args.print_context:
        print(f"[done] total_rows_acted={total_acted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())