# service/scripts/backfill_zai_cues_from_file.py
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import time
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv

from service.app.config import load_settings
from service.app.tools.sheets_tool import SheetsTool, _key, _norm_value
from service.app.tools.zai_cues_log_tool import ZaiCuesLogTool, ZaiCuesLogRow, _now_ist_str

from service.app.tools.embed_tool import EmbedTool
from service.app.tools.llm_tool import LLMTool
from service.app.tools.vector_tool import VectorTool
from service.app.pipeline.nodes.rerank_context import rerank_context
from service.app.integrations.appsheet_client import AppSheetClient

ZAI_CUES_TEMPERATURE = 0.4

# ----------------------------
# Repo + env
# ----------------------------

def _repo_root() -> Path:
    # .../service/scripts/backfill_zai_cues_from_file.py -> parents[2] == repo root
    return Path(__file__).resolve().parents[2]

def _load_env_service_dotenv() -> None:
    env_path = _repo_root() / "service" / ".env"
    if not env_path.exists():
        raise RuntimeError(f".env not found at {env_path}")
    load_dotenv(dotenv_path=env_path, override=False)

# ----------------------------
# Small helpers
# ----------------------------

def _iso_date() -> str:
    return date.today().isoformat()

def _payload_hash(payload: dict) -> str:
    b = json.dumps(payload or {}, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b).hexdigest()

def _stable_numeric_key(s: str, digits: int = 9) -> str:
    """
    AppSheet key column can be text or number depending on table config.
    We return string of digits to be safe.
    """
    h = hashlib.sha256((s or "").encode("utf-8")).hexdigest()
    n = int(h[:16], 16)
    mod = 10 ** max(1, int(digits))
    return str(n % mod)

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

_JSON_NOISE = re.compile(
    r"""(?ix)
    ^\s*[\{\}\[\],]*\s*$ |
    ^\s*"\w+"\s*:\s*\[?\s*$ |
    ^\s*\]\s*,?\s*$ |
    ^\s*\{\s*$ |
    ^\s*\}\s*$ |
    ^\s*"\s*cues\s*"\s*:\s*\[?\s*$ |
    ^\s*\d+\s*$ |
    ^\s*\d{1,4}([/-]\d{1,2}){1,2}\s*$
    """,
)

def _split_lines(text: str) -> List[str]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    out: List[str] = []
    for ln in lines:
        ln = re.sub(r"^\-\s*\[\s*\]\s*", "", ln).strip()
        ln = re.sub(r"^\-\s*", "", ln).strip()
        ln = re.sub(r"^\•\s*", "", ln).strip()
        ln = re.sub(r"^\d+[\).]\s*", "", ln).strip()
        if _JSON_NOISE.match(ln):
            continue
        if ln:
            out.append(ln)
    # dedup
    seen = set()
    dedup: List[str] = []
    for x in out:
        k = re.sub(r"\s+", " ", x).strip().lower()
        if k and k not in seen:
            dedup.append(x)
            seen.add(k)
    return dedup

def _parse_cues_json(raw: str) -> Optional[List[str]]:
    if not raw:
        return None
    s = raw.strip()
    try:
        obj = json.loads(s)
        if isinstance(obj, dict) and isinstance(obj.get("cues"), list):
            return [str(x).strip() for x in obj["cues"] if str(x).strip()]
    except Exception:
        pass
    m = re.search(r"\{.*\}", s, flags=re.S)
    if m:
        try:
            obj = json.loads(m.group(0))
            if isinstance(obj, dict) and isinstance(obj.get("cues"), list):
                return [str(x).strip() for x in obj["cues"] if str(x).strip()]
        except Exception:
            pass
    return None

def _clean_cue_text(s: str) -> str:
    s = (s or "").strip().strip('"').strip()
    s = re.sub(r"\s+", " ", s)
    s = s.rstrip(",").strip()
    return s

def _clamp_words(line: str, *, max_w: int = 10) -> str:
    words = [w for w in re.split(r"\s+", (line or "").strip()) if w]
    if len(words) > max_w:
        words = words[:max_w]
    return " ".join(words).strip()

def _normalize_cues(text: str, *, count: int) -> List[str]:
    parsed = _parse_cues_json(text)
    if parsed:
        out: List[str] = []
        for x in parsed:
            x = _clean_cue_text(x)
            if x:
                out.append(_clamp_words(x))
            if len(out) >= count:
                break
        return out[:count]

    cand = _split_lines(text)
    out: List[str] = []
    for ln in cand:
        ln = _clean_cue_text(ln)
        if not ln:
            continue
        out.append(_clamp_words(ln))
        if len(out) >= count:
            break
    return out

def _format_project_bullets(cues: List[str], *, max_items: int = 5) -> str:
    cues = [(_clamp_words(c) if c else "").strip() for c in (cues or [])]
    cues = [c for c in cues if c]
    picked = cues[:max_items]
    if not picked:
        return ""
    return "\n".join([f"• {c}" for c in picked]).strip()

def _read_legacy_ids(path: Path) -> List[str]:
    if not path.exists():
        raise RuntimeError(f"legacy_ids file not found: {path}")
    out: List[str] = []
    seen: Set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if not s:
            continue
        if s.startswith("#"):
            continue
        s = _norm_value(s)
        k = _key(s)
        if s and k and k not in seen:
            out.append(s)
            seen.add(k)
    return out

def _llm_generate_with_retries(
    llm: LLMTool,
    prompt: str,
    *,
    temperature: float = ZAI_CUES_TEMPERATURE,
    max_attempts: int = 3,
) -> str:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            return llm.generate_text(prompt, temperature=temperature)
        except Exception as e:
            last_err = e
            if attempt >= max_attempts:
                break
            time.sleep(0.8 * (2 ** (attempt - 1)) + random.random() * 0.2)
    raise RuntimeError(f"LLM failed after {max_attempts} attempts: {last_err}")

# ----------------------------
# Prompt loading (repo-relative)
# ----------------------------

def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()

def _load_prompt_from_packages(name: str) -> str:
    p = _repo_root() / "packages" / "prompts" / name
    if not p.exists():
        raise RuntimeError(f"Prompt file not found: {p}")
    return _read_text(p)

def _build_prompt(
    *,
    stage: str,
    vector_risks: str,
    process_material: str,
    recent_activity: str,
    previous_chips: str,
    target_count: int,
) -> str:
    # You already had zai_cues_10.md in repo earlier; we use it here.
    base = _load_prompt_from_packages("zai_cues_10.md")
    filled = (
        base.replace("{{stage}}", stage or "(unknown)")
            .replace("{{vector_risks}}", vector_risks or "(none found)")
            .replace("{{process_material}}", process_material or "(unknown)")
            .replace("{{recent_activity}}", recent_activity or "(unknown)")
            .replace("{{previous_chips}}", (previous_chips or "").strip())
    )

    override = f"""
STRICT OUTPUT OVERRIDE (for this run):
- Output MUST be VALID JSON ONLY.
- JSON schema exactly: {{"cues": ["..."]}} with EXACTLY {int(target_count)} strings.
- No extra keys, no markdown, no commentary, no trailing text.
- Each cue <= 10 words.
""".strip()

    return (filled.strip() + "\n\n" + override).strip()

# ----------------------------
# Minimal formatters (safe fallbacks)
# ----------------------------

def _fmt_recent_activity(*, related_checkins: List[Dict[str, Any]]) -> str:
    out: List[str] = []
    for r in (related_checkins or [])[-10:]:
        if not isinstance(r, dict):
            continue
        msg = ""
        for k in ("remarks", "remark", "message", "text", "note", "notes", "description"):
            v = r.get(k)
            if v:
                msg = str(v).strip()
                break
        if msg:
            msg = re.sub(r"\s+", " ", msg)[:180]
            out.append(f"- {msg}")
    return "\n".join(out).strip()

def _infer_stage(*, dispatch_date_str: str, recent_text_blob: str) -> str:
    blob = (recent_text_blob or "").lower()
    if any(x in blob for x in ("final", "dispatch", "packing", "ship", "ready")):
        return "Final Stage"
    if any(x in blob for x in ("mid", "wip", "in progress", "machining", "welding", "assembly")):
        return "Mid Stage"
    return "Early Stage"

def _fmt_process_material(*, project_name: str, part_number: str) -> str:
    return f"PROJECT: {project_name}\nPART: {part_number}".strip()

def _fmt_vector_risks(
    *,
    problems: List[Dict[str, Any]],
    resolutions: List[Dict[str, Any]],
    ccp: List[Dict[str, Any]],
    dash: List[Dict[str, Any]],
    packed_context: str,
) -> str:
    def pick_lines(items: List[Dict[str, Any]], label: str, n: int) -> List[str]:
        out = []
        for it in (items or [])[:n]:
            if not isinstance(it, dict):
                continue
            txt = ""
            for k in ("text", "content", "chunk", "summary", "remarks", "note", "title", "update_message"):
                v = it.get(k)
                if v:
                    txt = str(v).strip()
                    break
            if txt:
                txt = re.sub(r"\s+", " ", txt)[:220]
                out.append(f"{label}: {txt}")
        return out

    lines: List[str] = []
    lines += pick_lines(resolutions, "RESOLUTION", 6)
    lines += pick_lines(problems, "PROBLEM", 6)
    lines += pick_lines(ccp, "CCP", 4)
    lines += pick_lines(dash, "UPDATE", 4)

    if packed_context:
        pc = re.sub(r"\s+", " ", packed_context).strip()[:400]
        if pc:
            lines.append(f"CONTEXT: {pc}")

    return "\n".join(lines).strip()

# ----------------------------
# Core generation (single legacy_id)
# ----------------------------

def generate_cues_for_legacy_id(
    *,
    settings: Any,
    sheets: SheetsTool,
    legacy_id: str,
    target_count: int,
) -> Tuple[List[str], bool, str, str, str, str]:
    """
    Returns:
      cues, rerank_used, tenant_id, status_assembly, chips_str, cues10_json
    """
    legacy_id = _norm_value(legacy_id)
    pr = sheets.get_project_by_legacy_id(legacy_id)
    if not pr:
        raise RuntimeError(f"Project row not found for legacy_id={legacy_id}")

    # mapping keys are casefold headers in SheetsTool row dict
    k_status = _key(sheets.map.col("project", "status_assembly"))
    k_pname = _key(sheets.map.col("project", "project_name"))
    k_part = _key(sheets.map.col("project", "part_number"))
    k_tenant = _key(sheets.map.col("project", "company_row_id"))
    k_prev = _key(sheets.map.col("project", "ai_critcal_point"))
    k_dispatch = _key(sheets.map.col("project", "dispatch_date"))

    status_val = _norm_value(pr.get(k_status, ""))
    project_name = _norm_value(pr.get(k_pname, ""))
    part_number = _norm_value(pr.get(k_part, ""))
    tenant_id = _norm_value(pr.get(k_tenant, ""))
    previous_chips = _norm_value(pr.get(k_prev, ""))
    dispatch_date_str = _norm_value(pr.get(k_dispatch, ""))

    if not tenant_id:
        raise RuntimeError(f"Missing tenant_id (Company row id) for legacy_id={legacy_id}")

    # ---- Vector retrieval + rerank ----
    query_text = (
        "Micro inspection cues to avoid blind spots during manufacturing.\n"
        f"PROJECT_NAME: {project_name or '(unknown)'}\n"
        f"PART_NUMBER: {part_number or '(unknown)'}\n"
        f"LEGACY_ID: {legacy_id}\n"
        f"Output should be {int(target_count)} quick micro-checks."
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

    # best-effort rerank flag (depends on your implementation)
    rerank_used = bool(
        tmp_state.get("rerank_used")
        or tmp_state.get("did_rerank")
        or tmp_state.get("reranked")
        or tmp_state.get("used_reranker")
    )

    # ---- Recent activity (checkins) ----
    related_checkins: List[Dict[str, Any]] = []
    try:
        all_checkins = sheets.list_checkins()
        k_ci_legacy = _key(sheets.map.col("checkin", "legacy_id"))
        want = _key(legacy_id)
        related_checkins = [c for c in (all_checkins or []) if _key((c or {}).get(k_ci_legacy, "")) == want][-10:]
    except Exception:
        related_checkins = []

    recent_activity = _fmt_recent_activity(related_checkins=related_checkins)

    # stage used only for prompt quality
    recent_blob = f"{dispatch_date_str}\n{recent_activity}\n{packed_context}"
    stage = _infer_stage(dispatch_date_str=dispatch_date_str, recent_text_blob=recent_blob)

    vector_risks = _fmt_vector_risks(
        problems=problems,
        resolutions=resolutions,
        ccp=ccp,
        dash=dash,
        packed_context=packed_context,
    )
    process_material = _fmt_process_material(project_name=project_name, part_number=part_number)

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

    # retry once to fill missing
    if len(cues) < target_count:
        missing = target_count - len(cues)
        raw2 = _llm_generate_with_retries(
            llm,
            prompt
            + f"\nReturn VALID JSON ONLY with schema: {{\"cues\": [\"...\"]}} containing EXACTLY {missing} NEW cues (no repeats).",
        )
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
    cues10_json = json.dumps({"cues": cues}, ensure_ascii=False)

    # chips string = what we write in Project tab (top 5 bullets)
    chips_str = _format_project_bullets(cues, max_items=5)

    return cues, rerank_used, tenant_id, status_val, chips_str, cues10_json

# ----------------------------
# Writes
# ----------------------------

def write_project_chips(*, sheets: SheetsTool, legacy_id: str, chips: str) -> bool:
    col_out = sheets.map.col("project", "ai_critcal_point")  # "ZAI Recommendations"
    return bool(sheets.update_project_cell_by_legacy_id(_norm_value(legacy_id), column_name=col_out, value=(chips or "").strip()))

def upsert_appsheet_cues(
    *,
    settings: Any,
    legacy_id: str,
    cues: List[str],
) -> None:
    client = AppSheetClient(settings)
    if not client.enabled_cues():
        return

    generated_at = _iso_date()
    cue_items: List[Dict[str, str]] = []
    for cue in (cues or []):
        cue = (cue or "").strip()
        if not cue:
            continue
        cue_id = _stable_numeric_key(f"{legacy_id}||{cue}", digits=9)
        cue_items.append({"cue_id": cue_id, "cue": cue})

    client.upsert_cues_rows(
        legacy_id=(legacy_id or "").strip(),
        cue_items=cue_items,
        generated_at=generated_at,
    )

def append_cues_log(
    *,
    settings: Any,
    event_type: str,
    run_id: str,
    primary_id: str,
    tenant_id: str,
    legacy_id: str,
    status_assembly: str,
    skipped: bool,
    skip_reason: str,
    rerank_used: bool,
    cues10_json: str,
    chips: str,
) -> None:
    tool = ZaiCuesLogTool(settings)
    if not tool.enabled():
        return

    row = ZaiCuesLogRow(
        timestamp_ist=_now_ist_str(),
        event_type=(event_type or "BACKFILL_CUES").strip(),
        run_id=(run_id or "").strip(),
        primary_id=(primary_id or "").strip(),
        idempotency_primary_id=_payload_hash({"event": event_type, "legacy_id": legacy_id, "primary_id": primary_id}),
        tenant_id=(tenant_id or "").strip(),
        legacy_id=(legacy_id or "").strip(),
        status_assembly=(status_assembly or "").strip(),
        skipped=bool(skipped),
        skip_reason=(skip_reason or "").strip(),
        rerank_used=bool(rerank_used),
        cues10_json=_trim((cues10_json or "").strip(), 4000),
        chips=_trim((chips or "").strip(), 2000),
    )

    # best-effort append
    try:
        tool.append_row(row)
    except Exception:
        # don't break backfill
        pass

# ----------------------------
# Main
# ----------------------------

def main() -> int:
    _load_env_service_dotenv()

    ap = argparse.ArgumentParser(description="Backfill ZAI cues for legacy_ids from a file (secret-free).")
    ap.add_argument(
        "--file",
        default=str(_repo_root() / "service" / "scripts" / "legacy_ids.txt"),
        help="Path to legacy_ids.txt (one per line).",
    )
    ap.add_argument("--count", type=int, default=10, help="Number of cues per legacy_id (default 10).")
    ap.add_argument("--sleep", type=float, default=0.25, help="Sleep between items.")
    ap.add_argument("--limit", type=int, default=0, help="Process only first N IDs.")
    ap.add_argument("--force", action="store_true", help="Regenerate even if Project 'ZAI Recommendations' already has value.")
    ap.add_argument("--allow-non-mfg", action="store_true", help="Allow even if Status_assembly != 'mfg'.")
    ap.add_argument("--dry-run", action="store_true", help="Do not write Project/AppSheet; still logs skip/generate intent.")
    ap.add_argument("--no-log", action="store_true", help="Do not append to ZAI_CUES_LOG (override).")
    ap.add_argument("--event-type", default="BACKFILL_CUES", help="Event Type written into ZAI_CUES_LOG.")
    args = ap.parse_args()

    settings = load_settings()
    sheets = SheetsTool(settings)

    ids = _read_legacy_ids(Path(args.file))
    if args.limit and int(args.limit) > 0:
        ids = ids[: int(args.limit)]

    total = len(ids)
    if total == 0:
        print("[done] no legacy_ids found")
        return 0

    # project keys
    k_status = _key(sheets.map.col("project", "status_assembly"))
    k_chips = _key(sheets.map.col("project", "ai_critcal_point"))
    k_tenant = _key(sheets.map.col("project", "company_row_id"))

    ok_gen = 0
    ok_skip = 0
    ok_err = 0

    for i, legacy_id in enumerate(ids, start=1):
        legacy_id = _norm_value(legacy_id)
        run_id = f"backfill::{_iso_date()}::{i}"
        primary_id = legacy_id

        try:
            pr = sheets.get_project_by_legacy_id(legacy_id)
            if not pr:
                ok_skip += 1
                msg = "project_row_not_found"
                print(f"[{i}/{total}] SKIP {legacy_id} :: {msg}")
                if (not args.no_log) and settings.zai_cues_log_enabled:
                    append_cues_log(
                        settings=settings,
                        event_type=args.event_type,
                        run_id=run_id,
                        primary_id=primary_id,
                        tenant_id="",
                        legacy_id=legacy_id,
                        status_assembly="",
                        skipped=True,
                        skip_reason=msg,
                        rerank_used=False,
                        cues10_json="",
                        chips="",
                    )
                continue

            status_val = _norm_value(pr.get(k_status, ""))
            tenant_id = _norm_value(pr.get(k_tenant, ""))

            if (not args.allow_non_mfg) and (status_val.strip().lower() != "mfg"):
                ok_skip += 1
                msg = f"status_gate_not_mfg:{status_val}"
                print(f"[{i}/{total}] SKIP {legacy_id} :: {msg}")
                if (not args.no_log) and settings.zai_cues_log_enabled:
                    append_cues_log(
                        settings=settings,
                        event_type=args.event_type,
                        run_id=run_id,
                        primary_id=primary_id,
                        tenant_id=tenant_id,
                        legacy_id=legacy_id,
                        status_assembly=status_val,
                        skipped=True,
                        skip_reason=msg,
                        rerank_used=False,
                        cues10_json="",
                        chips=_norm_value(pr.get(k_chips, "")),
                    )
                continue

            existing_chips = _norm_value(pr.get(k_chips, ""))
            if existing_chips and (not args.force):
                ok_skip += 1
                msg = "already_generated_project_column"
                print(f"[{i}/{total}] SKIP {legacy_id} :: {msg}")
                if (not args.no_log) and settings.zai_cues_log_enabled:
                    append_cues_log(
                        settings=settings,
                        event_type=args.event_type,
                        run_id=run_id,
                        primary_id=primary_id,
                        tenant_id=tenant_id,
                        legacy_id=legacy_id,
                        status_assembly=status_val,
                        skipped=True,
                        skip_reason=msg,
                        rerank_used=False,
                        cues10_json="",
                        chips=existing_chips,
                    )
                continue

            cues, rerank_used, tenant_id2, status_val2, chips_str, cues10_json = generate_cues_for_legacy_id(
                settings=settings,
                sheets=sheets,
                legacy_id=legacy_id,
                target_count=max(1, int(args.count)),
            )

            # Print cues (compact)
            print(f"[{i}/{total}] GEN  {legacy_id} :: cues={len(cues)} rerank={rerank_used}")
            for c in cues:
                print(f"  - {c}")

            if not args.dry_run:
                # 1) write chips (top 5 bullets) to Project
                write_project_chips(sheets=sheets, legacy_id=legacy_id, chips=chips_str)

                # 2) upsert 10 cues into AppSheet cues table
                upsert_appsheet_cues(settings=settings, legacy_id=legacy_id, cues=cues)

            # 3) log
            if (not args.no_log) and settings.zai_cues_log_enabled:
                append_cues_log(
                    settings=settings,
                    event_type=args.event_type,
                    run_id=run_id,
                    primary_id=primary_id,
                    tenant_id=tenant_id2,
                    legacy_id=legacy_id,
                    status_assembly=status_val2,
                    skipped=False,
                    skip_reason="",
                    rerank_used=rerank_used,
                    cues10_json=cues10_json,
                    chips=chips_str,
                )

            ok_gen += 1

        except Exception as e:
            ok_err += 1
            msg = str(e)
            print(f"[{i}/{total}] ERROR {legacy_id} :: {msg}")

            # best-effort log error as skipped row (keeps sheet consistent)
            if (not args.no_log) and settings.zai_cues_log_enabled:
                try:
                    append_cues_log(
                        settings=settings,
                        event_type=args.event_type,
                        run_id=run_id,
                        primary_id=primary_id,
                        tenant_id="",
                        legacy_id=legacy_id,
                        status_assembly="",
                        skipped=True,
                        skip_reason=f"error:{_trim(msg, 180)}",
                        rerank_used=False,
                        cues10_json="",
                        chips="",
                    )
                except Exception:
                    pass

        if args.sleep and float(args.sleep) > 0:
            time.sleep(float(args.sleep))

    print(f"[done] generated={ok_gen} skipped={ok_skip} errors={ok_err} total={total} dry_run={bool(args.dry_run)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
