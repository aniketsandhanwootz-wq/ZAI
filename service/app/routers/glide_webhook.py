# service/app/routers/glide_webhook.py
from __future__ import annotations

import hmac
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request, HTTPException

from ..config import Settings
from ..queue import enqueue_glide_job

logger = logging.getLogger("zai.glide_webhook")

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


def _get_settings(request: Request) -> Settings:
    return request.app.state.settings  # type: ignore


def _consteq(a: str, b: str) -> bool:
    return hmac.compare_digest((a or "").strip(), (b or "").strip())


def _require_secret(request: Request, settings: Settings) -> None:
    """
    Accept secret via:
      - Header: x-webhook-secret
      - Header: authorization: Bearer <secret>
      - Query: ?secret=<secret>
    Reuse WEBHOOK_SECRET for now (same as Apps Script), so you don't add new env.
    """
    expected = (settings.webhook_secret or "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="Server missing WEBHOOK_SECRET")

    got = (request.headers.get("x-webhook-secret") or "").strip()

    if not got:
        auth = (request.headers.get("authorization") or "").strip()
        if auth.lower().startswith("bearer "):
            got = auth.split(" ", 1)[1].strip()

    if not got:
        got = (request.query_params.get("secret") or "").strip()

    if not got or not _consteq(got, expected):
        raise HTTPException(status_code=401, detail="Unauthorized (bad secret)")


def _as_list(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        out = []
        for v in x:
            s = str(v or "").strip()
            if s:
                out.append(s)
        return out
    s = str(x or "").strip()
    return [s] if s else []


def _pick_table_key(body: Dict[str, Any]) -> str:
    # keep aliases, Glide payload can vary
    for k in ("table_key", "table", "entity", "kb_table", "type"):
        v = str(body.get(k) or "").strip()
        if v:
            return v
    return ""


def _pick_row_ids(body: Dict[str, Any]) -> List[str]:
    # allow single or bulk
    for k in ("row_ids", "rowIds", "row_id", "rowId", "id", "$rowID"):
        if k in body:
            ids = _as_list(body.get(k))
            if ids:
                return ids
    return []


def _normalize_table_key(k: str) -> str:
    k = (k or "").strip().lower()
    k = k.replace(" ", "_").replace("-", "_")
    # common aliases
    alias = {
        "rawmaterial": "raw_material",
        "raw_materials": "raw_material",
        "process": "processes",
        "proc": "processes",
        "boughtout": "boughtouts",
        "bought_out": "boughtouts",
        "bo": "boughtouts",
        "project": "project",
        "projects": "project",
        "company": "company",
        "companies": "company",
        "company_profile": "company",
        "company_profiles": "company",
    }
    return alias.get(k, k)


@router.post("/glide")
async def glide_webhook(request: Request) -> Dict[str, Any]:
    settings = _get_settings(request)
    _require_secret(request, settings)

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object")

    table_key = _normalize_table_key(_pick_table_key(body))
    row_ids = _pick_row_ids(body)

    if not table_key:
        raise HTTPException(status_code=400, detail="Missing table/table_key in payload")
    if not row_ids:
        raise HTTPException(status_code=400, detail="Missing row_id/row_ids in payload")

    # optional metadata passthrough
    meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
    event = str(body.get("event") or body.get("action") or body.get("trigger") or "").strip() or "updated"

    payload = {
        "source": "glide_webhook",
        "table_key": table_key,
        "row_ids": row_ids,
        "event": event,
        "meta": meta,
    }

    job_id = enqueue_glide_job(settings, payload)

    logger.info("enqueued glide ingest job=%s table=%s rows=%s event=%s", job_id, table_key, len(row_ids), event)
    return {"ok": True, "enqueued": True, "job_id": job_id, "table_key": table_key, "row_ids": row_ids, "event": event}