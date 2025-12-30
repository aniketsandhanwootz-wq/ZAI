# service/app/main.py
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Optional, Literal, Dict, Any
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request

from .config import load_settings, Settings
from .consumer import start_consumer_thread
from .queue import enqueue_job
from .pipeline.graph import run_event_graph
from .pipeline.ingest.ccp_ingest import ingest_ccp
from .pipeline.ingest.history_ingest import ingest_history
from .pipeline.ingest.dashboard_ingest import ingest_dashboard  # ✅ WIRED
from .pipeline.ingest.migrate import run_migrations
from .logctx import setup_logging, request_id_var
from .schemas.webhook import WebhookPayload
from .routers import appsheet_webhook_router, teams_test_router

# Load .env from service/.env (override shell env so local tests match)
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=True)

setup_logging()
import logging

logger = logging.getLogger("zai")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    app.state.settings = settings
    logger.info(
        "startup: loaded settings. llm_provider=%s llm_model=%s run_consumer=%s run_migrations=%s",
        settings.llm_provider,
        settings.llm_model,
        settings.run_consumer,
        settings.run_migrations,
    )

    if settings.run_migrations:
        run_migrations(settings)

    if settings.run_consumer:
        start_consumer_thread(settings)

    yield


app = FastAPI(title="Wootz Checkin AI (MVP)", lifespan=lifespan)

# Routers
app.include_router(appsheet_webhook_router)
app.include_router(teams_test_router)


@app.middleware("http")
async def add_request_id(request: Request, call_next):
    rid = request.headers.get("x-request-id") or str(uuid.uuid4())
    request.state.request_id = rid

    token = request_id_var.set(rid)
    try:
        resp = await call_next(request)
    finally:
        request_id_var.reset(token)

    resp.headers["x-request-id"] = rid
    return resp


def _get_settings(request: Request) -> Settings:
    return request.app.state.settings  # type: ignore


@app.get("/health")
def health(request: Request) -> dict:
    s = _get_settings(request)
    return {
        "ok": True,
        "llm_provider": s.llm_provider,
        "llm_model": s.llm_model,
        "run_consumer": s.run_consumer,
        "run_migrations": s.run_migrations,
    }


# Legacy endpoint kept (optional). Prefer /webhooks/sheets or /webhooks/appsheet.
@app.post("/webhook/appsheet")
def appsheet_webhook_legacy(
    request: Request,
    payload: WebhookPayload,
    x_appsheet_secret: Optional[str] = Header(default=None),
):
    settings = _get_settings(request)
    if x_appsheet_secret != settings.appsheet_webhook_secret:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    job_id = enqueue_job(settings, payload.model_dump())
    return {"ok": True, "job_id": job_id, "note": "legacy path; prefer /webhooks/sheets"}


@app.post("/admin/trigger")
def admin_trigger(request: Request, payload: WebhookPayload):
    settings = _get_settings(request)
    result = run_event_graph(settings, payload.model_dump(exclude_none=True))
    return {"ok": True, "result": result}


@app.post("/admin/ingest")
def admin_ingest(
    request: Request,
    source: Literal["projects", "ccp", "history", "dashboard", "media", "all"] = "all",
    limit: int = 500,
):
    """
    Bulk backfill:
      - history: PROBLEM/RESOLUTION vectors (fast)
      - dashboard: dashboard_vectors
      - ccp: ccp_vectors
      - media: IMAGE_CAPTION artifacts + MEDIA vectors (via ingest-only pipeline; no AI reply/writeback)
    """
    settings = _get_settings(request)
    results: Dict[str, Any] = {}

    if source in ("ccp", "all"):
        results["ccp"] = ingest_ccp(settings)

    if source in ("history", "all"):
        results["history"] = ingest_history(settings, limit=limit)

    if source in ("dashboard", "all"):
        # reuse same limit (dashboard_ingest supports its own defaults too)
        results["dashboard"] = ingest_dashboard(settings, limit=max(1, int(limit)))

    if source in ("media", "all"):
        # ✅ bulk captioning + MEDIA vectors (no reply/writeback)
        from .tools.sheets_tool import SheetsTool, _key, _norm_value  # local import to keep startup light

        sheets = SheetsTool(settings)
        col_checkin_id = sheets.map.col("checkin", "checkin_id")
        k_checkin_id = _key(col_checkin_id)

        rows = sheets.list_checkins()
        if limit and limit > 0:
            rows = rows[:limit]

        seen = 0
        ok = 0
        err = 0
        err_samples: list[dict[str, str]] = []

        for r in rows:
            seen += 1
            checkin_id = _norm_value((r or {}).get(k_checkin_id, ""))
            if not checkin_id:
                continue

            out = run_event_graph(
                settings,
                {
                    "event_type": "CHECKIN_CREATED",
                    "checkin_id": checkin_id,
                    "meta": {"ingest_only": True, "media_only": True},
                },
            )
            if out.get("ok"):
                ok += 1
            else:
                err += 1
                if len(err_samples) < 20:
                    err_samples.append(
                        {"checkin_id": checkin_id, "error": str(out.get("error") or "")[:300]}
                    )

        results["media"] = {
            "source": "media",
            "rows_seen": seen,
            "runs_ok": ok,
            "runs_error": err,
            "error_samples": err_samples,
            "note": "Uses ingest-only pipeline: load_sheet_data -> build_thread_snapshot -> analyze_media -> upsert MEDIA vector.",
        }

    if source == "projects":
        results["projects"] = {"note": "No separate projects table in MVP; we look up Project row on-demand."}

    return {"ok": True, "results": results}
