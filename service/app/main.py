from __future__ import annotations

import uuid
from pathlib import Path
from typing import Optional, Literal, Dict, Any
from contextlib import asynccontextmanager
from .schemas.webhook import WebhookPayload
from .routers import appsheet_webhook_router, teams_test_router
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from .config import load_settings, Settings
from .consumer import start_consumer_thread
from .queue import enqueue_job
from .pipeline.graph import run_event_graph
from .pipeline.ingest.ccp_ingest import ingest_ccp
from .pipeline.ingest.history_ingest import ingest_history
from .pipeline.ingest.migrate import run_migrations
from .logctx import setup_logging, request_id_var

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


class WebhookPayload(BaseModel):
    event_type: Literal[
        "CHECKIN_CREATED",
        "CHECKIN_UPDATED",
        "CONVERSATION_ADDED",
        "CCP_UPDATED",
        "DASHBOARD_UPDATED",
        "MANUAL_TRIGGER",
    ]
    checkin_id: Optional[str] = None
    conversation_id: Optional[str] = None
    ccp_id: Optional[str] = None
    legacy_id: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None


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


@app.post("/webhook/appsheet")
def appsheet_webhook_legacy(
    request: Request,
    payload: WebhookPayload,
    x_appsheet_secret: Optional[str] = Header(default=None),
):
    # forward to new router logic by reusing same enqueue/secret check
    settings = _get_settings(request)
    if x_appsheet_secret != settings.appsheet_webhook_secret:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    job_id = enqueue_job(settings, payload.model_dump())
    return {"ok": True, "job_id": job_id, "note": "legacy path; prefer /webhooks/appsheet"}

@app.post("/admin/trigger")
def admin_trigger(request: Request, payload: WebhookPayload):
    settings = _get_settings(request)
    result = run_event_graph(settings, payload.model_dump(exclude_none=True))
    return {"ok": True, "result": result}


@app.post("/admin/ingest")
def admin_ingest(
    request: Request,
    source: Literal["projects", "ccp", "history", "all"] = "all",
    limit: int = 500,
):
    settings = _get_settings(request)
    results: Dict[str, Any] = {}

    if source in ("ccp", "all"):
        results["ccp"] = ingest_ccp(settings)

    if source in ("history", "all"):
        results["history"] = ingest_history(settings, limit=limit)

    if source == "projects":
        results["projects"] = {"note": "No separate projects table in MVP; we look up Project row on-demand."}

    return {"ok": True, "results": results}
