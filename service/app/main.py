from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from typing import Optional, Literal, Dict, Any

from .config import load_settings
from .consumer import start_consumer_thread
from .queue import enqueue_job
from .pipeline.graph import run_event_graph

from .pipeline.ingest.ccp_ingest import ingest_ccp
from .pipeline.ingest.history_ingest import ingest_history

app = FastAPI(title="Wootz Checkin AI (MVP)")


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
    legacy_id: Optional[str] = None  # your common "ID" field
    meta: Optional[Dict[str, Any]] = None


@app.on_event("startup")
def on_startup() -> None:
    settings = load_settings()
    # Start consumer loop in SAME service (MVP) if enabled
    if settings.run_consumer:
        start_consumer_thread(settings)
    # (Optional) Run DB migrations here later (we’ll add in next folder)


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.post("/webhook/appsheet")
def appsheet_webhook(
    payload: WebhookPayload,
    x_appsheet_secret: Optional[str] = Header(default=None),
):
    settings = load_settings()
    if x_appsheet_secret != settings.appsheet_webhook_secret:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    # Enqueue a job (Redis)
    job_id = enqueue_job(settings, payload.model_dump())
    return {"ok": True, "job_id": job_id}


# ---- Admin endpoints (staging/testing) ----

@app.post("/admin/trigger")
def admin_trigger(payload: WebhookPayload):
    """
    Manual trigger without AppSheet.
    Useful for testing before bots are created.
    """
    settings = load_settings()
    result = run_event_graph(settings, payload.model_dump())
    return {"ok": True, "result": result}


@app.post("/admin/ingest")
def admin_ingest(source: Literal["projects", "ccp", "history", "all"] = "all", limit: int = 500):
    """
    One-time ingestion for staging.
    - ccp: ingests CCP desc (+ direct PDF URLs if available)
    - history: ingests existing checkins + conversations into incident_vectors
    """
    settings = load_settings()
    results = {}

    if source in ("ccp", "all"):
        results["ccp"] = ingest_ccp(settings)

    if source in ("history", "all"):
        results["history"] = ingest_history(settings, limit=limit)

    if source == "projects":
        results["projects"] = {"note": "No separate projects table in MVP; we look up Project row on-demand."}

    return {"ok": True, "results": results}
