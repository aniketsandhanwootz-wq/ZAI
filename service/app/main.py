from __future__ import annotations

import uuid
from pathlib import Path
from typing import Dict, Any, Literal
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, Request

from .config import load_settings, Settings
from .consumer import start_consumer_thread, stop_consumer
from .pipeline.graph import run_event_graph
from .pipeline.ingest.ccp_ingest import ingest_ccp
from .pipeline.ingest.history_ingest import ingest_history
from .pipeline.ingest.dashboard_ingest import ingest_dashboard
from .pipeline.ingest.migrate import run_migrations
from .logctx import setup_logging, request_id_var
from .schemas.webhook import WebhookPayload
from .routers import appsheet_webhook_router, teams_test_router
from .pipeline.ingest.glide_ingest_project import ingest_glide_project
from .pipeline.ingest.glide_ingest_raw_material import ingest_glide_raw_material
from .pipeline.ingest.glide_ingest_processes import ingest_glide_processes
from .pipeline.ingest.glide_ingest_boughtouts import ingest_glide_boughtouts
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

    # graceful shutdown: stop embedded rq worker process
    try:
        stop_consumer()
    except Exception:
        pass

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


@app.post("/admin/trigger")
def admin_trigger(request: Request, payload: WebhookPayload):
    settings = _get_settings(request)
    result = run_event_graph(settings, payload.model_dump(exclude_none=True))
    return {"ok": True, "result": result}


@app.post("/admin/ingest")
def admin_ingest(
    request: Request,
    source: Literal[
        "projects", "ccp", "history", "dashboard", "media",
        "glide_project", "glide_raw_material", "glide_processes", "glide_boughtouts", "glide_all",
        "all"
    ] = "all",
    limit: int = 500,
):
    """
    Bulk backfill:
      - history: PROBLEM/RESOLUTION vectors
      - dashboard: dashboard_vectors
      - ccp: ccp_vectors
      - media: IMAGE_CAPTION artifacts + MEDIA vectors (ingest-only pipeline)
    """
    settings = _get_settings(request)
    results: Dict[str, Any] = {}

    if source in ("ccp", "all"):
        results["ccp"] = ingest_ccp(settings)

    if source in ("history", "all"):
        results["history"] = ingest_history(settings, limit=limit)

    if source in ("dashboard", "all"):
        results["dashboard"] = ingest_dashboard(settings, limit=max(1, int(limit)))

    if source in ("media", "all"):
        from .tools.sheets_tool import SheetsTool, _key, _norm_value

        sheets = SheetsTool(settings)

        # ---- CheckIN mapping keys ----
        k_ci_checkin_id = _key(sheets.map.col("checkin", "checkin_id"))
        k_ci_legacy_id = _key(sheets.map.col("checkin", "legacy_id"))
        k_ci_project = _key(sheets.map.col("checkin", "project_name"))
        k_ci_part = _key(sheets.map.col("checkin", "part_number"))

        # ---- Project mapping keys ----
        k_p_legacy = _key(sheets.map.col("project", "legacy_id"))
        k_p_tenant = _key(sheets.map.col("project", "company_row_id"))
        k_p_name = _key(sheets.map.col("project", "project_name"))
        k_p_part = _key(sheets.map.col("project", "part_number"))

        # Build project indexes ONCE (ID-first + fallback triplet)
        projects = sheets.list_projects()

        project_by_id: Dict[str, Dict[str, str]] = {}
        project_by_triplet: Dict[tuple[str, str, str], Dict[str, str]] = {}

        for pr in projects:
            pid = _norm_value((pr or {}).get(k_p_legacy, ""))
            tenant_id = _norm_value((pr or {}).get(k_p_tenant, ""))
            pname = _norm_value((pr or {}).get(k_p_name, ""))
            pnum = _norm_value((pr or {}).get(k_p_part, ""))

            if pid:
                project_by_id[_key(pid)] = {
                    "tenant_id": tenant_id,
                    "project_name": pname,
                    "part_number": pnum,
                    "legacy_id": pid,
                }

            if pid and pname and pnum:
                project_by_triplet[(_key(pname), _key(pnum), _key(pid))] = {
                    "tenant_id": tenant_id,
                    "project_name": pname,
                    "part_number": pnum,
                    "legacy_id": pid,
                }

        rows = sheets.list_checkins()
        if limit and limit > 0:
            rows = rows[:limit]

        seen = 0
        ok = 0
        err = 0
        skipped_missing_checkin_id = 0
        skipped_missing_legacy_id = 0
        skipped_missing_tenant = 0

        err_samples: list[dict[str, str]] = []
        missing_tenant_samples: list[dict[str, str]] = []

        for r in rows:
            seen += 1

            checkin_id = _norm_value((r or {}).get(k_ci_checkin_id, ""))
            if not checkin_id:
                skipped_missing_checkin_id += 1
                continue

            legacy_id = _norm_value((r or {}).get(k_ci_legacy_id, ""))
            project_name = _norm_value((r or {}).get(k_ci_project, ""))
            part_number = _norm_value((r or {}).get(k_ci_part, ""))

            if not legacy_id:
                skipped_missing_legacy_id += 1
                continue

            # Resolve tenant (ID-first, same spirit as history_ingest)
            proj = project_by_id.get(_key(legacy_id))
            if not proj and project_name and part_number:
                proj = project_by_triplet.get((_key(project_name), _key(part_number), _key(legacy_id)))

            tenant_id = _norm_value((proj or {}).get("tenant_id", ""))
            if not tenant_id:
                skipped_missing_tenant += 1
                if len(missing_tenant_samples) < 25:
                    missing_tenant_samples.append(
                        {
                            "checkin_id": checkin_id,
                            "legacy_id": legacy_id,
                            "project_name": project_name,
                            "part_number": part_number,
                        }
                    )
                continue

            out = run_event_graph(
                settings,
                {
                    "event_type": "CHECKIN_UPDATED",
                    "checkin_id": checkin_id,
                    "legacy_id": legacy_id,
                    "meta": {
                        "ingest_only": True,
                        "media_only": True,
                        "tenant_id": tenant_id,  # ✅ key fix
                    },
                },
            )

            if out.get("ok"):
                ok += 1
            else:
                err += 1
                if len(err_samples) < 20:
                    err_samples.append(
                        {
                            "checkin_id": checkin_id,
                            "legacy_id": legacy_id,
                            "error": str(out.get("error") or "")[:300],
                        }
                    )

        results["media"] = {
            "source": "media",
            "rows_seen": seen,
            "runs_ok": ok,
            "runs_error": err,
            "skipped_missing_checkin_id": skipped_missing_checkin_id,
            "skipped_missing_legacy_id": skipped_missing_legacy_id,
            "skipped_missing_tenant": skipped_missing_tenant,
            "missing_tenant_samples": missing_tenant_samples,
            "error_samples": err_samples,
            "note": "Media backfill runs CHECKIN_UPDATED with meta.tenant_id + ingest_only/media_only for stable tenant resolution.",
        }
    if source == "projects":
        results["projects"] = {"note": "No separate projects table in MVP; we look up Project row on-demand."}

    # -----------------------
    # Glide backfill (Phase 2)
    # -----------------------
    if source in ("glide_project", "glide_all", "all"):
        results["glide_project"] = ingest_glide_project(settings, limit=max(0, int(limit)))

    if source in ("glide_raw_material", "glide_all", "all"):
        results["glide_raw_material"] = ingest_glide_raw_material(settings, limit=max(0, int(limit)))

    if source in ("glide_processes", "glide_all", "all"):
        results["glide_processes"] = ingest_glide_processes(settings, limit=max(0, int(limit)))

    if source in ("glide_boughtouts", "glide_all", "all"):
        results["glide_boughtouts"] = ingest_glide_boughtouts(settings, limit=max(0, int(limit)))
    return {"ok": True, "results": results}
