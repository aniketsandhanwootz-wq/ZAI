# ZAI (Wootz.Work) - Manufacturing Intelligence Service

ZAI is a FastAPI + RQ service that ingests manufacturing data from Google Sheets, Glide, and Drive, builds retrieval memory in Postgres/pgvector, generates AI outputs, and writes actions back to AppSheet/Teams/n8n/email flows.

This README is the source of truth for:
- Runtime architecture and processing flow
- Webhook contracts and operational payloads
- Environment variable setup by feature
- Local run and deployment runbooks
- Troubleshooting and security practices

## Table Of Contents
1. [System Overview](#system-overview)
2. [Repository Structure](#repository-structure)
3. [Runtime Architecture](#runtime-architecture)
4. [Event Model And Pipeline Behavior](#event-model-and-pipeline-behavior)
5. [External Contracts](#external-contracts)
6. [Storage Model](#storage-model)
7. [Configuration](#configuration)
8. [Local Development](#local-development)
9. [Operational Scripts](#operational-scripts)
10. [Deployment](#deployment)
11. [Observability](#observability)
12. [Troubleshooting](#troubleshooting)
13. [Security Practices](#security-practices)
14. [Known Limitations](#known-limitations)
15. [Maintenance Checklist](#maintenance-checklist)

## System Overview
ZAI does four core jobs:

1. Ingest data
- Sheets tabs (CheckIN, Project, Conversation, CCP, Dashboard Updates, Users database, Suppliers capmap)
- Glide tables (company, raw_material, processes, boughtouts, optionally project)
- Drive/File attachments referenced in check-ins

2. Build retrieval memory
- Writes vectorized memory to Postgres `pgvector` tables
- Persists artifacts and file analysis metadata for idempotency and audit

3. Generate operational outputs
- Check-in AI reply + criticality signal
- Assembly checklist/cues flows
- CXO daily manufacturing email report

4. Write back actions
- AppSheet Conversation row writes
- Teams webhook posts
- n8n webhook trigger for critical check-ins

## Repository Structure
High-value paths:

- `service/app/main.py`
  FastAPI app, lifecycle, health, admin endpoints, router mounting.

- `service/app/pipeline/graph.py`
  Core orchestrator for webhook events.

- `service/app/pipeline/nodes/`
  Modular pipeline nodes: load data, analyze media/files, retrieve/rerank context, generate reply, writeback.

- `service/app/tools/`
  DB, vectors, Sheets, Drive, LLM, embedding, CXO report tooling.

- `service/app/integrations/`
  AppSheet, Glide, Teams, SMTP integration clients.

- `service/scripts/`
  Operational scripts (reconcile, backfills, CXO email run).

- `packages/contracts/`
  Mapping contracts (`sheets_mapping.yaml`, `zai_cues_log_mapping.yaml`).

- `packages/prompts/`
  Prompt templates (`checkin_reply.md`, `zai_cues_10.md`, `cxo_report.md`, etc.).

- `packages/db/migrations/`
  SQL schema and indexes.

## Runtime Architecture
At runtime, the service has these moving parts:

- API Process
  - FastAPI receives webhooks/admin calls.
  - Enqueues jobs to Redis via RQ.

- Embedded Worker Process
  - Spawned by app lifecycle when `RUN_CONSUMER=1`.
  - Runs `rq worker` in a separate process monitored by `service/app/consumer.py`.

- Postgres
  - Stores vectors, run logs, artifacts, and company/cache data.

- Redis
  - Queue transport for background processing.

- External Integrations
  - Google Sheets API
  - Google Drive API (OAuth token based)
  - Glide Tables API
  - AppSheet API
  - Teams webhook / Power Automate webhook
  - n8n webhook
  - SMTP (CXO report)

## Event Model And Pipeline Behavior
Event payload schema is defined in `service/app/schemas/webhook.py`.
Supported event types:
- `CHECKIN_CREATED`
- `CHECKIN_UPDATED`
- `CONVERSATION_ADDED`
- `CCP_UPDATED`
- `DASHBOARD_UPDATED`
- `PROJECT_UPDATED`
- `MANUAL_TRIGGER`

### Main behavior by event

1. `CHECKIN_CREATED`
- Runs full pipeline including AI reply generation and writeback.
- May post to Teams.
- May call n8n if critical.

2. `CHECKIN_UPDATED` and `CONVERSATION_ADDED`
- Ingest-focused by default.
- No human-facing reply writeback unless explicitly forced in meta logic.

3. `CCP_UPDATED`
- Incremental CCP ingest path.
- Also refreshes assembly todo generation.

4. `DASHBOARD_UPDATED`
- Ingest by row id if available, else legacy-id fallback.
- Also refreshes assembly todo generation.

5. `PROJECT_UPDATED`
- Primarily assembly todo refresh path.

### High-level node order for `CHECKIN_CREATED`
1. `load_sheet_data`
2. `generate_assembly_todo`
3. `build_thread_snapshot`
4. `analyze_media`
5. `analyze_attachments`
6. `retrieve_context`
7. `rerank_context`
8. `generate_ai_reply`
9. `annotate_media`
10. `upsert_vectors`
11. `writeback`

## External Contracts

### Sheets webhook endpoint
- `POST /webhooks/sheets`
- Header required: `x-sheets-secret: <WEBHOOK_SECRET>`
- Body: `WebhookPayload` (see schema above)

Minimal example:

```json
{
  "event_type": "CHECKIN_CREATED",
  "checkin_id": "CHK_12345",
  "legacy_id": "PRJ_001"
}
```

### Glide webhook endpoint
- `POST /webhooks/glide`
- Secret accepted as one of:
  - `x-webhook-secret` header
  - `Authorization: Bearer <secret>`
  - `?secret=<secret>` query
- Uses same `WEBHOOK_SECRET`

Minimal example:

```json
{
  "table_key": "raw_material",
  "row_ids": ["row_a", "row_b"],
  "event": "updated",
  "meta": {}
}
```

### n8n webhook payload (critical checkin path)
This is sent from `writeback.py` only when:
- event is `CHECKIN_CREATED`
- `is_critical == true`
- `N8N_WHATSAPP_WEBHOOK_URL` is configured

Current payload shape:

```json
{
  "type": "checkin_created",
  "tenant_id": "string",
  "checkin_id": "string",
  "checkin_url": "string",
  "project_name": "string",
  "company_name": "string",
  "ai_reply": "string",
  "part_number": "string",
  "status": "string",
  "checkin_text": "string",
  "created_by": "string",
  "created_by_phone": "string",
  "internal_poc_phones": ["string", "string"],
  "annotated_images": ["url1", "url2"],
  "checkin_images": ["url1", "url2"],
  "item_id": "string"
}
```

Note:
- `internal_poc_phones` is a list resolved from `Project.Internal POC` emails using `Users database` contact lookup.

### Admin endpoints
- `GET /health`
- `POST /admin/trigger`
- `POST /admin/migrate`
- `POST /admin/ingest`

These are operational endpoints and should be protected at network/auth layer in production.

## Storage Model
Primary DB entities include:

- Run tracking
  - `ai_runs`

- Retrieval memory
  - `incident_vectors`
  - `ccp_vectors`
  - `dashboard_vectors`
  - `glide_kb_items`
  - `glide_kb_vectors`
  - `company_vectors`

- Artifacts and attachment analysis
  - `artifacts`
  - `checkin_file_artifacts`

- Company profile cache
  - `company_profiles`

Migrations are in `packages/db/migrations/`.

Startup migrator currently executes `001` to `010` from `service/app/pipeline/ingest/migrate.py`.
If you need migration `011`, apply it manually or extend `migrate.py` list.

## Configuration
All config is env-driven through `service/app/config.py`.

### Boot-critical variables
Service startup requires these:
- `DATABASE_URL`
- `REDIS_URL`
- `GOOGLE_SHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON` or `GOOGLE_SERVICE_ACCOUNT_FILE`
- `WEBHOOK_SECRET` (or fallback `APPSHEET_WEBHOOK_SECRET`)

### Core AI variables
- `LLM_PROVIDER`
- `LLM_API_KEY`
- `LLM_MODEL`
- `LLM_FALLBACK_MODELS` (optional CSV)

- `EMBEDDING_PROVIDER`
- `EMBEDDING_API_KEY`
- `EMBEDDING_MODEL`
- `EMBEDDING_DIMS`

### Runtime toggles
- `RUN_CONSUMER` (`1/0`)
- `CONSUMER_QUEUES` (default `default`)
- `RUN_MIGRATIONS` (`1/0`)

### Sheets and mapping
- `SHEETS_MAPPING_PATH` (default `packages/contracts/sheets_mapping.yaml`)
- `GOOGLE_SHEET_ADDITIONAL_PHOTOS_ID` (optional)
- `ADDITIONAL_PHOTOS_TAB_NAME` (optional)

### Drive and vision
- `GOOGLE_DRIVE_ROOT_FOLDER_ID`
- `GOOGLE_DRIVE_ANNOTATED_FOLDER_ID`
- `DRIVE_PREFIX_MAP_JSON`
- `DRIVE_TOKEN_JSON` (OAuth token JSON string or path)

- `VISION_PROVIDER`
- `VISION_API_KEY`
- `VISION_MODEL`

### AppSheet
- `APPSHEET_BASE_URL`
- `APPSHEET_APP_ID`
- `APPSHEET_ACCESS_KEY`
- `APPSHEET_CUES_TABLE`
- `APPSHEET_CONVERSATION_TABLE`
- `APPSHEET_CONVERSATION_KEY_COL`
- `APPSHEET_CONVERSATION_CRITICAL_COL`
- `APPSHEET_CUES_COL_*` overrides (optional)

### Glide
Either set individual vars:
- `GLIDE_API_KEY`
- `GLIDE_APP_ID`
- `GLIDE_BASE_URL`
- `GLIDE_COMPANY_TABLE`
- `GLIDE_RAW_MATERIAL_TABLE`
- `GLIDE_PROCESSES_TABLE`
- `GLIDE_BOUGHTOUTS_TABLE`
- `GLIDE_PROJECT_TABLE` (optional)
- related `GLIDE_*_COLUMN` overrides

Or set:
- `GLIDE_CONFIG_JSON` (object with tables/columns)

### CXO report (email)
- `CXO_REPORT_ENABLED`
- `CXO_REPORT_TO_EMAIL`
- `CXO_REPORT_FROM_EMAIL`
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASSWORD`
- `SMTP_USE_STARTTLS`
- `CXO_REPORT_DAYS`
- `CXO_REPORT_BATCH_SIZE`
- `CXO_REPORT_MAX_PAYLOAD_BYTES`
- `CXO_REPORT_FAIL_OPEN`

### Tracing (optional)
- `LANGSMITH_TRACING` or `LANGCHAIN_TRACING_V2`
- `LANGSMITH_API_KEY` or `LANGCHAIN_API_KEY`
- `LANGSMITH_PROJECT`/`LANGCHAIN_PROJECT`

## Local Development

### Prerequisites
- Python 3.11 recommended (matches Docker images)
- Postgres with `pgvector`
- Redis

### Setup

```bash
cd /Users/aniketsandhan/Desktop/ZAI
python3 -m venv service/.venv
source service/.venv/bin/activate
pip install -r service/requirements.txt
```

Create and populate `service/.env` with required variables.

### Run API (from `service/`)

```bash
cd service
source .venv/bin/activate
uvicorn app.main:app --reload --port 8000
```

Notes:
- `app.main` auto-loads `service/.env` at startup.
- Worker is auto-spawned when `RUN_CONSUMER=1`.

### Run API (from repo root)

```bash
source service/.venv/bin/activate
uvicorn service.app.main:app --reload --port 8000
```

### Health check

```bash
curl http://localhost:8000/health
```

## Operational Scripts

### 1) Glide reconcile
Purpose: sync Glide tables into DB/vector memory.

```bash
cd /Users/aniketsandhan/Desktop/ZAI
source service/.venv/bin/activate
python -m service.scripts.glide_reconcile --tables company,raw_material,processes,boughtouts
```

Useful flags:
- `--limit N`
- `--dry-run`

### 2) Backfill CHECKIN_CREATED
Purpose: replay checkin-created flow for IDs from a file.

```bash
cd /Users/aniketsandhan/Desktop/ZAI
source service/.venv/bin/activate
python -m service.scripts.backfill_checkin_created --file missed_ids.txt --mode inline
```

Useful flags:
- `--dry-run`
- `--sleep`
- `--limit`

### 3) Backfill ZAI cues from file

```bash
cd /Users/aniketsandhan/Desktop/ZAI
source service/.venv/bin/activate
python -m service.scripts.backfill_zai_cues_from_file --file service/scripts/legacy_ids.txt
```

Useful flags:
- `--dry-run`
- `--force`
- `--allow-non-mfg`
- `--limit`

### 4) Send CXO report manually
Run from `service/` because module path is `app.*`:

```bash
cd /Users/aniketsandhan/Desktop/ZAI/service
source .venv/bin/activate
set -a
source .env
set +a
python -m app.scripts.send_cxo_daily_report
```

Behavior summary:
- Reads assemblies from Sheets (Project tab, mfg-only)
- Fetches checkins/updates from DB
- Builds rows, enriches major/quality via LLM in batches
- Sends HTML table email via SMTP
- Does not write back to Sheets in this flow

## Deployment

### Web service image
`service/Dockerfile`:
- Installs `service/requirements.txt`
- Copies `service/app`, `service/scripts`, `packages`
- Runs `uvicorn app.main:app`

### CXO cron image
`service/Dockerfile.cxo_cron`:
- Same base install
- Runs one-shot:
  - `python -m app.scripts.send_cxo_daily_report`

### Suggested deployment split
- Web service: API + embedded worker (`RUN_CONSUMER=1`)
- Cron service: CXO report schedule

## Observability

### Logging
- Contextual logs include request id and run id (`service/app/logctx.py`).
- Run lifecycle tracked in `ai_runs` (`RUNNING/SUCCESS/ERROR`).

### Run IDs and idempotency
- `graph.py` computes per-event primary ids.
- Idempotency supports scoped replay modes.

### Health
- `/health` returns provider/model/runtime flags.

## Troubleshooting

### `ModuleNotFoundError: No module named 'app'`
Cause: script run from wrong working directory for `app.*` imports.
Fix:
- `cd service`
- run `python -m app.scripts.<script_name>`

### Drive token error `invalid_grant: Token has been expired or revoked`
Cause: `DRIVE_TOKEN_JSON` refresh token invalid/revoked.
Fix:
- Regenerate OAuth token JSON.
- Update `DRIVE_TOKEN_JSON` value.

### Queue unavailable (503 on webhook)
Cause: Redis unavailable or wrong URL.
Fix:
- Verify `REDIS_URL`.
- Verify Redis network access from service.

### Glide rate limits / reconcile failures
Fixes:
- Reduce reconcile frequency.
- Use `--limit` for scoped recovery.
- Retry with stable network and credentials.

### CXO report not sent
Checklist:
- `CXO_REPORT_ENABLED=1`
- `CXO_REPORT_TO_EMAIL` and `CXO_REPORT_FROM_EMAIL` set
- SMTP credentials valid
- Script run from `service/` with env loaded

## Security Practices

Never commit:
- `.env` files
- service account JSON keys
- Drive OAuth token JSON
- AppSheet/Glide/LLM API keys

Use environment/secret manager in deployment.

Protect admin endpoints behind trusted network/auth.

Avoid printing raw secrets in logs or traces.

## Known Limitations

- Startup migration runner currently applies SQL files `001..010` only.
- Test coverage is minimal; no broad automated test suite is currently enforced.
- Admin endpoints are operationally useful but should be access-controlled externally.

## Maintenance Checklist

For each release:
1. Verify env keys for target environment.
2. Validate DB migrations applied.
3. Validate `/health` provider/model values.
4. Smoke test one webhook event per key path.
5. Verify AppSheet/Teams/n8n downstream behavior.
6. Verify CXO report script in dry run/manual run context.
