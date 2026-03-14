# ZAI

Operational AI runtime for Wootz manufacturing workflows.

ZAI ingests manufacturing updates from Google Sheets, Glide, Drive, and AppSheet, builds retrieval memory in Postgres/pgvector, runs event-driven AI workflows, and pushes results back into operational systems such as AppSheet, Teams, n8n, and SMTP.

It is built for three things:

- reliable ingestion from messy operational systems
- retrieval-backed AI generation on live manufacturing context
- deterministic writeback and operator runbooks

## At A Glance

| Layer | Stack | Responsibility |
| --- | --- | --- |
| API | FastAPI | Webhooks, admin endpoints, health, request lifecycle |
| Queue | Redis + RQ | Background execution and replay-safe job handling |
| Pipeline | `service/app/pipeline/*` | Event graph, ingest, retrieval, generation, writeback |
| Memory | Postgres + pgvector | Incident, dashboard, CCP, Glide KB, artifacts, runs |
| Integrations | Sheets, Drive, Glide, AppSheet, Teams, n8n, SMTP | External data sources and sinks |

## System Map

```text
Google Sheets / Glide / Drive / AppSheet
                |
                v
        FastAPI webhook surface
                |
                v
          Event graph (RQ jobs)
                |
    +-----------+-----------+
    |                       |
    v                       v
Ingestion + artifacts   AI generation
    |                       |
    +-----------+-----------+
                |
                v
       Postgres + pgvector memory
                |
    +-----------+-----------+-----------+
    |           |           |           |
    v           v           v           v
AppSheet      Teams        n8n        SMTP
```

## What ZAI Does

### 1. Ingest operational data

- Check-ins, conversations, dashboard updates, CCP rows, projects, and Glide knowledge tables
- File and image references from Drive-backed workflows
- Incremental and bulk backfill paths for recovery

### 2. Build retrieval memory

- Incident memory from check-ins and conversations
- Dashboard movement memory
- CCP memory
- Company and Glide KB vectors
- Attachment and artifact metadata for audit and idempotency

### 3. Generate operator-facing outputs

- AI replies for check-ins
- Critical escalation signals
- Assembly todo / cue refresh
- CXO daily manufacturing report email

### 4. Write back actions

- AppSheet conversation updates
- Teams notifications
- n8n webhook for critical flows
- SMTP report delivery

## Quick Start

### Prerequisites

- Python 3.11
- Postgres with `pgvector`
- Redis
- Google Sheets service-account credential
- Google Drive OAuth token if Drive-backed files are needed

### Install

```bash
cd /Users/aniketsandhan/Desktop/ZAI
python3 -m venv service/.venv
source service/.venv/bin/activate
pip install -r service/requirements.txt
```

### Minimum local setup

Create `service/.env` and populate the required variables:

- `DATABASE_URL`
- `REDIS_URL`
- `GOOGLE_SHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON` or `GOOGLE_SERVICE_ACCOUNT_FILE`
- `WEBHOOK_SECRET`
- `LLM_PROVIDER`
- `LLM_API_KEY`
- `LLM_MODEL`
- `EMBEDDING_PROVIDER`
- `EMBEDDING_API_KEY`
- `EMBEDDING_MODEL`
- `EMBEDDING_DIMS`

### Run the API

From `service/`:

```bash
cd /Users/aniketsandhan/Desktop/ZAI/service
source .venv/bin/activate
uvicorn app.main:app --reload --port 8000
```

Health check:

```bash
curl http://localhost:8000/health
```

Operational notes:

- `app.main` auto-loads `service/.env`
- worker auto-starts when `RUN_CONSUMER=1`
- migrations auto-run when `RUN_MIGRATIONS=1`

## Credential Model

This repo uses two different Google auth models. Mixing them up is the fastest way to break local runs.

| Surface | Env var | Expected credential type |
| --- | --- | --- |
| Google Sheets | `GOOGLE_SERVICE_ACCOUNT_JSON` or `GOOGLE_SERVICE_ACCOUNT_FILE` | Real service-account JSON |
| Google Drive | `DRIVE_TOKEN_JSON` | OAuth token JSON string or path |

Important:

- `GOOGLE_SERVICE_ACCOUNT_FILE` must point to a service-account key, not a user OAuth token file
- `DRIVE_TOKEN_JSON` is separate and is allowed to be a user OAuth token

## Repository Map

| Path | Purpose |
| --- | --- |
| `service/app/main.py` | FastAPI app, startup lifecycle, health, admin endpoints |
| `service/app/pipeline/graph.py` | Core event orchestrator |
| `service/app/pipeline/nodes/` | Pipeline nodes for loading, retrieval, generation, media, writeback |
| `service/app/pipeline/ingest/` | Bulk and incremental ingest logic |
| `service/app/tools/` | Sheets, vector, embedding, drive, LLM, CXO report helpers |
| `service/app/integrations/` | AppSheet, Glide, Teams, SMTP clients |
| `service/scripts/` | Operator scripts, backfills, reconciliation, report execution |
| `packages/contracts/` | Column mapping contracts |
| `packages/prompts/` | Prompt templates |
| `packages/db/migrations/` | SQL schema and index migrations |

## Runtime Architecture

### API process

- receives webhook and admin traffic
- validates payloads and secrets
- enqueues work to Redis/RQ

### Embedded worker process

- spawned by app lifecycle when `RUN_CONSUMER=1`
- managed by `service/app/consumer.py`
- executes event graph jobs in a separate process

### Postgres

Stores:

- vectors and retrieval memory
- AI run tracking
- artifact metadata
- cached company profiles

### Redis

- RQ transport
- queue buffering between webhooks and processing

## Event Model

Payload schema lives in `service/app/schemas/webhook.py`.

### Supported events

| Event | Primary identity | Default behavior |
| --- | --- | --- |
| `CHECKIN_CREATED` | `checkin_id` | Full pipeline, reply generation, writeback, optional Teams/n8n |
| `CHECKIN_UPDATED` | `checkin_id` | Ingest-focused refresh |
| `CONVERSATION_ADDED` | `conversation_id` | Incremental ingest of thread context |
| `CCP_UPDATED` | `ccp_id` | Incremental CCP ingest and assembly todo refresh |
| `DASHBOARD_UPDATED` | `dashboard_update_id` | Exact-row dashboard ingest and assembly todo refresh |
| `PROJECT_UPDATED` | `legacy_id` | Project/assembly refresh path |
| `MANUAL_TRIGGER` | operator supplied | Manual execution entry point |

### Dashboard identity model

`DASHBOARD_UPDATED` is wired around canonical `Dashboard Update ID`.

- canonical stored identity: `Dashboard Update ID`
- backward-compatible lookup aliases: `dashboard_row_id`, `row_id`
- active graph path does not fall back to assembly-level `legacy_id`

### High-level `CHECKIN_CREATED` flow

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

## External Interfaces

### Sheets webhook

- `POST /webhooks/sheets`
- required header: `x-sheets-secret: <WEBHOOK_SECRET>`
- body: `WebhookPayload`

Example:

```json
{
  "event_type": "CHECKIN_CREATED",
  "checkin_id": "CHK_12345",
  "legacy_id": "PRJ_001"
}
```

### Glide webhook

- `POST /webhooks/glide`
- accepted secrets:
  - `x-webhook-secret`
  - `Authorization: Bearer <secret>`
  - `?secret=<secret>`

Example:

```json
{
  "table_key": "raw_material",
  "row_ids": ["row_a", "row_b"],
  "event": "updated",
  "meta": {}
}
```

### Admin endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Runtime/provider health |
| `POST /admin/trigger` | Manual event execution |
| `POST /admin/migrate` | Run SQL migrations |
| `POST /admin/ingest` | Bulk ingest/backfill entry point |

These endpoints are operational surfaces and should be protected outside the app.

## Storage Model

### Primary tables

| Category | Tables |
| --- | --- |
| Run tracking | `ai_runs` |
| Retrieval memory | `incident_vectors`, `ccp_vectors`, `dashboard_vectors`, `glide_kb_vectors`, `company_vectors` |
| Glide KB base data | `glide_kb_items` |
| Artifacts | `artifacts`, `checkin_file_artifacts` |
| Cache/profile | `company_profiles` |

Migrations live in `packages/db/migrations/`.

Current startup migrator runs SQL files `001` to `010` from `service/app/pipeline/ingest/migrate.py`.

## Configuration

All runtime configuration is loaded through `service/app/config.py`.

### Boot-critical

| Variable | Required | Notes |
| --- | --- | --- |
| `DATABASE_URL` | Yes | Postgres connection |
| `REDIS_URL` | Yes | Queue backend |
| `GOOGLE_SHEET_ID` | Yes | Primary spreadsheet |
| `GOOGLE_SERVICE_ACCOUNT_JSON` or `GOOGLE_SERVICE_ACCOUNT_FILE` | Yes | Sheets auth |
| `WEBHOOK_SECRET` or `APPSHEET_WEBHOOK_SECRET` | Yes | Webhook auth |

### Core AI

- `LLM_PROVIDER`
- `LLM_API_KEY`
- `LLM_MODEL`
- `LLM_FALLBACK_MODELS`
- `EMBEDDING_PROVIDER`
- `EMBEDDING_API_KEY`
- `EMBEDDING_MODEL`
- `EMBEDDING_DIMS`

### Runtime flags

- `RUN_CONSUMER`
- `CONSUMER_QUEUES`
- `RUN_MIGRATIONS`

### Sheets and mapping

- `SHEETS_MAPPING_PATH`
- `GOOGLE_SHEET_ADDITIONAL_PHOTOS_ID`
- `ADDITIONAL_PHOTOS_TAB_NAME`

### Drive and vision

- `GOOGLE_DRIVE_ROOT_FOLDER_ID`
- `GOOGLE_DRIVE_ANNOTATED_FOLDER_ID`
- `DRIVE_PREFIX_MAP_JSON`
- `DRIVE_TOKEN_JSON`
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
- `APPSHEET_CUES_COL_*`

### Glide

Either configure explicit Glide env vars or use `GLIDE_CONFIG_JSON`.

Relevant Glide surfaces:

- company
- raw material
- processes
- boughtouts
- optional project table

### CXO report

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

### Tracing

- `LANGSMITH_TRACING` or `LANGCHAIN_TRACING_V2`
- `LANGSMITH_API_KEY` or `LANGCHAIN_API_KEY`
- `LANGSMITH_PROJECT` or `LANGCHAIN_PROJECT`

## Operator Runbooks

### Glide reconcile

Purpose: sync Glide knowledge tables into DB/vector memory.

```bash
cd /Users/aniketsandhan/Desktop/ZAI
source service/.venv/bin/activate
python -m service.scripts.glide_reconcile --tables company,raw_material,processes,boughtouts
```

Useful flags:

- `--limit`
- `--dry-run`

### Backfill `CHECKIN_CREATED`

Purpose: replay missed check-in created flows for a list of IDs.

```bash
cd /Users/aniketsandhan/Desktop/ZAI
source service/.venv/bin/activate
python -m service.scripts.backfill_checkin_created --file missed_ids.txt --mode inline
```

Useful flags:

- `--dry-run`
- `--sleep`
- `--limit`
- `--mode enqueue`

### Backfill ZAI cues

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

### Send CXO report manually

This command attempts a real SMTP send. There is no dry-run flag in the current script.

```bash
cd /Users/aniketsandhan/Desktop/ZAI/service
source .venv/bin/activate
PYTHONPATH=. .venv/bin/python -c 'from dotenv import load_dotenv; load_dotenv(".env", override=True); import runpy; runpy.run_path("scripts/send_cxo_daily_report.py", run_name="__main__")'
```

What it does:

- reads assemblies from Sheets
- fetches recent DB updates
- builds major movement and quality summaries
- runs LLM batch enrichment
- sends HTML email via SMTP

### Backfill dashboard updates by `Dashboard Update ID`

Purpose: backfill canonical dashboard vectors for specific dashboard update IDs.

```bash
cd /Users/aniketsandhan/Desktop/ZAI
source service/.venv/bin/activate
python service/scripts/backfill_dashboard_updates_by_id.py --ids-file service/scripts/dashboard_update_ids.txt --dry-run
python service/scripts/backfill_dashboard_updates_by_id.py --ids-file service/scripts/dashboard_update_ids.txt
```

Behavior:

- reads rows from the `Dashboard Updates` sheet using canonical `Dashboard Update ID`
- validates `legacy_id`, `update_message`, tenant resolution, and source timestamp
- `--dry-run` performs validation and embedding calls without Postgres writes
- live run writes only to Postgres, not back to Sheets

## Deployment

### Web service image

`service/Dockerfile`

- installs `service/requirements.txt`
- copies `service/app`, `service/scripts`, `packages`
- runs `uvicorn app.main:app`

### CXO cron image

`service/Dockerfile.cxo_cron`

- same dependency base
- runs one-shot `python -m app.scripts.send_cxo_daily_report`

### Recommended split

- web service: API + embedded worker with `RUN_CONSUMER=1`
- cron service: CXO report schedule

## Observability

### Runtime visibility

- contextual logs include request id and run id
- `ai_runs` tracks `RUNNING`, `SUCCESS`, and `ERROR`
- `/health` exposes provider/model/runtime flags

### Idempotency

- event graph computes event-specific primary identities
- replay flows can bypass idempotency explicitly when needed

## Troubleshooting

### `ModuleNotFoundError: No module named 'app'`

Cause: running an `app.*` script from the wrong working directory.

Fix:

- `cd service`
- run the script from there, or use the documented wrapper command

### Google Sheets auth error about missing `client_email`

Cause: `GOOGLE_SERVICE_ACCOUNT_FILE` points to an OAuth token file instead of a service-account JSON.

Fix:

- use a real service-account JSON for Sheets
- keep Drive OAuth in `DRIVE_TOKEN_JSON`

### Drive error `invalid_grant`

Cause: Drive refresh token is expired or revoked.

Fix:

- regenerate the token JSON
- update `DRIVE_TOKEN_JSON`

### Queue unavailable / webhook returns `503`

Cause: Redis unreachable or misconfigured.

Fix:

- verify `REDIS_URL`
- verify network access from the running service

### CXO report not sent

Checklist:

- `CXO_REPORT_ENABLED=1`
- `CXO_REPORT_TO_EMAIL` and `CXO_REPORT_FROM_EMAIL` set
- SMTP credentials valid
- script run from `service/` with env loaded

## Security

Never commit:

- `.env` files
- service-account JSON files
- Drive OAuth token JSON
- SMTP passwords
- API keys

Operational rules:

- protect admin endpoints at network/auth layer
- avoid printing raw secrets in logs
- rotate credentials if they were exposed in terminal history, screenshots, or chat logs

## Known Limitations

- startup migrator currently applies SQL files `001` to `010` only
- broad automated test coverage is still limited
- admin endpoints are powerful and rely on external protection

## Release Checklist

1. verify env values for the target environment
2. verify migrations are applied
3. verify `/health`
4. smoke-test one webhook on each major path
5. verify downstream AppSheet/Teams/n8n behavior
6. verify CXO manual run path if report delivery changed
