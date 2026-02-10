# ZAI (Wootz.Work) — Manufacturing Intelligence Service

ZAI is a backend service that ingests manufacturing/QMS data (AppSheet + Google Sheets + Glide tables + Drive files), builds a searchable knowledge base (pgvector), and generates operational outputs (AI replies, cues, summaries, and context) back into workflows (Teams/AppSheet/Glide/Sheets).

This repo contains:
- A FastAPI service (`service/app`) for webhooks + APIs
- A worker/consumer for async jobs
- Ingestion pipelines (Sheets + Glide + Drive file artifacts)
- Vector store (Postgres + pgvector) + DB migrations
- Utility scripts (backfills + Glide reconcile)

---

## What ZAI does

### 1) Ingest & normalize data
- **AppSheet / Sheets**: projects, checkins, CCP, dashboard updates, conversations, etc.
- **Glide**: company profiles + KB tables (raw_material, processes, boughtouts, optionally project).
- **Drive / Files**: PDFs/images/excels attached in “Files” fields; extracts text/captions/analysis.

### 2) Create searchable “memory”
- Stores **embeddings** for:
  - Incident context (problem/resolution/media)
  - CCP chunks (description, pdf text, image captions)
  - Dashboard updates
  - Glide KB chunks (raw material / processes / boughtouts)
  - Company profile vectors

### 3) Retrieval + generation
- On a trigger event (typically checkin created / critical conversation), the pipeline:
  - retrieves relevant history + CCP + dashboard + KB
  - optionally analyzes attachments/media
  - generates: AI reply, assembly todo, cues, etc.
  - writes back to downstream systems (AppSheet / Teams / Sheets)

---

## Repository layout (high-level)

- `service/app/`
  - `main.py`: FastAPI entrypoint
  - `routers/`: webhook endpoints (AppSheet/Glide/Teams test)
  - `pipeline/`
    - `graph.py`: orchestrates flow
    - `nodes/`: retrieval, rerank, generate, analyze, writeback
    - `ingest/`: ingest modules (company, CCP, dashboard, Glide KB, history)
  - `tools/`: Sheets/Drive/Embedding/Vector/DB helpers
  - `integrations/`: clients for AppSheet / Glide / Teams

- `packages/db/migrations/`: Postgres schema + indexes (pgvector, artifacts, KB tables, run logs)
- `packages/contracts/sheets_mapping.yaml`: mapping for Sheets tabs/columns
- `packages/prompts/`: prompt templates used in generation

- `service/scripts/`
  - `glide_reconcile.py`: bulk sync Glide KB tables into DB + vectors (cron use-case)
  - other backfill scripts (often ignored in git if sensitive)

---

## Core components

### Postgres + pgvector (DB)
Stores:
- `ai_runs`: run log / idempotency
- `incident_vectors`, `ccp_vectors`, `dashboard_vectors`
- `glide_kb_items`, `glide_kb_vectors`
- `company_profiles`, `company_vectors`
- `artifacts`, `checkin_file_artifacts`

### Glide ingest strategy (minimal Glide API calls)
- Full-table runs use `GlideClient.list_table_rows(table)` (paginated) **once per table**
- “Change detection” is done via:
  - compute a stable `row_hash` from normalized row JSON
  - compare against DB (`get_glide_kb_item_row_hash`)
- Per-row Glide fetch (`get_row_by_row_id`) is only used for **incremental single-row webhook** paths, not for reconcile full scans.

> Note: Per-row **DB queries** and **embedding calls** still happen when rows change. That does not affect Glide API quotas but does affect runtime cost.

---

## Configuration (.env)

ZAI loads configuration from environment variables. Some values can also come from `GLIDE_CONFIG_JSON`.

### Required (typical)
**Runtime / Infra**
- `DATABASE_URL` (Postgres)
- `REDIS_URL`
- `WEBHOOK_SECRET` (or legacy `APPSHEET_WEBHOOK_SECRET`)

**Sheets**
- `GOOGLE_SHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON` **or** `GOOGLE_SERVICE_ACCOUNT_FILE`
- `SHEETS_MAPPING_PATH` (default: `packages/contracts/sheets_mapping.yaml`)

**LLM / Embeddings**
- `LLM_PROVIDER`
- `LLM_API_KEY`
- `LLM_MODEL`
- `EMBEDDING_PROVIDER`
- `EMBEDDING_API_KEY`
- `EMBEDDING_MODEL`
- `EMBEDDING_DIMS`

### Optional but common
**Drive**
- `GOOGLE_DRIVE_ROOT_FOLDER_ID`
- `GOOGLE_DRIVE_ANNOTATED_FOLDER_ID`
- `DRIVE_PREFIX_MAP_JSON` (prefix->folderId map)

**Vision**
- `VISION_PROVIDER`
- `VISION_API_KEY`
- `VISION_MODEL`

**Teams**
- `TEAMS_WEBHOOK_URL`
- `POWER_AUTOMATE_WEBHOOK_URL` (defaults to Teams webhook if not set)

**AppSheet**
- `APPSHEET_BASE_URL`
- `APPSHEET_APP_ID`
- `APPSHEET_ACCESS_KEY`
- `APPSHEET_CUES_TABLE`
- `APPSHEET_CUES_COL_*` (optional overrides)
- `APPSHEET_CONVERSATION_TABLE`
- `APPSHEET_CONVERSATION_KEY_COL`
- `APPSHEET_CONVERSATION_CRITICAL_COL`

### Glide configuration
Either set individual env vars:
- `GLIDE_API_KEY`
- `GLIDE_APP_ID`
- `GLIDE_BASE_URL` (default: `https://api.glideapp.io`)
- `GLIDE_COMPANY_TABLE`, `GLIDE_RAW_MATERIAL_TABLE`, `GLIDE_PROCESSES_TABLE`, `GLIDE_BOUGHTOUTS_TABLE`, optional `GLIDE_PROJECT_TABLE`
- Column overrides like `GLIDE_*_ROWID_COLUMN`, `GLIDE_*_TENANT_COLUMN`, etc.

Or set a single JSON:
- `GLIDE_CONFIG_JSON` supporting both old/new schemas (see `service/app/config.py`).

---

## Running locally

### 1) Install dependencies
Use your existing workflow (pip/poetry). Ensure Python version matches your environment.

### 2) Provide env vars
Create `.env` (ignored by git) with required configuration.

### 3) Run the API
Example:
- `uvicorn service.app.main:app --reload --port 5600`

### 4) Run worker/consumer (if enabled)
Controlled via:
- `RUN_CONSUMER=1`
- `CONSUMER_QUEUES=default` (or comma list)

---

## Glide reconcile cron job (Render)

Purpose: periodically sync Glide tables into DB + vectors with minimal Glide API calls.

### Command
Example (all 4 tables):
- `python3 -m service.scripts.glide_reconcile --tables company,raw_material,processes,boughtouts`

Optional limit:
- `--limit 5` (applies per table scan)

### Important: mapping path on Render
If you see:
`RuntimeError: sheets_mapping.yaml not found at: /app/packages/contracts/sheets_mapping.yaml`

Fix by setting:
- `SHEETS_MAPPING_PATH=packages/contracts/sheets_mapping.yaml`

(or ensure your working directory aligns with that path in your Render cron service)

### Git ignore strategy (keep only reconcile script)
If `service/scripts/` contains sensitive files, keep the folder ignored and whitelist only what you need:
- Track: `service/scripts/__init__.py`, `service/scripts/glide_reconcile.py`
- Ignore everything else in `service/scripts/` (tokens, creds, backfills)

---

## Operational notes

### Sequencing vs parallelism
The reconcile script runs **sequentially** per table (company → raw_material → processes → boughtouts) unless you explicitly modify it to parallelize. This is safer for DB load and rate limiting.

### Idempotency
- `ai_runs` provides run tracking (RUNNING/SUCCESS/ERROR) and helps avoid duplicate work.
- Glide KB items use `row_hash` to skip unchanged rows.
- Vectors use content hashes to avoid duplicate inserts and to remove stale chunks on changes.

### Performance expectations
Glide API calls are minimal (paginated list calls per table). Runtime is dominated by:
- DB reads per row (`get_glide_kb_item_row_hash`)
- Embeddings for changed rows
- Vector inserts/deletes

If needed, next optimization step:
- Preload existing `(tenant_id,item_id)->row_hash` for a table to reduce per-row DB queries.

---

## Troubleshooting

### Render cron error: `"PYTHONPATH=." executable not found`
In Render cron command, don’t start with `PYTHONPATH=. ...` as a standalone token.
Use:
- `python3 -m service.scripts.glide_reconcile ...`
If you need PYTHONPATH, set it via Render environment variables, not as an “executable”.

### Build error: `requirements.txt not found`
Your repo uses `service/requirements.txt` (not root). Ensure Render build command points to correct path, e.g.:
- `pip install -r service/requirements.txt`
Or configure the cron service as a Docker-based job using the existing Dockerfile.

---

## Security / secrets
Never commit:
- `.env`, service account JSON keys, drive tokens, AppSheet keys, Glide API keys.
Keep secrets in Render env groups (or secret manager) and whitelist only safe scripts for git.

---

## Status / next improvements (typical roadmap)
- Batch DB hash preload for Glide KB ingest
- More robust tenant resolution for Glide KB rows
- Better monitoring + alerting for failed cron runs
- Incremental Glide webhooks for near real-time KB updates (optional)
- LangGraph-based orchestration (optional) if you want richer stateful flows

---