# Flow 1 — Current Truth (DB + Vectors + Triggers + Idempotency)

This document freezes the current production truth for:
- Postgres schema (run logs, vectors, artifacts)
- Idempotency keys
- Event graph branching
- What writes happen where

Repo paths referenced are canonical.

---

## 0) Key identifiers (naming truth)

### Tenant ID
- `tenant_id` = Project sheet column `company_row_id` (Glide `$rowID`).
- In pipeline state: `state["tenant_id"]`.

### Entity IDs
- `checkin_id` (CheckIN row identity used for incident vectors)
- `conversation_id` (Conversation row identity; pipeline still attaches to checkin_id)
- `ccp_id` (CCP row identity)
- `legacy_id` (Project/CCP/Dashboard link key)

---

## 1) Tables (schema truth)

### 1.1 ai_runs (run log / idempotency)
Primary purpose:
- Track pipeline run status
- Enforce “same event shouldn’t run twice” via unique index

Columns:
- `run_id UUID PK`
- `tenant_id TEXT NOT NULL`
- `event_type TEXT NOT NULL`
- `primary_id TEXT NOT NULL`
- `status TEXT DEFAULT 'QUEUED'`
- `error_message TEXT`
- timestamps

Idempotency index (intended):
- Unique on `(tenant_id, event_type, primary_id)`

> NOTE: If you still have an index that includes created_at, it weakens idempotency.

---

### 1.2 incident_vectors (checkin history + closure + media)
Primary purpose:
- Store embeddings for retrieval at checkin reply time

Primary key:
- `(tenant_id, checkin_id, vector_type)`

Columns:
- `vector_type` ∈ { `PROBLEM`, `RESOLUTION`, `MEDIA` }  (current usage)
- `embedding vector(1536)`
- metadata: `project_name`, `part_number`, `legacy_id`, `status`
- `summary_text` is the stored text for that vector

Upsert behavior:
- Always upsert on PK

---

### 1.3 ccp_vectors (CCP knowledge chunks)
Primary purpose:
- Store CCP chunks for retrieval

Uniqueness:
- Unique `(tenant_id, ccp_id, chunk_type, content_hash)`

Chunk types (current usage):
- `CCP_DESC`  (chunked from CCP description)
- `PDF_TEXT`  (text extracted from PDF attachments)
- `IMG_CAPTION` (caption from image attachments via VisionTool)

Idempotency:
- `content_hash` computed via stable hash:
  - includes `ccp_id`, `chunk_type`, `stable_key` (DESC or file_hash), normalized text.

---

### 1.4 dashboard_vectors (project updates memory)
Primary purpose:
- Store embeddings for dashboard update rows

Uniqueness:
- Unique `(tenant_id, content_hash)`

Insert behavior:
- DO NOTHING on conflict (append-only per unique update)

---

### 1.5 artifacts (media outputs / cache helpers)
Primary purpose:
- Store generated artifact URLs + metadata
- Use meta JSON for idempotency/caching by source_hash

Lookup pattern:
- `(artifact_type, meta.tenant_id, meta.checkin_id, meta.source_hash)` via index

---

### 1.6 company_profiles + company_vectors
company_profiles:
- cache of Glide company row

company_vectors:
- one vector per tenant_row_id (currently)
- `tenant_row_id TEXT PK`
- `embedding vector(1536)`
- `content_hash` for idempotency

---

## 2) Vector index strategy (retrieval performance truth)

ivfflat indexes:
- incident_vectors.embedding
- ccp_vectors.embedding
- dashboard_vectors.embedding

Plus filter indexes:
- incident_vectors: `(tenant_id, vector_type, project_name, part_number)`
- ccp_vectors: `(tenant_id, project_name, part_number)`
- dashboard_vectors: `(tenant_id, project_name, part_number)`
- ai_runs lookup: `(tenant_id, event_type, primary_id, created_at desc)`

Operational:
- After creating ivfflat, ANALYZE is required for good recall/speed.

---

## 3) Trigger and execution model (runtime truth)

### 3.1 Web service -> Queue
- `service/app/queue.py::enqueue_job(settings, payload)`
  - Enqueues RQ job on queue `"default"`:
    - `process_event_task(payload)`

### 3.2 Worker
- `service/app/worker_tasks.py::process_event_task(payload)`
  - Loads settings
  - Calls `run_event_graph(settings, payload)`

### 3.3 Worker lifecycle
- `service/app/consumer.py` can spawn `rq worker` as a separate process in the same web container when `RUN_CONSUMER=1`.

---

## 4) Event graph truth (branching)

Allowed events:
- CHECKIN_CREATED
- CHECKIN_UPDATED
- CONVERSATION_ADDED
- CCP_UPDATED
- DASHBOARD_UPDATED
- MANUAL_TRIGGER

Primary ID selection (base):
- checkin_id OR conversation_id OR ccp_id OR legacy_id OR "UNKNOWN"

Scoped primary_id (important for idempotency):
- if ingest_only and media_only  -> "<primary_id>::MEDIA_V1"
- else if ingest_only            -> "<primary_id>::INGEST_V1"
- else                           -> "<primary_id>"

RunLog.start key:
- `(tenant_id_hint, event_type, primary_id_scoped)`
where:
- tenant_id_hint = payload.meta.tenant_id OR "UNKNOWN"

Later:
- after load_sheet_data resolves tenant_id, RunLog.update_tenant(run_id, tenant_id) runs.

---

## 5) Node responsibilities (what each node produces)

### 5.1 load_sheet_data
Inputs:
- payload.{checkin_id, conversation_id, ccp_id, legacy_id, meta}

Outputs to state:
- checkin_row, project_row, conversation_rows
- project_name, part_number, legacy_id, checkin_status, checkin_description
- tenant_id resolved:
  - meta override OR Project sheet lookup by legacy_id (ID-first) OR fallback triplet
- closure_notes extracted from conversation (filtered hints)
- inspection images:
  - resolves sheet cell refs -> drive file ids -> `https://drive.google.com/uc?export=view&id=...`
  - state["checkin_image_urls"] (up to 3)
- company routing:
  - company_name / company_key derived from project_name
  - optional Glide override if tenant_id exists
- company description:
  - Postgres cache -> Glide fallback -> embed -> upsert company_vectors

---

### 5.2 build_thread_snapshot (not shown here, but required invariant)
Must produce:
- `state["thread_snapshot_text"]` (the canonical “problem text” used for embedding and retrieval query)

---

### 5.3 analyze_media (not shown here, but required invariant)
Must produce:
- `state["image_captions"]` as a list of caption strings (can be empty)
and/or artifact outputs in artifacts table.

---

### 5.4 upsert_vectors
Writes to incident_vectors:
- PROBLEM: embed(thread_snapshot_text) -> upsert
- RESOLUTION: embed(closure_notes formatted) -> upsert (only if closure_notes exists)
- MEDIA: embed("MEDIA CAPTIONS:\n- ...") -> upsert (only if captions exist)

---

### 5.5 retrieve_context
Reads (pgvector cosine distance):
- company_vectors (top 1)
- incident_vectors: PROBLEM, RESOLUTION, MEDIA
- ccp_vectors
- dashboard_vectors

Writes to state:
- similar_problems / similar_resolutions / similar_media
- relevant_ccp_chunks
- relevant_dashboard_updates
- company_profile_text (best match)

Also:
- drops current checkin_id from retrieved incidents.

---

## 6) Reply/writeback invariant (critical)

Only CHECKIN_CREATED can generate reply and writeback.

Rules in graph:
- CHECKIN_UPDATED / CONVERSATION_ADDED default to ingest_only=True.
- event_type != CHECKIN_CREATED:
  - vectors may be refreshed
  - reply/writeback is skipped even if caller forgot ingest_only
- CHECKIN_CREATED:
  - full pipeline runs:
    retrieve_context -> rerank_context -> generate_ai_reply -> annotate_media -> upsert_vectors -> writeback

---

## 7) Invariants we must preserve in new ingestions

### 7.1 Data correctness invariants
- tenant_id must be resolved before any vector write.
- all embeddings stored must match EMBEDDING_DIMS (1536).
- content_hash must be stable under whitespace/format noise.

### 7.2 Idempotency invariants
- Run idempotency:
  - same (tenant_id, event_type, scoped primary_id) should not start two RUNNING runs.
- Storage idempotency:
  - incident_vectors is last-write-wins per (tenant, checkin, vector_type)
  - ccp_vectors is stable set per (tenant, ccp, chunk_type, content_hash)
  - dashboard_vectors is append-only per (tenant, content_hash)
  - artifacts caching uses meta.source_hash

### 7.3 Performance invariants
- ivfflat indexes exist for embeddings
- ANALYZE after index creation is executed (best effort)

---

## 8) Stable ingestion template for new domains (Raw Material / Process / BO / Project)

For any new entity type:
1) Resolve tenant_id (ID-first)
2) Build stable content_hash = sha256(normalized_text + stable_key)
3) If hash exists -> skip embedding
4) Embed text (document embedding)
5) Upsert/insert into <domain>_vectors with uniqueness key:
   - (tenant_id, entity_id, chunk_type, content_hash)
6) Add metadata filter columns: project_name/part_number/legacy_id as needed
7) Add ivfflat + metadata indexes + ANALYZE