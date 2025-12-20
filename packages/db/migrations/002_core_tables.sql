-- ---------- RUN LOGS / IDEMPOTENCY ----------
CREATE TABLE IF NOT EXISTS ai_runs (
  run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  tenant_id TEXT NOT NULL,
  event_type TEXT NOT NULL,                 -- CHECKIN_CREATED / CONVERSATION_ADDED / etc
  primary_id TEXT NOT NULL,                 -- checkin_id or conversation_id etc

  status TEXT NOT NULL DEFAULT 'QUEUED',    -- QUEUED/RUNNING/SUCCESS/ERROR
  error_message TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ
);

-- Helps prevent accidental duplicates (MVP)
CREATE UNIQUE INDEX IF NOT EXISTS uq_ai_runs_idempotency
ON ai_runs (tenant_id, event_type, primary_id, created_at);


-- ---------- INCIDENT VECTORS (THREAD MEMORY) ----------
-- 2 vectors per checkin thread:
-- PROBLEM: description + recent remarks snapshot
-- RESOLUTION: final summary + precautions (later)

CREATE TABLE IF NOT EXISTS incident_vectors (
  tenant_id TEXT NOT NULL,
  checkin_id TEXT NOT NULL,

  vector_type TEXT NOT NULL,                -- PROBLEM / RESOLUTION
  embedding vector(3072) NOT NULL,

  project_name TEXT,
  part_number TEXT,
  legacy_id TEXT,

  status TEXT,                              -- Doubt/Pass/Fail etc
  summary_text TEXT NOT NULL,               -- the text we embedded (for audit)

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (tenant_id, checkin_id, vector_type)
);


-- ---------- CCP KNOWLEDGE VECTORS ----------
-- Stores chunks from:
-- - CCP Description
-- - CCP PDF extracted text
-- - CCP Image caption/tags (future)

CREATE TABLE IF NOT EXISTS ccp_vectors (
  chunk_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  tenant_id TEXT NOT NULL,

  ccp_id TEXT NOT NULL,
  ccp_name TEXT,

  project_name TEXT,
  part_number TEXT,
  legacy_id TEXT,

  chunk_type TEXT NOT NULL,                 -- CCP_DESC / PDF_TEXT / IMAGE_CAPTION
  chunk_text TEXT NOT NULL,
  source_ref TEXT,                          -- file/photo URL/path (pointer only)

  embedding vector(3072) NOT NULL,

  content_hash TEXT NOT NULL,               -- hash(chunk_text + ccp_id + chunk_type) for upserts

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- avoid duplicates per chunk
CREATE UNIQUE INDEX IF NOT EXISTS uq_ccp_vectors_chunk
ON ccp_vectors (tenant_id, ccp_id, chunk_type, content_hash);


-- ---------- OPTIONAL: DASHBOARD UPDATES VECTORS ----------
CREATE TABLE IF NOT EXISTS dashboard_vectors (
  item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  tenant_id TEXT NOT NULL,

  project_name TEXT,
  part_number TEXT,
  legacy_id TEXT,

  update_message TEXT NOT NULL,
  embedding vector(3072) NOT NULL,

  content_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dashboard_vectors
ON dashboard_vectors (tenant_id, content_hash);


-- ---------- ARTIFACT POINTERS (ANNOTATED IMAGE, EVIDENCE JSON, ETC.) ----------
CREATE TABLE IF NOT EXISTS artifacts (
  artifact_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  run_id UUID REFERENCES ai_runs(run_id) ON DELETE CASCADE,
  artifact_type TEXT NOT NULL,              -- ANNOTATED_IMAGE / EVIDENCE_JSON / etc
  url TEXT NOT NULL,                        -- pointer only
  meta JSONB,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
