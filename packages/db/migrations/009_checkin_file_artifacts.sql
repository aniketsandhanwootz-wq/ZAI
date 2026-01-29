-- 009_checkin_file_artifacts.sql
-- Stores extracted text + analysis for checkin "Files" attachments.
-- Idempotent: primary key = (tenant_id, checkin_id, source_hash)

CREATE TABLE IF NOT EXISTS checkin_file_artifacts (
  tenant_id   TEXT NOT NULL,
  checkin_id  TEXT NOT NULL,

  -- stable identifier per attachment ref (prefer content hash if bytes exist)
  source_hash TEXT NOT NULL,

  source_ref  TEXT,
  filename    TEXT,
  mime_type   TEXT,
  byte_size   INTEGER,

  drive_file_id TEXT,
  direct_url    TEXT,

  -- optional: sha256 of bytes (if downloaded)
  content_hash TEXT,

  extracted_text TEXT,
  extracted_json JSONB,
  analysis_json  JSONB,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (tenant_id, checkin_id, source_hash)
);

CREATE INDEX IF NOT EXISTS idx_checkin_file_artifacts_tenant_checkin
ON checkin_file_artifacts (tenant_id, checkin_id);

CREATE INDEX IF NOT EXISTS idx_checkin_file_artifacts_content_hash
ON checkin_file_artifacts (content_hash);