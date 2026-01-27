-- 008_glide_kb.sql
-- Glide KB tables aligned with service/app/tools/vector_tool.py
-- IMPORTANT: This migration must be idempotent (NO DROP TABLE).
-- If you ever need a reset, create a separate admin-only script or a new migration.

-- Items: one per Glide record
CREATE TABLE IF NOT EXISTS glide_kb_items (
  tenant_id TEXT NOT NULL,
  item_id TEXT NOT NULL,              -- e.g. "native-table-xxx:<rowid>"
  table_name TEXT NOT NULL,
  row_id TEXT NOT NULL,

  row_hash TEXT NOT NULL,

  project_name TEXT NULL,
  part_number TEXT NULL,
  legacy_id TEXT NULL,

  title TEXT NULL,
  rag_text TEXT NOT NULL DEFAULT '',
  raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,

  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (tenant_id, item_id)
);

-- Vectors: chunks per item
CREATE TABLE IF NOT EXISTS glide_kb_vectors (
  tenant_id TEXT NOT NULL,
  item_id TEXT NOT NULL,

  chunk_index INT NOT NULL,
  chunk_text TEXT NOT NULL,

  embedding VECTOR(1536) NOT NULL,
  content_hash TEXT NOT NULL,

  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (tenant_id, item_id, content_hash),
  FOREIGN KEY (tenant_id, item_id)
    REFERENCES glide_kb_items (tenant_id, item_id)
    ON DELETE CASCADE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_glide_kb_items_meta
  ON glide_kb_items (tenant_id, legacy_id, project_name, part_number);

CREATE INDEX IF NOT EXISTS idx_glide_kb_vectors_item
  ON glide_kb_vectors (tenant_id, item_id);

CREATE INDEX IF NOT EXISTS idx_glide_kb_vectors_ivfflat
  ON glide_kb_vectors USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);