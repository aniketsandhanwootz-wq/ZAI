-- 008_glide_kb.sql
-- Glide Knowledge Base storage: items + chunk vectors
-- NOTE: embedding dimension assumed 1536 to match existing defaults.

-- ----------------------------
-- Items (one row per Glide record)
-- ----------------------------
CREATE TABLE IF NOT EXISTS glide_kb_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  tenant_id TEXT NOT NULL,

  -- Which Glide table + row
  source_table TEXT NOT NULL,
  source_row_id TEXT NOT NULL,

  -- Legacy project join spine (Phase 0)
  legacy_id TEXT NULL,

  -- Common join keys (best-effort)
  project_name TEXT NULL,
  part_number TEXT NULL,

  -- Useful content fields
  title TEXT NULL,
  body TEXT NULL,

  -- Full original Glide row payload (normalized/cleaned in code)
  raw JSONB NOT NULL DEFAULT '{}'::jsonb,

  -- For idempotent upsert & change detection
  row_hash TEXT NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (tenant_id, source_table, source_row_id)
);

-- Keep updated_at fresh on updates (simple trigger)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger WHERE tgname = 'trg_glide_kb_items_updated_at'
  ) THEN
    CREATE OR REPLACE FUNCTION set_updated_at_glide_kb_items()
    RETURNS TRIGGER AS $f$
    BEGIN
      NEW.updated_at = now();
      RETURN NEW;
    END;
    $f$ LANGUAGE plpgsql;

    CREATE TRIGGER trg_glide_kb_items_updated_at
    BEFORE UPDATE ON glide_kb_items
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at_glide_kb_items();
  END IF;
END $$;

-- ----------------------------
-- Vectors (chunks per item)
-- ----------------------------
CREATE TABLE IF NOT EXISTS glide_kb_vectors (
  id BIGSERIAL PRIMARY KEY,

  tenant_id TEXT NOT NULL,

  -- denormalized filters (fast WHERE)
  legacy_id TEXT NULL,
  project_name TEXT NULL,
  part_number TEXT NULL,

  source_table TEXT NOT NULL,
  source_row_id TEXT NOT NULL,

  item_id UUID NOT NULL REFERENCES glide_kb_items(id) ON DELETE CASCADE,

  chunk_index INT NOT NULL,
  chunk_text TEXT NOT NULL,

  content_hash TEXT NOT NULL,

  -- IMPORTANT: adjust dim if your embedding model changes
  embedding VECTOR(1536) NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (tenant_id, source_table, source_row_id, chunk_index, content_hash)
);

-- ----------------------------
-- Indexes (consistent with your current pattern)
-- ----------------------------

-- Filter indexes
CREATE INDEX IF NOT EXISTS idx_glide_kb_items_meta
ON glide_kb_items (tenant_id, legacy_id, project_name, part_number);

CREATE INDEX IF NOT EXISTS idx_glide_kb_vectors_meta
ON glide_kb_vectors (tenant_id, legacy_id, project_name, part_number);

CREATE INDEX IF NOT EXISTS idx_glide_kb_vectors_lookup
ON glide_kb_vectors (tenant_id, source_table, source_row_id);

-- Vector index (IVFFLAT, cosine) — matches your existing approach
CREATE INDEX IF NOT EXISTS idx_glide_kb_vectors_ivfflat
ON glide_kb_vectors USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);