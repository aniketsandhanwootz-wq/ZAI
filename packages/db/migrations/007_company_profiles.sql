-- ---------- COMPANY PROFILES (Glide cache) ----------
CREATE TABLE IF NOT EXISTS company_profiles (
  tenant_row_id TEXT PRIMARY KEY,         -- Glide $rowID
  company_name TEXT,
  company_description TEXT,

  source TEXT NOT NULL DEFAULT 'glide',
  raw JSONB,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_company_profiles_updated_at
ON company_profiles (updated_at);

-- ---------- COMPANY VECTORS (pgvector for client context) ----------
CREATE TABLE IF NOT EXISTS company_vectors (
  tenant_row_id TEXT PRIMARY KEY,         -- Glide $rowID
  embedding vector(1536) NOT NULL,

  company_name TEXT,
  company_description TEXT NOT NULL,

  content_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_company_vectors_hash
ON company_vectors (tenant_row_id, content_hash);
