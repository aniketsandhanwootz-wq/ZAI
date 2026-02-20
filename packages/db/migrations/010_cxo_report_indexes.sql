-- Ensure dashboard_vectors has updated_at (so CXO report can time-filter reliably)
ALTER TABLE dashboard_vectors
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();

-- Backfill nulls (if any)
UPDATE dashboard_vectors
SET updated_at = now()
WHERE updated_at IS NULL;

-- Speed up daily CXO report lookups (filter by tenant_id, legacy_id, updated_at)
CREATE INDEX IF NOT EXISTS idx_incident_vectors_tenant_legacy_updated
ON incident_vectors (tenant_id, legacy_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_dashboard_vectors_tenant_legacy_updated
ON dashboard_vectors (tenant_id, legacy_id, updated_at DESC);