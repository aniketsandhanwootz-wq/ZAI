-- Speed up daily CXO report lookups when windowing by created_at (stable "first seen" timestamp)

CREATE INDEX IF NOT EXISTS idx_incident_vectors_tenant_legacy_created
ON incident_vectors (tenant_id, legacy_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dashboard_vectors_tenant_legacy_created
ON dashboard_vectors (tenant_id, legacy_id, created_at DESC);