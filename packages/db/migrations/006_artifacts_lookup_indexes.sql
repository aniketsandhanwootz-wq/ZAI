-- 006_artifacts_lookup_indexes.sql

-- Speed up idempotency lookups (caption/annotated images)
CREATE INDEX IF NOT EXISTS idx_artifacts_type_tenant_checkin_hash
ON artifacts (
  artifact_type,
  (meta->>'tenant_id'),
  (meta->>'checkin_id'),
  (meta->>'source_hash')
);
