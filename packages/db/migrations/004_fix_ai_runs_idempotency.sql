-- 004_fix_ai_runs_idempotency.sql

-- 1) dedupe existing rows so UNIQUE index creation won't fail
WITH ranked AS (
  SELECT
    run_id,
    tenant_id,
    event_type,
    primary_id,
    created_at,
    ROW_NUMBER() OVER (
      PARTITION BY tenant_id, event_type, primary_id
      ORDER BY created_at DESC
    ) AS rn
  FROM ai_runs
)
DELETE FROM ai_runs
WHERE run_id IN (
  SELECT run_id FROM ranked WHERE rn > 1
);

-- 2) drop old ineffective unique index
DROP INDEX IF EXISTS uq_ai_runs_idempotency;

-- 3) create correct unique index
CREATE UNIQUE INDEX IF NOT EXISTS uq_ai_runs_idempotency
ON ai_runs (tenant_id, event_type, primary_id);
