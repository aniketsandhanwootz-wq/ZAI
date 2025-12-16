-- Vector indexes (HNSW) for faster similarity search
-- Note: HNSW requires pgvector. If your DB plan is tiny, indexing may still work but slower.

CREATE INDEX IF NOT EXISTS idx_incident_vectors_hnsw
ON incident_vectors
USING hnsw (embedding vector_cosine_ops);

CREATE INDEX IF NOT EXISTS idx_ccp_vectors_hnsw
ON ccp_vectors
USING hnsw (embedding vector_cosine_ops);

CREATE INDEX IF NOT EXISTS idx_dashboard_vectors_hnsw
ON dashboard_vectors
USING hnsw (embedding vector_cosine_ops);

-- Useful metadata indexes (filters)
CREATE INDEX IF NOT EXISTS idx_incident_meta
ON incident_vectors (tenant_id, project_name, part_number);

CREATE INDEX IF NOT EXISTS idx_ccp_meta
ON ccp_vectors (tenant_id, project_name, part_number);

CREATE INDEX IF NOT EXISTS idx_runs_lookup
ON ai_runs (tenant_id, event_type, primary_id, created_at DESC);
