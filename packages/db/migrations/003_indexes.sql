-- Vector indexes (HNSW) for faster similarity search
-- Note: HNSW requires pgvector. If your DB plan is tiny, indexing may still work but slower.

-- For high-dimensional embeddings (e.g., 3072), HNSW is not supported (>2000 dims).
-- Use ivfflat instead. (Requires ANALYZE + lists tuning)
CREATE INDEX IF NOT EXISTS idx_incident_vectors_ivfflat
ON incident_vectors USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE INDEX IF NOT EXISTS idx_ccp_vectors_ivfflat
ON ccp_vectors USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE INDEX IF NOT EXISTS idx_dashboard_vectors_ivfflat
ON dashboard_vectors USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);


-- Useful metadata indexes (filters)
CREATE INDEX IF NOT EXISTS idx_incident_meta
ON incident_vectors (tenant_id, project_name, part_number);

CREATE INDEX IF NOT EXISTS idx_ccp_meta
ON ccp_vectors (tenant_id, project_name, part_number);

CREATE INDEX IF NOT EXISTS idx_runs_lookup
ON ai_runs (tenant_id, event_type, primary_id, created_at DESC);
