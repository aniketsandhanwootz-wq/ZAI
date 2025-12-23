CREATE INDEX IF NOT EXISTS idx_incident_vectors_type_meta
ON incident_vectors (tenant_id, vector_type, project_name, part_number);

CREATE INDEX IF NOT EXISTS idx_dashboard_vectors_meta
ON dashboard_vectors (tenant_id, project_name, part_number);
