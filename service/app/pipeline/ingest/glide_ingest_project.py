from __future__ import annotations

from ...config import Settings
from .glide_ingest_base import GlideIngestSpec, full_scan_table, incremental_upsert_row


def project_spec(settings: Settings) -> GlideIngestSpec:
    # Tune these column names to your Glide “Project” table
    return GlideIngestSpec(
        entity="project",
        table_name=getattr(settings, "glide_project_table", "") or "",

        tenant_id_column=getattr(settings, "glide_project_tenant_column", "") or "Company Row ID",
        rowid_column=getattr(settings, "glide_project_rowid_column", "") or "Row ID",

        project_name_column=getattr(settings, "glide_project_name_column", "") or "Project",
        part_number_column=getattr(settings, "glide_project_part_number_column", "") or "Part Number",
        legacy_id_column=getattr(settings, "glide_project_legacy_id_column", "") or "Legacy ID",

        title_column=getattr(settings, "glide_project_title_column", "") or "Project",
        drop_keys=[
            "Updated At",
            "Last Updated",
        ],
        rag_include_keys=None,  # keep full for projects unless you want to trim
    )


def ingest_glide_project(settings: Settings, *, limit: int = 0) -> dict:
    ps = project_spec(settings)
    # project table ingests using itself as “project_spec” index source
    return full_scan_table(settings, spec=ps, project_spec=ps, limit=limit)


def upsert_glide_project_row(settings: Settings, *, row_id: str) -> dict:
    ps = project_spec(settings)
    return incremental_upsert_row(settings, spec=ps, project_spec=ps, row_id=row_id)