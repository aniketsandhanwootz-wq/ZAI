from __future__ import annotations

from ...config import Settings
from .glide_ingest_base import GlideIngestSpec, full_scan_table, incremental_upsert_row
from .glide_ingest_project import project_spec


def processes_spec(settings: Settings) -> GlideIngestSpec:
    return GlideIngestSpec(
        entity="process",
        table_name=getattr(settings, "glide_processes_table", "") or "",

        tenant_id_column=getattr(settings, "glide_processes_tenant_column", "") or "Company Row ID",
        rowid_column=getattr(settings, "glide_processes_rowid_column", "") or "Row ID",

        project_name_column=getattr(settings, "glide_processes_project_name_column", "") or "Project",
        part_number_column=getattr(settings, "glide_processes_part_number_column", "") or "Part Number",
        legacy_id_column=getattr(settings, "glide_processes_legacy_id_column", "") or "Legacy ID",
        project_row_id_column=getattr(settings, "glide_processes_project_row_id_column", "") or "Project Row ID",

        title_column=getattr(settings, "glide_processes_title_column", "") or "Process Name",

        drop_keys=["Updated At", "Last Updated"],
        rag_include_keys=None,
    )


def ingest_glide_processes(settings: Settings, *, limit: int = 0) -> dict:
    spec = processes_spec(settings)
    ps = project_spec(settings)
    return full_scan_table(settings, spec=spec, project_spec=ps, limit=limit)


def upsert_glide_process_row(settings: Settings, *, row_id: str) -> dict:
    spec = processes_spec(settings)
    ps = project_spec(settings)
    return incremental_upsert_row(settings, spec=spec, project_spec=ps, row_id=row_id)