from __future__ import annotations

from ...config import Settings
from .glide_ingest_base import GlideIngestSpec, full_scan_table, incremental_upsert_row
from .glide_ingest_project import project_spec


def boughtouts_spec(settings: Settings) -> GlideIngestSpec:
    return GlideIngestSpec(
        entity="boughtout",
        table_name=getattr(settings, "glide_boughtouts_table", "") or "",

        tenant_id_column=getattr(settings, "glide_boughtouts_tenant_column", "") or "Company Row ID",
        rowid_column=getattr(settings, "glide_boughtouts_rowid_column", "") or "$rowID",

        project_name_column=getattr(settings, "glide_boughtouts_project_name_column", "") or "Project",
        part_number_column=getattr(settings, "glide_boughtouts_part_number_column", "") or "Part Number",
        legacy_id_column=getattr(settings, "glide_boughtouts_legacy_id_column", "") or "Legacy ID",
        project_row_id_column=getattr(settings, "glide_boughtouts_project_row_id_column", "") or "Project Row ID",

        title_column=getattr(settings, "glide_boughtouts_title_column", "") or "Name",

        drop_keys=["Updated At", "Last Updated"],
        rag_include_keys=None,
    )


def ingest_glide_boughtouts(settings: Settings, *, limit: int = 0) -> dict:
    spec = boughtouts_spec(settings)
    ps = project_spec(settings)
    return full_scan_table(settings, spec=spec, project_spec=ps, limit=limit)


def upsert_glide_boughtouts_row(settings: Settings, *, row_id: str) -> dict:
    spec = boughtouts_spec(settings)
    ps = project_spec(settings)
    return incremental_upsert_row(settings, spec=spec, project_spec=ps, row_id=row_id)