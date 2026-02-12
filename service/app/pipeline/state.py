# service/app/pipeline/state.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict


class PipelineState(TypedDict, total=False):
    # Core
    payload: Dict[str, Any]
    run_id: str
    event_type: str
    primary_id: str
    idempotency_primary_id: str
    logs: List[str]

    # Meta flags
    meta: Dict[str, Any]
    ingest_only: bool
    media_only: bool

    # IDs
    tenant_id: Optional[str]
    project_name: Optional[str]
    part_number: Optional[str]
    legacy_id: Optional[str]
    checkin_id: Optional[str]
    conversation_id: Optional[str]
    ccp_id: Optional[str]

    # Loaded sheet data
    checkin_row: Optional[Dict[str, Any]]
    project_row: Optional[Dict[str, Any]]
    conversation_rows: List[Dict[str, Any]]

    # Thread / retrieval
    thread_snapshot_text: str
    packed_context: str
    closure_notes: str

    similar_incidents: List[Dict[str, Any]]
    similar_problems: List[Dict[str, Any]]
    similar_resolutions: List[Dict[str, Any]]
    similar_media: List[Dict[str, Any]]
    relevant_ccp_chunks: List[Dict[str, Any]]
    relevant_dashboard_updates: List[Dict[str, Any]]
    relevant_glide_kb_chunks: List[Dict[str, Any]]

    # Media analysis
    media_images: List[Dict[str, Any]]
    image_captions: List[str]
    defects_by_image: List[Dict[str, Any]]

    # Company context
    company_name: Optional[str]
    company_description: Optional[str]
    company_key: Optional[str]

    # Assembly todo / writeback
    assembly_todo_written: bool
    ai_reply: str
    writeback_done: bool