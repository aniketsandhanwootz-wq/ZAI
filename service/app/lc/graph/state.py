# service/app/lc/graph/state.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, TypedDict


class LCState(TypedDict, total=False):
    # Core
    payload: Dict[str, Any]
    run_id: str
    event_type: str
    primary_id: str
    idempotency_primary_id: str

    # Derived IDs / context
    tenant_id: str
    project_name: str
    part_number: str
    legacy_id: str
    checkin_id: str
    conversation_id: str
    ccp_id: str

    # Prepared text
    thread_snapshot_text: str

    # Retrieval
    similar_incidents: List[Dict[str, Any]]
    relevant_ccp_chunks: List[Dict[str, Any]]

    # Media/attachments
    image_captions: List[str]

    # Outputs
    ai_reply: str
    confidence: float
    writeback_done: bool
    assembly_todo_written: bool

    # Control flags (computed from payload meta)
    ingest_only: bool
    media_only: bool
    force_reply: bool

    # Debug
    logs: List[str]