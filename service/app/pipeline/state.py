from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class GraphState:
    # Input
    event: Dict[str, Any]

    # Derived IDs
    event_type: str = ""
    tenant_id: Optional[str] = None
    project_name: Optional[str] = None
    part_number: Optional[str] = None
    legacy_id: Optional[str] = None
    checkin_id: Optional[str] = None
    conversation_id: Optional[str] = None
    ccp_id: Optional[str] = None

    # Run tracking
    run_id: Optional[str] = None

    # Loaded sheet data
    checkin_row: Optional[Dict[str, Any]] = None
    project_row: Optional[Dict[str, Any]] = None
    conversation_rows: List[Dict[str, Any]] = field(default_factory=list)
    ccp_row: Optional[Dict[str, Any]] = None

    # Prepared texts
    thread_snapshot_text: Optional[str] = None

    # Retrieval results
    similar_incidents: List[Dict[str, Any]] = field(default_factory=list)
    relevant_ccp_chunks: List[Dict[str, Any]] = field(default_factory=list)

    # LLM output
    ai_reply: Optional[str] = None
    confidence: Optional[float] = None

    # Writeback
    writeback_done: bool = False

    # Debug
    logs: List[str] = field(default_factory=list)
