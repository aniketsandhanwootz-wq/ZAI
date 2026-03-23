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
    dispatch_date: Optional[str] = None
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
    retrieval_query_text: Optional[str] = None

    # Attachments (Files column)
    attachment_context: Optional[str] = None
    attachments_analyzed: List[Dict[str, Any]] = field(default_factory=list)

    # Retrieval results (expanded)
    similar_incidents: List[Dict[str, Any]] = field(default_factory=list)
    similar_problems: List[Dict[str, Any]] = field(default_factory=list)
    similar_resolutions: List[Dict[str, Any]] = field(default_factory=list)
    similar_media: List[Dict[str, Any]] = field(default_factory=list)
    relevant_ccp_chunks: List[Dict[str, Any]] = field(default_factory=list)
    relevant_dashboard_updates: List[Dict[str, Any]] = field(default_factory=list)
    relevant_glide_kb_chunks: List[Dict[str, Any]] = field(default_factory=list)

    # Packed prompt context + evidence index
    packed_context: Optional[str] = None
    evidence_index: List[Dict[str, Any]] = field(default_factory=list)
    evidence_index_text: Optional[str] = None

    # LLM output
    ai_reply: Optional[str] = None
    is_critical: Optional[bool] = None
    confidence: Optional[float] = None
    defects_by_image: List[Dict[str, Any]] = field(default_factory=list)

    # NEW: reply grounding outputs
    reply_citations: List[Dict[str, Any]] = field(default_factory=list)
    edge_tab_refs: List[Dict[str, Any]] = field(default_factory=list)

    # Writeback
    writeback_done: bool = False

    # Debug
    logs: List[str] = field(default_factory=list)
