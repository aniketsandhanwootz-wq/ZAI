from typing import Dict, Any

from langgraph.graph import StateGraph, END

from ..config import Settings
from .state import GraphState
from .nodes.load_sheet_data import load_sheet_data
from .nodes.build_thread_snapshot import build_thread_snapshot
from .nodes.upsert_vectors import upsert_vectors
from .nodes.retrieve_context import retrieve_context
from .nodes.generate_ai_reply import generate_ai_reply
from .nodes.writeback import writeback


def _route_by_event(state: GraphState) -> str:
    # For now, everything goes through the same “checkin pipeline”.
    # Later we can branch: CCP_UPDATED -> ccp_ingest flow etc.
    et = state.event_type
    if et in ("CCP_UPDATED",):
        return "ccp_path"  # placeholder
    return "checkin_path"


def build_graph() -> Any:
    g = StateGraph(GraphState)

    g.add_node("load_sheet_data", load_sheet_data)
    g.add_node("build_thread_snapshot", build_thread_snapshot)
    g.add_node("upsert_vectors", upsert_vectors)
    g.add_node("retrieve_context", retrieve_context)
    g.add_node("generate_ai_reply", generate_ai_reply)
    g.add_node("writeback", writeback)

    # main path
    g.set_entry_point("load_sheet_data")
    g.add_edge("load_sheet_data", "build_thread_snapshot")
    g.add_edge("build_thread_snapshot", "upsert_vectors")
    g.add_edge("upsert_vectors", "retrieve_context")
    g.add_edge("retrieve_context", "generate_ai_reply")
    g.add_edge("generate_ai_reply", "writeback")
    g.add_edge("writeback", END)

    return g.compile()


_GRAPH = build_graph()


def run_event_graph(settings: Settings, payload: Dict[str, Any]) -> Dict[str, Any]:
    state = GraphState(event=payload)
    result_state: GraphState = _GRAPH.invoke(state, {"settings": settings})  # type: ignore
    return {
        "event_type": result_state.event_type,
        "tenant_id": result_state.tenant_id,
        "checkin_id": result_state.checkin_id,
        "ai_reply": result_state.ai_reply,
        "writeback_done": result_state.writeback_done,
        "logs": result_state.logs[-30:],
    }
