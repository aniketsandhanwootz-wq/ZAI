# service/app/tools/langchain_tools.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Callable
import base64
import traceback
from dataclasses import dataclass
from functools import lru_cache

from pydantic import BaseModel, Field

from langchain_core.tools import StructuredTool

from ..config import Settings
from .langsmith_trace import traceable_wrap

# --- Existing tools (unchanged internals) ---
from .sheets_tool import SheetsTool
from .drive_tool import DriveTool
from .vector_tool import VectorTool
from .db_tool import DBTool
from .embed_tool import EmbedTool
from .llm_tool import LLMTool
from .vision_tool import VisionTool
from .attachment_tool import AttachmentResolver, ResolvedAttachment, split_cell_refs
from .annotate_tool import AnnotateTool
from .company_tool import CompanyTool


# --------------------------
# Shared envelopes / helpers
# --------------------------

class ToolError(BaseModel):
    code: str = Field(..., description="Stable error code, e.g. 'HTTP_ERROR', 'NOT_FOUND', 'BAD_INPUT', 'INTERNAL'")
    message: str = Field(..., description="Human-readable error summary")
    details: Dict[str, Any] = Field(default_factory=dict, description="Optional structured details")

class ToolResponse(BaseModel):
    ok: bool
    result: Any = None
    error: Optional[ToolError] = None

class EmptyIn(BaseModel):
    pass
def _b64_encode(b: bytes) -> str:
    return base64.b64encode(b or b"").decode("utf-8")

def _b64_decode(s: str) -> bytes:
    if not s:
        return b""
    return base64.b64decode(s)

def _safe_call(fn: Callable[[], Any], *, code: str = "INTERNAL", name: str = "") -> Dict[str, Any]:
    """
    Never raises. Always returns ToolResponse dict.
    """
    try:
        out = fn()
        return ToolResponse(ok=True, result=out, error=None).model_dump()
    except Exception as e:
        tb = traceback.format_exc(limit=10)
        return ToolResponse(
            ok=False,
            result=None,
            error=ToolError(
                code=code,
                message=str(e) or f"{name} failed",
                details={"trace": tb, "tool": name},
            ),
        ).model_dump()


# --------------------------
# Registry wrapper (the “heart”)
# --------------------------

@dataclass(frozen=True)
class ToolRegistry:
    """
    A stable interface for nodes:
      registry.invoke("tool_name", {...}) -> ToolResponse dict
    This ensures:
      - a single consistent envelope
      - tool calls are traceable as LangSmith spans
      - nodes don’t import internal tool classes directly
    """
    tools: Dict[str, StructuredTool]

    def has(self, name: str) -> bool:
        return name in self.tools

    def get(self, name: str) -> StructuredTool:
        t = self.tools.get(name)
        if not t:
            raise KeyError(f"Unknown tool: {name}")
        return t

    def invoke(self, name: str, inp: Dict[str, Any]) -> Dict[str, Any]:
        """
        Always returns a single ToolResponse dict. Never raises.

        Important:
        - Many tool wrappers already return our {ok,result,error} envelope via _safe_call().
          In that case, we pass it through unchanged (no double-wrapping).
        - If a tool returns a raw value, we wrap it into ToolResponse(ok=True,...).
        - If tool.invoke raises, we return ToolResponse(ok=False,...).
        """
        def _do():
            tool = self.get(name)
            payload = inp if isinstance(inp, dict) else {"value": inp}
            return tool.invoke(payload)

        traced = traceable_wrap(lambda: _do(), name=f"zai.tool.{name}", run_type="tool")

        try:
            out = traced()

            # If wrapper already returned the envelope, do NOT wrap again.
            if isinstance(out, dict) and "ok" in out and ("result" in out or "error" in out):
                return out

            return ToolResponse(ok=True, result=out, error=None).model_dump()

        except Exception as e:
            tb = traceback.format_exc(limit=10)
            return ToolResponse(
                ok=False,
                result=None,
                error=ToolError(
                    code="TOOL_ERROR",
                    message=str(e) or f"{name} failed",
                    details={"trace": tb, "tool": name},
                ),
            ).model_dump()


# --------------------------
# Registry singleton (Settings-safe)
# --------------------------

_REGISTRY_CACHE: Dict[str, ToolRegistry] = {}

def _settings_cache_key(settings: Settings) -> str:
    """
    Settings objects (pydantic/BaseSettings) are typically unhashable, so lru_cache(settings)
    will crash. Use a stable string key derived from fields that affect tool behavior.
    Keep this conservative: if any of these change, tools should rebuild.
    """
    parts = [
        f"db={getattr(settings, 'database_url', '')}",
        f"sheet={getattr(settings, 'google_sheet_id', '')}",
        f"llm_provider={getattr(settings, 'llm_provider', '')}",
        f"llm_model={getattr(settings, 'llm_model', '')}",
        f"embed_provider={getattr(settings, 'embedding_provider', '')}",
        f"embed_model={getattr(settings, 'embedding_model', '')}",
        f"vision_provider={getattr(settings, 'vision_provider', '')}",
        f"vision_model={getattr(settings, 'vision_model', '')}",
        f"drive_root={getattr(settings, 'drive_root_folder_id', '')}",
    ]
    return "|".join(parts)

def get_tool_registry(settings: Settings) -> ToolRegistry:
    """
    Cached ToolRegistry keyed by a stable subset of settings.
    This avoids crashes from hashing Settings objects.
    """
    key = _settings_cache_key(settings)
    reg = _REGISTRY_CACHE.get(key)
    if reg:
        return reg
    reg = ToolRegistry(tools=build_langchain_tools(settings))
    _REGISTRY_CACHE[key] = reg
    return reg


def list_tool_names(settings: Settings) -> List[str]:
    """Convenience for debugging/contract validation."""
    return sorted(list(get_tool_registry(settings).tools.keys()))


def validate_required_tools(settings: Settings, required: Set[str]) -> Dict[str, Any]:
    """
    Validate that all required tool names exist.
    Returns ToolResponse envelope.
    """
    def _do():
        reg = get_tool_registry(settings)
        missing = sorted([n for n in (required or set()) if not reg.has(n)])
        return {"missing": missing, "present_count": len(reg.tools)}
    return _safe_call(_do, code="TOOL_CONTRACT_ERROR", name="tools.validate_required_tools")


# --------------------------
# SheetsTool wrappers
# --------------------------

class SheetsGetCheckinIn(BaseModel):
    checkin_id: str

class SheetsGetProjectByLegacyIdIn(BaseModel):
    legacy_id: str

class SheetsGetProjectRowTripletIn(BaseModel):
    project_name: str
    part_number: str
    legacy_id: str

class SheetsListProjectsIn(BaseModel):
    pass

class SheetsListCheckinsIn(BaseModel):
    pass

class SheetsGetConversationsIn(BaseModel):
    checkin_id: str

class SheetsAppendAICommentIn(BaseModel):
    checkin_id: str
    remark: str
    status: str = ""
    photos: str = ""
    conversation_id: Optional[str] = None
    added_by: str = "zai@wootz.work"
    timestamp: Optional[str] = None

class SheetsUpdateProjectCellIn(BaseModel):
    legacy_id: str
    column_name: str
    value: str

class SheetsListAdditionalPhotosIn(BaseModel):
    checkin_id: str
    tab_name: str

class SheetsResolveLegacyIdForGlideRowIn(BaseModel):
    row: Dict[str, Any]

class SheetsMapColIn(BaseModel):
    table: str
    field: str

def build_sheets_tools(settings: Settings) -> List[StructuredTool]:
    sheets = SheetsTool(settings)

    def get_checkin(inp: SheetsGetCheckinIn) -> Dict[str, Any]:
        return _safe_call(lambda: sheets.get_checkin_by_id(inp.checkin_id), code="SHEETS_ERROR", name="sheets.get_checkin_by_id")

    def get_project(inp: SheetsGetProjectByLegacyIdIn) -> Dict[str, Any]:
        return _safe_call(lambda: sheets.get_project_by_legacy_id(inp.legacy_id), code="SHEETS_ERROR", name="sheets.get_project_by_legacy_id")

    def get_project_triplet(inp: SheetsGetProjectRowTripletIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: sheets.get_project_row(inp.project_name, inp.part_number, inp.legacy_id),
            code="SHEETS_ERROR",
            name="sheets.get_project_row",
        )

    def list_projects(_: SheetsListProjectsIn) -> Dict[str, Any]:
        return _safe_call(lambda: sheets.list_projects(), code="SHEETS_ERROR", name="sheets.list_projects")

    def list_checkins(_: SheetsListCheckinsIn) -> Dict[str, Any]:
        return _safe_call(lambda: sheets.list_checkins(), code="SHEETS_ERROR", name="sheets.list_checkins")

    def list_ccp(_: EmptyIn) -> Dict[str, Any]:
        return _safe_call(lambda: sheets.list_ccp(), code="SHEETS_ERROR", name="sheets.list_ccp")

    def list_dashboard(_: EmptyIn) -> Dict[str, Any]:
        return _safe_call(lambda: sheets.list_dashboard_updates(), code="SHEETS_ERROR", name="sheets.list_dashboard_updates")

    def get_convos(inp: SheetsGetConversationsIn) -> Dict[str, Any]:
        return _safe_call(lambda: sheets.get_conversations_for_checkin(inp.checkin_id), code="SHEETS_ERROR", name="sheets.get_conversations_for_checkin")

    def append_ai(inp: SheetsAppendAICommentIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: sheets.append_conversation_ai_comment(
                inp.checkin_id,
                inp.remark,
                inp.status,
                inp.photos,
                conversation_id=inp.conversation_id,
                added_by=inp.added_by,
                timestamp=inp.timestamp,
            ),
            code="SHEETS_ERROR",
            name="sheets.append_conversation_ai_comment",
        )

    def update_project_cell(inp: SheetsUpdateProjectCellIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: sheets.update_project_cell_by_legacy_id(inp.legacy_id, column_name=inp.column_name, value=inp.value),
            code="SHEETS_ERROR",
            name="sheets.update_project_cell_by_legacy_id",
        )

    def list_additional_photos(inp: SheetsListAdditionalPhotosIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: sheets.list_additional_photos_for_checkin(inp.checkin_id, tab_name=inp.tab_name),
            code="SHEETS_ERROR",
            name="sheets.list_additional_photos_for_checkin",
        )

    def resolve_legacy(inp: SheetsResolveLegacyIdForGlideRowIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: sheets.resolve_legacy_id_for_glide_row(inp.row),
            code="SHEETS_ERROR",
            name="sheets.resolve_legacy_id_for_glide_row",
        )

    def map_col(inp: SheetsMapColIn) -> Dict[str, Any]:
        return _safe_call(lambda: sheets.map.col(inp.table, inp.field), code="SHEETS_ERROR", name="sheets.map.col")

    return [
        StructuredTool.from_function(name="sheets_get_checkin_by_id", description="Fetch a checkin row dict by checkin_id from Sheets.", args_schema=SheetsGetCheckinIn, func=get_checkin),
        StructuredTool.from_function(name="sheets_get_project_by_legacy_id", description="Fetch a project row dict by legacy_id from Sheets.", args_schema=SheetsGetProjectByLegacyIdIn, func=get_project),
        StructuredTool.from_function(name="sheets_get_project_row_triplet", description="Fetch a project row using (project_name, part_number, legacy_id).", args_schema=SheetsGetProjectRowTripletIn, func=get_project_triplet),
        StructuredTool.from_function(name="sheets_list_projects", description="List Project rows from Sheets.", args_schema=SheetsListProjectsIn, func=list_projects),
        StructuredTool.from_function(name="sheets_list_checkins", description="List Checkin rows from Sheets.", args_schema=SheetsListCheckinsIn, func=list_checkins),
        StructuredTool.from_function(name="sheets_list_ccp", description="List CCP rows from Sheets.", args_schema=EmptyIn, func=list_ccp),
        StructuredTool.from_function(name="sheets_list_dashboard_updates", description="List dashboard updates rows from Sheets.", args_schema=EmptyIn, func=list_dashboard),
        StructuredTool.from_function(name="sheets_get_conversations_for_checkin", description="List conversation rows filtered by checkin_id.", args_schema=SheetsGetConversationsIn, func=get_convos),
        StructuredTool.from_function(name="sheets_append_conversation_ai_comment", description="Append an AI comment into Conversation tab.", args_schema=SheetsAppendAICommentIn, func=append_ai),
        StructuredTool.from_function(name="sheets_update_project_cell_by_legacy_id", description="Update a single cell in Project tab using legacy_id.", args_schema=SheetsUpdateProjectCellIn, func=update_project_cell),
        StructuredTool.from_function(name="sheets_list_additional_photos_for_checkin", description="List additional photos rows for checkin from a provided tab name.", args_schema=SheetsListAdditionalPhotosIn, func=list_additional_photos),
        StructuredTool.from_function(name="sheets_resolve_legacy_id_for_glide_row", description="Resolve legacy_id for an arbitrary Glide row dict using Phase-0 rules.", args_schema=SheetsResolveLegacyIdForGlideRowIn, func=resolve_legacy),
        StructuredTool.from_function(name="sheets_map_col", description="Return the mapped sheet header for (table, field) using sheets_mapping.yaml.", args_schema=SheetsMapColIn, func=map_col),
    ]


# --------------------------
# DriveTool wrappers
# --------------------------

class DriveResolvePathIn(BaseModel):
    rel_path: str
    root_folder_id: Optional[str] = None

class DriveDownloadBytesIn(BaseModel):
    file_id: str

class DriveUploadBytesIn(BaseModel):
    folder_parts: List[str] = Field(default_factory=list)
    file_name: str
    content_b64: str
    mime_type: str
    make_public: bool = True
    root_folder_id: Optional[str] = None

class DriveUploadAnnotatedIn(BaseModel):
    checkin_id: str
    file_name: str
    content_b64: str
    mime_type: str = "image/png"
    make_public: bool = True

def build_drive_tools(settings: Settings) -> List[StructuredTool]:
    drive = DriveTool(settings)

    def resolve_path(inp: DriveResolvePathIn) -> Dict[str, Any]:
        def _do():
            item = drive.resolve_path(inp.rel_path, root_folder_id=inp.root_folder_id)
            if not item:
                return None
            return {"file_id": item.file_id, "name": item.name, "mime_type": item.mime_type, "parents": item.parents}
        return _safe_call(_do, code="DRIVE_ERROR", name="drive.resolve_path")

    def download_bytes(inp: DriveDownloadBytesIn) -> Dict[str, Any]:
        def _do():
            b = drive.download_file_bytes(inp.file_id) or b""
            return {"content_b64": _b64_encode(b), "byte_size": len(b)}
        return _safe_call(_do, code="DRIVE_ERROR", name="drive.download_file_bytes")

    def upload_bytes(inp: DriveUploadBytesIn) -> Dict[str, Any]:
        def _do():
            b = _b64_decode(inp.content_b64)
            return drive.upload_bytes_to_subpath(
                folder_parts=inp.folder_parts,
                file_name=inp.file_name,
                content_bytes=b,
                mime_type=inp.mime_type,
                make_public=inp.make_public,
                root_folder_id=inp.root_folder_id,
            )
        return _safe_call(_do, code="DRIVE_ERROR", name="drive.upload_bytes_to_subpath")

    def upload_annotated(inp: DriveUploadAnnotatedIn) -> Dict[str, Any]:
        def _do():
            b = _b64_decode(inp.content_b64)
            return drive.upload_annotated_bytes(
                checkin_id=inp.checkin_id,
                file_name=inp.file_name,
                content_bytes=b,
                mime_type=inp.mime_type,
                make_public=inp.make_public,
            )
        return _safe_call(_do, code="DRIVE_ERROR", name="drive.upload_annotated_bytes")

    return [
        StructuredTool.from_function(name="drive_resolve_path", description="Resolve a relative Drive path to a file item.", args_schema=DriveResolvePathIn, func=resolve_path),
        StructuredTool.from_function(name="drive_download_file_bytes", description="Download Drive file content as base64.", args_schema=DriveDownloadBytesIn, func=download_bytes),
        StructuredTool.from_function(name="drive_upload_bytes_to_subpath", description="Upload bytes (base64) to Drive under folder_parts.", args_schema=DriveUploadBytesIn, func=upload_bytes),
        StructuredTool.from_function(name="drive_upload_annotated_bytes", description="Upload annotated image bytes (base64) under annotated root.", args_schema=DriveUploadAnnotatedIn, func=upload_annotated),
    ]


# --------------------------
# VectorTool wrappers
# --------------------------

class VectorSearchIncidentsIn(BaseModel):
    tenant_id: str
    query_embedding: List[float]
    top_k: int = 30
    project_name: Optional[str] = None
    part_number: Optional[str] = None
    legacy_id: Optional[str] = None
    vector_type: Optional[str] = None

class VectorSearchCCPIn(BaseModel):
    tenant_id: str
    query_embedding: List[float]
    top_k: int = 30
    project_name: Optional[str] = None
    part_number: Optional[str] = None
    legacy_id: Optional[str] = None

class VectorSearchDashboardIn(BaseModel):
    tenant_id: str
    query_embedding: List[float]
    top_k: int = 20
    project_name: Optional[str] = None
    part_number: Optional[str] = None
    legacy_id: Optional[str] = None

class VectorSearchGlideKBIn(BaseModel):
    tenant_id: str
    query_embedding: List[float]
    top_k: int = 30
    project_name: Optional[str] = None
    part_number: Optional[str] = None
    legacy_id: Optional[str] = None
    table_names: Optional[List[str]] = None

class VectorUpsertIncidentIn(BaseModel):
    tenant_id: str
    checkin_id: str
    vector_type: str
    embedding: List[float]
    project_name: Optional[str] = None
    part_number: Optional[str] = None
    legacy_id: Optional[str] = None
    status: str = ""
    text: str = ""

class VectorUpsertCompanyProfileIn(BaseModel):
    tenant_row_id: str
    company_name: str
    company_description: str
    embedding: List[float]

def build_vector_tools(settings: Settings) -> List[StructuredTool]:
    vt = VectorTool(settings)

    def search_incidents(inp: VectorSearchIncidentsIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: vt.search_incidents(
                tenant_id=inp.tenant_id,
                query_embedding=inp.query_embedding,
                top_k=inp.top_k,
                project_name=inp.project_name,
                part_number=inp.part_number,
                legacy_id=inp.legacy_id,
                vector_type=inp.vector_type,
            ),
            code="DB_ERROR",
            name="vector.search_incidents",
        )

    def search_ccp(inp: VectorSearchCCPIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: vt.search_ccp_chunks(
                tenant_id=inp.tenant_id,
                query_embedding=inp.query_embedding,
                top_k=inp.top_k,
                project_name=inp.project_name,
                part_number=inp.part_number,
                legacy_id=inp.legacy_id,
            ),
            code="DB_ERROR",
            name="vector.search_ccp_chunks",
        )

    def search_dashboard(inp: VectorSearchDashboardIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: vt.search_dashboard_updates(
                tenant_id=inp.tenant_id,
                query_embedding=inp.query_embedding,
                top_k=inp.top_k,
                project_name=inp.project_name,
                part_number=inp.part_number,
                legacy_id=inp.legacy_id,
            ),
            code="DB_ERROR",
            name="vector.search_dashboard_updates",
        )

    def search_glide_kb(inp: VectorSearchGlideKBIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: vt.search_glide_kb_chunks(
                tenant_id=inp.tenant_id,
                query_embedding=inp.query_embedding,
                top_k=inp.top_k,
                project_name=inp.project_name,
                part_number=inp.part_number,
                legacy_id=inp.legacy_id,
                table_names=inp.table_names,
            ),
            code="DB_ERROR",
            name="vector.search_glide_kb_chunks",
        )

    def upsert_incident(inp: VectorUpsertIncidentIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: vt.upsert_incident_vector(
                tenant_id=inp.tenant_id,
                checkin_id=inp.checkin_id,
                vector_type=inp.vector_type,
                embedding=inp.embedding,
                project_name=inp.project_name,
                part_number=inp.part_number,
                legacy_id=inp.legacy_id,
                status=inp.status,
                text=inp.text,
            ),
            code="DB_ERROR",
            name="vector.upsert_incident_vector",
        )

    def upsert_company(inp: VectorUpsertCompanyProfileIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: vt.upsert_company_profile(
                tenant_row_id=inp.tenant_row_id,
                company_name=inp.company_name,
                company_description=inp.company_description,
                embedding=inp.embedding,
            ),
            code="DB_ERROR",
            name="vector.upsert_company_profile",
        )

    return [
        StructuredTool.from_function(name="vector_search_incidents", description="Vector search in incident_vectors.", args_schema=VectorSearchIncidentsIn, func=search_incidents),
        StructuredTool.from_function(name="vector_search_ccp_chunks", description="Vector search in ccp_vectors.", args_schema=VectorSearchCCPIn, func=search_ccp),
        StructuredTool.from_function(name="vector_search_dashboard_updates", description="Vector search in dashboard_vectors.", args_schema=VectorSearchDashboardIn, func=search_dashboard),
        StructuredTool.from_function(name="vector_search_glide_kb_chunks", description="Vector search in glide_kb_vectors joined with metadata.", args_schema=VectorSearchGlideKBIn, func=search_glide_kb),
        StructuredTool.from_function(name="vector_upsert_incident_vector", description="Upsert an incident vector (PROBLEM/RESOLUTION/MEDIA).", args_schema=VectorUpsertIncidentIn, func=upsert_incident),
        StructuredTool.from_function(name="vector_upsert_company_profile", description="Upsert a company profile vector.", args_schema=VectorUpsertCompanyProfileIn, func=upsert_company),
    ]


# --------------------------
# DBTool wrappers
# --------------------------

class DBExistingArtifactHashesIn(BaseModel):
    tenant_id: str
    checkin_id: str
    artifact_type: str

class DBGetArtifactByHashIn(BaseModel):
    tenant_id: str
    checkin_id: str
    artifact_type: str
    source_hash: str

class DBCheckinFileBriefsIn(BaseModel):
    tenant_id: str
    checkin_id: str
    max_items: int = 6

class DBImageCaptionsIn(BaseModel):
    tenant_id: str
    checkin_id: str

def build_db_tools(settings: Settings) -> List[StructuredTool]:
    db = DBTool(settings.database_url)

    def existing_hashes(inp: DBExistingArtifactHashesIn) -> Dict[str, Any]:
        def _do():
            s: Set[str] = db.existing_artifact_source_hashes(
                tenant_id=inp.tenant_id, checkin_id=inp.checkin_id, artifact_type=inp.artifact_type
            )
            return {"hashes": sorted(list(s))}
        return _safe_call(_do, code="DB_ERROR", name="db.existing_artifact_source_hashes")

    def get_artifact(inp: DBGetArtifactByHashIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: db.get_artifact_url_and_meta_by_source_hash(
                tenant_id=inp.tenant_id,
                checkin_id=inp.checkin_id,
                artifact_type=inp.artifact_type,
                source_hash=inp.source_hash,
            ),
            code="DB_ERROR",
            name="db.get_artifact_url_and_meta_by_source_hash",
        )

    def checkin_file_briefs(inp: DBCheckinFileBriefsIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: db.get_checkin_file_briefs(tenant_id=inp.tenant_id, checkin_id=inp.checkin_id, max_items=inp.max_items),
            code="DB_ERROR",
            name="db.get_checkin_file_briefs",
        )

    def image_captions(inp: DBImageCaptionsIn) -> Dict[str, Any]:
        return _safe_call(
            lambda: db.image_captions_by_hash(tenant_id=inp.tenant_id, checkin_id=inp.checkin_id),
            code="DB_ERROR",
            name="db.image_captions_by_hash",
        )

    return [
        StructuredTool.from_function(name="db_existing_artifact_source_hashes", description="Get already-seen artifact source_hashes for tenant+checkin+type.", args_schema=DBExistingArtifactHashesIn, func=existing_hashes),
        StructuredTool.from_function(name="db_get_artifact_url_and_meta_by_source_hash", description="Get (url, meta) for artifact (latest) by source_hash.", args_schema=DBGetArtifactByHashIn, func=get_artifact),
        StructuredTool.from_function(name="db_get_checkin_file_briefs", description="Get brief checkin_file_artifacts for prompt context.", args_schema=DBCheckinFileBriefsIn, func=checkin_file_briefs),
        StructuredTool.from_function(name="db_image_captions_by_hash", description="Get cached image captions keyed by source_hash.", args_schema=DBImageCaptionsIn, func=image_captions),
    ]


# --------------------------
# EmbedTool wrappers
# --------------------------

class EmbedTextIn(BaseModel):
    text: str

def build_embed_tools(settings: Settings) -> List[StructuredTool]:
    emb = EmbedTool(settings)

    def embed_text(inp: EmbedTextIn) -> Dict[str, Any]:
        return _safe_call(lambda: emb.embed_text(inp.text), code="EMBED_ERROR", name="embed.embed_text")

    def embed_query(inp: EmbedTextIn) -> Dict[str, Any]:
        return _safe_call(lambda: emb.embed_query(inp.text), code="EMBED_ERROR", name="embed.embed_query")

    return [
        StructuredTool.from_function(name="embed_text", description="Create embedding for document storage.", args_schema=EmbedTextIn, func=embed_text),
        StructuredTool.from_function(name="embed_query", description="Create embedding for retrieval query.", args_schema=EmbedTextIn, func=embed_query),
    ]


# --------------------------
# LLMTool wrappers
# --------------------------

class LLMGenerateTextIn(BaseModel):
    prompt: str

class LLMImageItem(BaseModel):
    image_index: int
    mime_type: str = "image/jpeg"
    image_b64: str

class LLMGenerateJSONWithImagesIn(BaseModel):
    prompt: str
    images: List[LLMImageItem] = Field(default_factory=list)
    temperature: float = 0.0

def build_llm_tools(settings: Settings) -> List[StructuredTool]:
    llm = LLMTool(settings)

    def gen_text(inp: LLMGenerateTextIn) -> Dict[str, Any]:
        return _safe_call(lambda: llm.generate_text(inp.prompt), code="LLM_ERROR", name="llm.generate_text")

    def gen_json(inp: LLMGenerateJSONWithImagesIn) -> Dict[str, Any]:
        def _do():
            imgs = []
            for it in inp.images or []:
                b = _b64_decode(it.image_b64)
                if not b:
                    continue
                imgs.append({"image_index": int(it.image_index), "mime_type": it.mime_type, "image_bytes": b})
            return llm.generate_json_with_images(prompt=inp.prompt, images=imgs, temperature=float(inp.temperature))
        return _safe_call(_do, code="LLM_ERROR", name="llm.generate_json_with_images")

    return [
        StructuredTool.from_function(name="llm_generate_text", description="Generate plain text response from LLM.", args_schema=LLMGenerateTextIn, func=gen_text),
        StructuredTool.from_function(name="llm_generate_json_with_images", description="Generate JSON from multimodal LLM using inline images.", args_schema=LLMGenerateJSONWithImagesIn, func=gen_json),
    ]


# --------------------------
# VisionTool wrappers
# --------------------------

class VisionCaptionIn(BaseModel):
    image_b64: str
    mime_type: str = "image/jpeg"
    context_hint: str = ""
    model: Optional[str] = None

def build_vision_tools(settings: Settings) -> List[StructuredTool]:
    vis = VisionTool(settings)

    def caption(inp: VisionCaptionIn) -> Dict[str, Any]:
        def _do():
            b = _b64_decode(inp.image_b64)
            return vis.caption_for_retrieval(
                image_bytes=b,
                mime_type=inp.mime_type,
                context_hint=inp.context_hint,
                model=inp.model,
            )
        return _safe_call(_do, code="VISION_ERROR", name="vision.caption_for_retrieval")

    return [
        StructuredTool.from_function(name="vision_caption_for_retrieval", description="Create a 6-line retrieval caption for an image (no defect detection).", args_schema=VisionCaptionIn, func=caption),
    ]


# --------------------------
# AttachmentResolver wrappers
# --------------------------

class AttachmentResolveIn(BaseModel):
    ref: str

class AttachmentFetchIn(BaseModel):
    source_ref: str
    kind: str
    name: str
    mime_type: str = ""
    is_pdf: bool = False
    is_image: bool = False
    drive_file_id: Optional[str] = None
    direct_url: Optional[str] = None
    rel_path: Optional[str] = None
    timeout: int = 40
    max_bytes: int = 15_000_000

class SplitCellRefsIn(BaseModel):
    cell: str

def build_attachment_tools(settings: Settings) -> List[StructuredTool]:
    drive = DriveTool(settings)
    resolver = AttachmentResolver(drive)

    def resolve(inp: AttachmentResolveIn) -> Dict[str, Any]:
        def _do():
            att = resolver.resolve(inp.ref)
            if not att:
                return None
            return {
                "source_ref": att.source_ref,
                "kind": att.kind,
                "name": att.name,
                "mime_type": att.mime_type,
                "is_pdf": bool(att.is_pdf),
                "is_image": bool(att.is_image),
                "drive_file_id": att.drive_file_id,
                "direct_url": att.direct_url,
                "rel_path": att.rel_path,
            }
        return _safe_call(_do, code="ATTACHMENT_ERROR", name="attachment.resolve")

    def fetch(inp: AttachmentFetchIn) -> Dict[str, Any]:
        def _do():
            att = ResolvedAttachment(
                source_ref=inp.source_ref,
                kind=inp.kind,
                name=inp.name,
                mime_type=inp.mime_type,
                is_pdf=bool(inp.is_pdf),
                is_image=bool(inp.is_image),
                drive_file_id=inp.drive_file_id,
                direct_url=inp.direct_url,
                rel_path=inp.rel_path,
            )
            b = resolver.fetch_bytes(att, timeout=int(inp.timeout), max_bytes=int(inp.max_bytes)) or b""
            return {"content_b64": _b64_encode(b), "byte_size": len(b)}
        return _safe_call(_do, code="ATTACHMENT_ERROR", name="attachment.fetch_bytes")

    def split_refs(inp: SplitCellRefsIn) -> Dict[str, Any]:
        return _safe_call(lambda: split_cell_refs(inp.cell), code="ATTACHMENT_ERROR", name="attachment.split_cell_refs")

    return [
        StructuredTool.from_function(name="attachment_resolve", description="Resolve a cell ref (url/drive path) into a structured attachment descriptor.", args_schema=AttachmentResolveIn, func=resolve),
        StructuredTool.from_function(name="attachment_fetch_bytes", description="Fetch attachment bytes as base64 from resolved attachment descriptor.", args_schema=AttachmentFetchIn, func=fetch),
        StructuredTool.from_function(name="attachment_split_cell_refs", description="Split a sheet cell containing multiple attachment refs.", args_schema=SplitCellRefsIn, func=split_refs),
    ]


# --------------------------
# AnnotateTool wrappers
# --------------------------

class AnnotateDrawIn(BaseModel):
    image_b64: str
    boxes: List[Dict[str, Any]] = Field(default_factory=list)
    out_format: str = "PNG"

def build_annotate_tools() -> List[StructuredTool]:
    ann = AnnotateTool()

    def draw(inp: AnnotateDrawIn) -> Dict[str, Any]:
        def _do():
            b = _b64_decode(inp.image_b64)
            out = ann.draw(b, inp.boxes, out_format=inp.out_format)
            return {"image_b64": _b64_encode(out), "byte_size": len(out)}
        return _safe_call(_do, code="ANNOTATE_ERROR", name="annotate.draw")

    return [
        StructuredTool.from_function(name="annotate_draw", description="Draw normalized bounding boxes on an image and return annotated image as base64.", args_schema=AnnotateDrawIn, func=draw),
    ]


# --------------------------
# CompanyTool wrappers
# --------------------------

class CompanyGetContextIn(BaseModel):
    tenant_row_id: str

class CompanyFromProjectNameIn(BaseModel):
    project_name: str
    tenant_row_id: str = ""

def build_company_tools(settings: Settings) -> List[StructuredTool]:
    ct = CompanyTool(settings)

    def get_ctx(inp: CompanyGetContextIn) -> Dict[str, Any]:
        def _do():
            c = ct.get_company_context(inp.tenant_row_id)
            if not c:
                return None
            return {
                "tenant_row_id": c.tenant_row_id,
                "company_key": c.company_key,
                "company_name": c.company_name,
                "company_description": c.company_description,
            }
        return _safe_call(_do, code="COMPANY_ERROR", name="company.get_company_context")

    def from_proj(inp: CompanyFromProjectNameIn) -> Dict[str, Any]:
        def _do():
            c = ct.from_project_name(inp.project_name, tenant_row_id=inp.tenant_row_id)
            if not c:
                return None
            return {
                "tenant_row_id": c.tenant_row_id,
                "company_key": c.company_key,
                "company_name": c.company_name,
                "company_description": c.company_description,
            }
        return _safe_call(_do, code="COMPANY_ERROR", name="company.from_project_name")

    return [
        StructuredTool.from_function(name="company_get_company_context", description="Get company context for a tenant_row_id (cache->glide).", args_schema=CompanyGetContextIn, func=get_ctx),
        StructuredTool.from_function(name="company_from_project_name", description="Derive company context from project name.", args_schema=CompanyFromProjectNameIn, func=from_proj),
    ]


# --------------------------
# Toolkit builder (stable API)
# --------------------------

def build_langchain_tools(settings: Settings) -> Dict[str, StructuredTool]:
    """
    Single entrypoint to get all LC tools.
    Returns mapping tool_name -> StructuredTool

    IMPORTANT:
    - Tool names are now the canonical interface. Nodes should call by name via ToolRegistry.
    - All tools return ToolResponse dict via our wrappers, so tool.invoke never leaks exceptions upstream.
    """
    tools: List[StructuredTool] = []
    tools += build_sheets_tools(settings)
    tools += build_drive_tools(settings)
    tools += build_vector_tools(settings)
    tools += build_db_tools(settings)
    tools += build_embed_tools(settings)
    tools += build_llm_tools(settings)
    tools += build_vision_tools(settings)
    tools += build_attachment_tools(settings)
    tools += build_annotate_tools()
    tools += build_company_tools(settings)
    return {t.name: t for t in tools}