from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple
import re
import mimetypes
import requests

from .drive_tool import DriveTool, DriveItem


_DRIVE_ID_PATTERNS = [
    re.compile(r"drive\.google\.com/file/d/([a-zA-Z0-9_-]+)"),
    re.compile(r"[?&]id=([a-zA-Z0-9_-]+)"),
]


def _looks_like_url(s: str) -> bool:
    return bool(re.match(r"^https?://", (s or "").strip(), re.I))


def _extract_drive_id(url: str) -> Optional[str]:
    u = (url or "").strip()
    for rx in _DRIVE_ID_PATTERNS:
        m = rx.search(u)
        if m:
            return m.group(1)
    return None


def _guess_mime_from_name(name: str) -> str:
    mt, _ = mimetypes.guess_type(name or "")
    return mt or ""


@dataclass
class ResolvedAttachment:
    source_ref: str                 # original cell value
    kind: str                       # "url" | "drive_path" | "drive_id" | "unknown"
    name: str
    mime_type: str
    is_pdf: bool
    is_image: bool

    # bytes loader
    drive_file_id: Optional[str] = None
    direct_url: Optional[str] = None
    rel_path: Optional[str] = None


class AttachmentResolver:
    """
    Resolves:
      - direct URLs (http/https) -> download via requests
      - Drive URLs -> extract file id (best-effort) -> download via Drive API later (optional)
      - Drive relative paths under GOOGLE_DRIVE_ROOT_FOLDER_ID -> resolve via DriveTool
    """

    def __init__(self, drive: DriveTool):
        self.drive = drive

    def resolve(self, ref: str) -> Optional[ResolvedAttachment]:
        raw = (ref or "").strip()
        if not raw:
            return None

        # Case 1: URL
        if _looks_like_url(raw):
            did = _extract_drive_id(raw)
            if did:
                return ResolvedAttachment(
                    source_ref=raw,
                    kind="drive_id",
                    name=did,
                    mime_type="",
                    is_pdf=False,
                    is_image=False,
                    drive_file_id=did,
                )

            mime = ""
            name = raw.split("/")[-1] if "/" in raw else raw
            mt = _guess_mime_from_name(name)
            if mt:
                mime = mt

            low = name.lower()
            is_pdf = low.endswith(".pdf") or mime == "application/pdf"
            is_img = (mime.startswith("image/") if mime else False) or low.endswith((".png", ".jpg", ".jpeg", ".webp"))

            return ResolvedAttachment(
                source_ref=raw,
                kind="url",
                name=name,
                mime_type=mime,
                is_pdf=is_pdf,
                is_image=is_img,
                direct_url=raw,
            )

        # Case 2: drive relative path (supports PREFIX/... with prefix->folderId)
        rel = raw.strip().strip("/")
        root_override = None

        if "/" in rel:
            prefix = rel.split("/", 1)[0].strip().strip("/")
            rest = rel.split("/", 1)[1].strip().lstrip("/")
            fid = self.drive.get_root_for_prefix(prefix)
            if fid:
                root_override = fid
                rel = rest
            else:
                # prefix present in path but not configured => skip for now (your requirement)
                name = raw.split("/")[-1] if "/" in raw else raw
                mt = _guess_mime_from_name(name)
                low = name.lower()
                is_pdf = low.endswith(".pdf") or mt == "application/pdf"
                is_img = (mt.startswith("image/") if mt else False) or low.endswith((".png", ".jpg", ".jpeg", ".webp"))
                return ResolvedAttachment(
                    source_ref=raw,
                    kind="unknown",
                    name=name,
                    mime_type=mt,
                    is_pdf=is_pdf,
                    is_image=is_img,
                    rel_path=raw,
                )

        item: Optional[DriveItem] = self.drive.resolve_path(rel, root_folder_id=root_override)
        if not item:
            name = raw.split("/")[-1] if "/" in raw else raw
            mt = _guess_mime_from_name(name)
            low = name.lower()
            is_pdf = low.endswith(".pdf") or mt == "application/pdf"
            is_img = (mt.startswith("image/") if mt else False) or low.endswith((".png", ".jpg", ".jpeg", ".webp"))
            return ResolvedAttachment(
                source_ref=raw,
                kind="unknown",
                name=name,
                mime_type=mt,
                is_pdf=is_pdf,
                is_image=is_img,
                rel_path=raw,
            )

        name = item.name
        mime = item.mime_type or _guess_mime_from_name(name)
        low = name.lower()
        is_pdf = low.endswith(".pdf") or mime == "application/pdf"
        is_img = mime.startswith("image/") or low.endswith((".png", ".jpg", ".jpeg", ".webp"))

        return ResolvedAttachment(
            source_ref=raw,
            kind="drive_path",
            name=name,
            mime_type=mime,
            is_pdf=is_pdf,
            is_image=is_img,
            drive_file_id=item.file_id,
            rel_path=raw,
        )
    def fetch_bytes(self, att: ResolvedAttachment, *, timeout: int = 40) -> Optional[bytes]:
        if not att:
            return None

        # URL bytes
        if att.direct_url:
            try:
                r = requests.get(att.direct_url, timeout=timeout)
                r.raise_for_status()
                return r.content
            except Exception:
                return None

        # Drive bytes
        if att.drive_file_id:
            return self.drive.download_file_bytes(att.drive_file_id)

        return None


def split_cell_refs(cell: str) -> list[str]:
    """
    Sheet cells can contain:
      - comma separated
      - newline separated
      - mixed
    """
    s = (cell or "").strip()
    if not s:
        return []
    s = s.replace("\r", "\n")
    s = s.replace("\n", ",")
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]
