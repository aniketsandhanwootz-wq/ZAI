# service/app/tools/attachment_tool.py
# Tool to resolve and fetch attachments from various sources.
# Supports:
#   - direct URLs (http/https)
#   - Google Drive URLs and relative paths
#   - bare Drive file IDs
#   - identifies mime types and whether the attachment is an image or PDF
#   - fetches bytes via requests or Drive API
#   - splits cell references with multiple attachments
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import re
import mimetypes
import requests

from .drive_tool import DriveTool, DriveItem


_DRIVE_ID_PATTERNS = [
    re.compile(r"drive\.google\.com/file/d/([a-zA-Z0-9_-]+)"),
    re.compile(r"[?&]id=([a-zA-Z0-9_-]+)"),
]

_DRIVE_BARE_ID_RX = re.compile(r"^[a-zA-Z0-9_-]{10,}$")
_EMAIL_RX = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)


def _looks_like_url(s: str) -> bool:
    return bool(re.match(r"^https?://", (s or "").strip(), re.I))


def _is_bare_drive_id(s: str) -> bool:
    t = (s or "").strip()
    if not t:
        return False
    if _EMAIL_RX.match(t):
        return False
    if t.startswith("<<") and t.endswith(">>"):
        return False
    if "<" in t or ">" in t:
        return False
    return bool(_DRIVE_BARE_ID_RX.match(t))


def _is_garbage_ref(s: str) -> bool:
    t = (s or "").strip()
    if not t:
        return True

    low = t.lower()

    # common AppSheet garbage
    if low.startswith("unable to load image data"):
        return True

    # plain emails should never be treated as attachments
    if _EMAIL_RX.match(t):
        return True

    # lines that are not url and not a path but contain #filename=
    if "#filename=" in low and not _looks_like_url(t) and "/" not in t and not _is_bare_drive_id(t):
        return True

    return False


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
      - Drive URLs -> extract file id -> download via Drive API
      - bare Drive file IDs -> download via Drive API
      - Drive relative paths under configured roots -> resolve via DriveTool
    """

    def __init__(self, drive: DriveTool):
        self.drive = drive

    def resolve(self, ref: str) -> Optional[ResolvedAttachment]:
        raw = (ref or "").strip()
        if not raw or _is_garbage_ref(raw):
            return None

        # Case 0: bare drive file id
        if _is_bare_drive_id(raw) and not _looks_like_url(raw) and "/" not in raw:
            return ResolvedAttachment(
                source_ref=raw,
                kind="drive_id",
                name=raw,
                mime_type="",
                is_pdf=False,
                is_image=False,
                drive_file_id=raw,
            )

        # Case 1: URL
        if _looks_like_url(raw):
            did = _extract_drive_id(raw)
            if did:
                # drive share links don't carry extension/mime reliably
                return ResolvedAttachment(
                    source_ref=raw,
                    kind="drive_id",
                    name=did,
                    mime_type="",
                    is_pdf=False,
                    is_image=False,
                    drive_file_id=did,
                )

            name = raw.split("/")[-1] if "/" in raw else raw
            mt = _guess_mime_from_name(name)
            low = name.lower()
            is_pdf = low.endswith(".pdf") or mt == "application/pdf"
            is_img = (mt.startswith("image/") if mt else False) or low.endswith((".png", ".jpg", ".jpeg", ".webp"))

            return ResolvedAttachment(
                source_ref=raw,
                kind="url",
                name=name,
                mime_type=mt,
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

    def fetch_bytes(
        self,
        att: ResolvedAttachment,
        *,
        timeout: int = 40,
        max_bytes: int = 15_000_000,
    ) -> Optional[bytes]:
        if not att:
            return None

        # 1) Direct URL bytes (streaming + max size safety)
        if att.direct_url:
            try:
                with requests.get(att.direct_url, timeout=timeout, stream=True) as r:
                    r.raise_for_status()
                    buf = bytearray()
                    for chunk in r.iter_content(chunk_size=256 * 1024):
                        if not chunk:
                            continue
                        buf.extend(chunk)
                        if len(buf) > max_bytes:
                            return None
                    return bytes(buf)
            except Exception:
                return None

        # 2) Drive bytes
        if att.drive_file_id:
            return self.drive.download_file_bytes(att.drive_file_id)

        return None


def split_cell_refs(cell: str) -> list[str]:
    """
    Sheet cells can contain:
      - comma separated
      - newline separated
      - semicolon separated
      - mixed

    We filter garbage lines (emails / "Unable to load image data..." etc.)
    We accept:
      - URLs
      - drive-ish relative paths (contains '/')
      - bare drive file ids
    """
    s = (cell or "").strip()
    if not s:
        return []

    s = s.replace("\r", "\n")
    s = s.replace("\n", ",")
    s = s.replace(";", ",")

    parts = [p.strip() for p in s.split(",")]
    out: list[str] = []

    for p in parts:
        if not p:
            continue
        if _is_garbage_ref(p):
            continue

        if _looks_like_url(p) or ("/" in p) or _is_bare_drive_id(p):
            out.append(p)

    return out