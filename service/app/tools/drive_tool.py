from __future__ import annotations

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from io import BytesIO
import logging
import re

import os
import json

from google.oauth2.credentials import Credentials as OAuthCredentials
from google.auth.transport.requests import Request

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from ..config import Settings



DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]
logger = logging.getLogger("zai.drive")

_DRIVE_ID_RX = re.compile(r"^[a-zA-Z0-9_-]{10,}$")


def _is_valid_drive_id(v: str) -> bool:
    s = (v or "").strip()
    if not s:
        return False
    if s.startswith("<<") and s.endswith(">>"):
        return False
    if "folderId" in s or "<" in s or ">" in s:
        return False
    return bool(_DRIVE_ID_RX.match(s))


@dataclass
class DriveItem:
    file_id: str
    name: str
    mime_type: str
    parents: List[str]


class DriveTool:
    """
    Drive helper:
      - resolve a relative path under a root folder
      - download file bytes
      - upload bytes to a subfolder path
      - (optional) make public + return webViewLink
    """

    def __init__(self, settings: Settings):
        self.settings = settings

        token_raw = (os.getenv("DRIVE_TOKEN_JSON", "") or "").strip()
        if not token_raw:
            raise RuntimeError(
                "Missing DRIVE_TOKEN_JSON. Generate it using service/scripts/gen_drive_token.py "
                "and set it in Render env vars."
            )

        try:
            token_info = json.loads(token_raw)
        except Exception as e:
            raise RuntimeError("DRIVE_TOKEN_JSON is not valid JSON") from e

        creds = OAuthCredentials.from_authorized_user_info(token_info, scopes=DRIVE_SCOPES)

        # Ensure token is usable (refresh if needed)
        if not creds.valid:
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                raise RuntimeError(
                    "DRIVE_TOKEN_JSON credentials are not valid and not refreshable "
                    "(missing refresh_token or expired). Regenerate token."
                )

        self._svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        self._creds = creds

        self.root_folder_id = (getattr(settings, "google_drive_root_folder_id", "") or "").strip()
        self.annotated_root_folder_id = (getattr(settings, "google_drive_annotated_folder_id", "") or "").strip()

        self._folder_cache: Dict[tuple[str, str], Optional[str]] = {}
        self._file_cache: Dict[tuple[str, str], Optional[DriveItem]] = {}

    def _list_by_query(self, q: str, fields: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        page_token = None
        while True:
            try:
                resp = (
                    self._svc.files()
                    .list(
                        q=q,
                        spaces="drive",
                        fields=f"nextPageToken, files({fields})",
                        pageToken=page_token,
                        pageSize=200,
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                    )
                    .execute()
                )
            except HttpError as e:
                # ✅ Never crash ingestion because Drive lookup failed
                logger.warning("Drive list failed (non-fatal). q=%s err=%s", q[:200], str(e))
                return out
            except Exception as e:
                logger.warning("Drive list failed (non-fatal). q=%s err=%s", q[:200], str(e))
                return out

            out.extend(resp.get("files", []) or [])
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return out

    def _find_folder_id(self, parent_id: str, folder_name: str) -> Optional[str]:
        key = (parent_id, folder_name)
        if key in self._folder_cache:
            return self._folder_cache[key]

        q = (
            f"'{parent_id}' in parents and "
            f"mimeType='application/vnd.google-apps.folder' and "
            f"name='{folder_name}' and trashed=false"
        )
        items = self._list_by_query(q, fields="id,name,mimeType,parents")
        folder_id = items[0]["id"] if items else None
        self._folder_cache[key] = folder_id
        return folder_id

    def _create_folder(self, parent_id: str, folder_name: str) -> str:
        body = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
        resp = self._svc.files().create(body=body, fields="id", supportsAllDrives=True).execute()
        fid = resp["id"]
        self._folder_cache[(parent_id, folder_name)] = fid
        return fid

    def _ensure_folder(self, parent_id: str, folder_name: str) -> str:
        fid = self._find_folder_id(parent_id, folder_name)
        if fid:
            return fid
        return self._create_folder(parent_id, folder_name)

    def _find_file_in_folder(self, parent_id: str, file_name: str) -> Optional[DriveItem]:
        key = (parent_id, file_name)
        if key in self._file_cache:
            return self._file_cache[key]

        q = f"'{parent_id}' in parents and name='{file_name}' and trashed=false"
        items = self._list_by_query(q, fields="id,name,mimeType,parents")
        if not items:
            self._file_cache[key] = None
            return None

        it = items[0]
        item = DriveItem(
            file_id=it["id"],
            name=it.get("name", ""),
            mime_type=it.get("mimeType", ""),
            parents=it.get("parents", []) or [],
        )
        self._file_cache[key] = item
        return item

    def resolve_path(self, rel_path: str, *, root_folder_id: Optional[str] = None) -> Optional[DriveItem]:
        root = (root_folder_id or self.root_folder_id or "").strip()
        if not root or not _is_valid_drive_id(root):
            return None

        p = (rel_path or "").strip().strip("/")
        if not p:
            return None

        parts = [x for x in p.split("/") if x]
        if not parts:
            return None

        parent_id = root
        for folder in parts[:-1]:
            fid = self._find_folder_id(parent_id, folder)
            if not fid:
                return None
            parent_id = fid

        filename = parts[-1]
        return self._find_file_in_folder(parent_id, filename)

    def download_file_bytes(self, file_id: str) -> Optional[bytes]:
        try:
            req = self._svc.files().get_media(fileId=file_id, supportsAllDrives=True)
            return req.execute()
        except HttpError:
            return None
        except Exception:
            return None

    def _make_public(self, file_id: str) -> None:
        body = {"type": "anyone", "role": "reader"}
        self._svc.permissions().create(fileId=file_id, body=body, supportsAllDrives=True).execute()

    def upload_bytes_to_subpath(
        self,
        *,
        folder_parts: List[str],
        file_name: str,
        content_bytes: bytes,
        mime_type: str,
        make_public: bool = True,
        root_folder_id: Optional[str] = None,
    ) -> Dict[str, str]:
        root = (root_folder_id or self.root_folder_id or "").strip()
        if not root:
            raise RuntimeError("Drive upload root folder id is not set")

        parent_id = root
        for f in folder_parts or []:
            parent_id = self._ensure_folder(parent_id, f)

        media = MediaIoBaseUpload(BytesIO(content_bytes), mimetype=mime_type, resumable=False)
        body = {"name": file_name, "parents": [parent_id]}

        resp = (
            self._svc.files()
            .create(body=body, media_body=media, fields="id,webViewLink,webContentLink", supportsAllDrives=True)
            .execute()
        )
        fid = resp["id"]

        if make_public:
            try:
                self._make_public(fid)
                resp2 = (
                    self._svc.files()
                    .get(fileId=fid, fields="id,webViewLink,webContentLink", supportsAllDrives=True)
                    .execute()
                )
                resp.update(resp2)
            except Exception:
                pass

        return {
            "file_id": fid,
            "webViewLink": resp.get("webViewLink", ""),
            "webContentLink": resp.get("webContentLink", ""),
        }

    def upload_annotated_bytes(
        self,
        *,
        checkin_id: str,
        file_name: str,
        content_bytes: bytes,
        mime_type: str = "image/png",
        make_public: bool = True,
    ) -> Dict[str, str]:
        root = (self.annotated_root_folder_id or "").strip()
        if not root:
            raise RuntimeError("GOOGLE_DRIVE_ANNOTATED_FOLDER_ID is not set")

        return self.upload_bytes_to_subpath(
            folder_parts=["Annotated", str(checkin_id)],
            file_name=file_name,
            content_bytes=content_bytes,
            mime_type=mime_type,
            make_public=make_public,
            root_folder_id=root,
        )

    def get_root_for_prefix(self, prefix: str) -> Optional[str]:
        prefix = (prefix or "").strip().strip("/")
        mp = getattr(self.settings, "drive_prefix_map", {}) or {}
        fid = (mp.get(prefix) or "").strip()
        if not _is_valid_drive_id(fid):
            return None
        return fid
