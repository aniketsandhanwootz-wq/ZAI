from __future__ import annotations

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import base64

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ..config import Settings, parse_service_account_info


DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


@dataclass
class DriveItem:
    file_id: str
    name: str
    mime_type: str
    parents: List[str]


class DriveTool:
    """
    Minimal Drive read-only helper:
      - resolve a relative path under a root folder
      - download file bytes
    """

    def __init__(self, settings: Settings):
        info = parse_service_account_info(settings.google_service_account_json)
        creds = Credentials.from_service_account_info(info, scopes=DRIVE_SCOPES)
        self._svc = build("drive", "v3", credentials=creds, cache_discovery=False)

        self.root_folder_id = (getattr(settings, "google_drive_root_folder_id", "") or "").strip()

        # small caches
        self._folder_cache: Dict[tuple[str, str], Optional[str]] = {}  # (parent_id, folder_name) -> folder_id
        self._file_cache: Dict[tuple[str, str], Optional[DriveItem]] = {}  # (parent_id, file_name) -> DriveItem

    def _list_by_query(self, q: str, fields: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        page_token = None
        while True:
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

    def _find_file_in_folder(self, parent_id: str, file_name: str) -> Optional[DriveItem]:
        key = (parent_id, file_name)
        if key in self._file_cache:
            return self._file_cache[key]

        q = (
            f"'{parent_id}' in parents and "
            f"name='{file_name}' and trashed=false"
        )
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
        """
        rel_path examples:
          CCP_Files_/220/Files.1009.pdf
          CheckIn_Images/abc.jpg
          Conversation_Images/xyz.png
        """
        root = (root_folder_id or self.root_folder_id or "").strip()
        if not root:
            return None

        p = (rel_path or "").strip().strip("/")
        if not p:
            return None

        parts = [x for x in p.split("/") if x]
        if not parts:
            return None

        parent_id = root

        # walk folders
        for folder in parts[:-1]:
            fid = self._find_folder_id(parent_id, folder)
            if not fid:
                return None
            parent_id = fid

        # final is file name
        filename = parts[-1]
        return self._find_file_in_folder(parent_id, filename)

    def download_file_bytes(self, file_id: str) -> Optional[bytes]:
        try:
            req = self._svc.files().get_media(fileId=file_id, supportsAllDrives=True)
            data = req.execute()
            return data
        except HttpError:
            return None
        except Exception:
            return None
