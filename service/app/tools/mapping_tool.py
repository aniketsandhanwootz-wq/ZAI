from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import os
import yaml


@dataclass(frozen=True)
class SheetMapping:
    raw: Dict[str, Any]

    @property
    def tabs(self) -> Dict[str, str]:
        return self.raw["spreadsheet"]["tabs"]

    def tab(self, key: str) -> str:
        return self.tabs[key]

    def col(self, tab_key: str, field: str) -> str:
        return self.raw["columns"][tab_key][field]

    @property
    def writeback(self) -> Dict[str, Any]:
        return self.raw.get("writeback", {})


def _find_repo_root(start: Path) -> Path:
    """
    Tries to find repo root by walking up until 'packages/contracts/sheets_mapping.yaml' exists.
    """
    p = start
    for _ in range(8):
        candidate = p / "packages" / "contracts" / "sheets_mapping.yaml"
        if candidate.exists():
            return p
        if p.parent == p:
            break
        p = p.parent
    return start


def load_sheet_mapping() -> SheetMapping:
    """
    Loads packages/contracts/sheets_mapping.yaml by default.
    You can override path with env: SHEETS_MAPPING_PATH
    """
    override = os.getenv("SHEETS_MAPPING_PATH", "").strip()
    if override:
        path = Path(override).expanduser().resolve()
        print(path)
    else:
        # service/app/tools -> go up to repo root
        here = Path(__file__).resolve()
        root = _find_repo_root(here.parent.parent.parent.parent)  # jumps out of service/app/tools
        path = root / "packages" / "contracts" / "sheets_mapping.yaml"

    if not path.exists():
        raise RuntimeError(f"sheets_mapping.yaml not found at: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError("Invalid sheets_mapping.yaml format")
    return SheetMapping(raw=data)
