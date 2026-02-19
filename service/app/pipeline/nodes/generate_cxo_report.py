from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from ...config import Settings
from ...tools.llm_tool import LLMTool


def _find_repo_root(start: Path) -> Path:
    p = start
    for _ in range(8):
        if (p / "packages" / "prompts" / "cxo_report.md").exists():
            return p
        if p.parent == p:
            break
        p = p.parent
    return start


def _load_prompt_template() -> str:
    here = Path(__file__).resolve()
    root = _find_repo_root(here.parent.parent.parent.parent)
    path = root / "packages" / "prompts" / "cxo_report.md"
    return path.read_text(encoding="utf-8")


def generate_cxo_report_html(
    *,
    settings: Settings,
    all_assemblies: List[Dict[str, Any]],
    checkins: List[Dict[str, Any]],
    project_updates: List[Dict[str, Any]],
) -> str:
    tpl = _load_prompt_template()

    prompt = (
        tpl.replace("{{ALL_ASSEMBLIES_JSON}}", json.dumps(all_assemblies, ensure_ascii=False))
        .replace("{{CHECKINS_JSON}}", json.dumps(checkins, ensure_ascii=False))
        .replace("{{PROJECT_UPDATES_JSON}}", json.dumps(project_updates, ensure_ascii=False))
    )

    llm = LLMTool(settings)
    return llm.generate_text(prompt)