# service/app/tools/__init__.py
from __future__ import annotations

from .langchain_tools import (
    build_langchain_tools,          # returns dict[name] -> StructuredTool
    get_tool_registry,              # cached ToolRegistry
    ToolRegistry,                   # typed registry wrapper
)

__all__ = ["build_langchain_tools", "get_tool_registry", "ToolRegistry"]