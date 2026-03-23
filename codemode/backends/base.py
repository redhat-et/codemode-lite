"""Abstract base class for sandbox backends and shared execution types."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger("codemode.backend")


@dataclass
class ExecutionResult:
    """Result of executing code in a sandbox backend."""

    success: bool
    output: Any = None
    error: Optional[str] = None
    duration: float = 0.0
    memory_used: int = 0
    tool_calls: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "output": self.output,
            "error": self.error,
            "duration": self.duration,
            "memory_used": self.memory_used,
            "tool_calls": self.tool_calls,
        }


class SandboxBackend(ABC):
    """Abstract base class that all sandbox backends must implement."""

    @abstractmethod
    async def execute(
        self, code: str, tools: dict, timeout: int = 30, **kwargs
    ) -> ExecutionResult:
        """Execute Python code inside the sandbox."""
        ...

    @abstractmethod
    def is_available(self) -> bool:
        """Return True if this backend is ready to execute code."""
        ...

    @abstractmethod
    def get_name(self) -> str:
        """Return a human-readable name for this backend."""
        ...
