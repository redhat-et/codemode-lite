"""Abstract base class for sandbox backends and shared execution types."""

from __future__ import annotations

import asyncio
import json
import logging
import time
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


class TimeoutContext:
    """Async context manager that enforces a timeout."""

    def __init__(self, seconds: int) -> None:
        self.seconds = seconds
        self._start: float = 0.0

    async def __aenter__(self) -> "TimeoutContext":
        self._start = time.monotonic()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        return False

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self._start


def parse_result(raw_output: Any, error: Optional[str] = None, duration: float = 0.0, memory_used: int = 0) -> ExecutionResult:
    """Build an ExecutionResult from raw execution output."""
    if error is not None:
        return ExecutionResult(success=False, output=None, error=error, duration=duration, memory_used=memory_used)
    return ExecutionResult(success=True, output=raw_output, error=None, duration=duration, memory_used=memory_used)


class TimeoutError(Exception):
    """Raised when execution exceeds timeout."""
    pass


async def run_with_timeout(coro, timeout: int) -> Any:
    """Run a coroutine with a timeout."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        raise TimeoutError(f"Execution timed out after {timeout} seconds")


def parse_execution_output(raw_output: str) -> Any:
    """Parse raw output from sandbox execution."""
    try:
        return json.loads(raw_output)
    except (json.JSONDecodeError, TypeError):
        return raw_output


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

    async def _execute_with_timeout(self, coro, timeout: int) -> ExecutionResult:
        """Helper: run *coro* with a wall-clock timeout."""
        start = time.monotonic()
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            elapsed = time.monotonic() - start
            return ExecutionResult(
                success=False, output=None,
                error=f"Execution timed out after {timeout}s",
                duration=elapsed, memory_used=0,
            )
