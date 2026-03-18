"""Tool call routing and logging proxy."""
import asyncio
import json
import logging
import time
from typing import Any, Callable

logger = logging.getLogger("codemode.proxy")


class ToolProxy:
    """Proxy that routes tool calls, logs invocations, and unwraps responses."""

    def __init__(self, tools: dict[str, Callable]) -> None:
        self._tools: dict[str, Callable] = dict(tools)
        self._call_log: list[dict[str, Any]] = []

    async def call(self, _tool_name: str, *args: Any, **kwargs: Any) -> Any:
        name = _tool_name
        if name not in self._tools:
            raise KeyError(f"Tool not found: {name}")

        fn = self._tools[name]
        start = time.monotonic()
        success = True
        result: Any = None
        error_msg: str | None = None

        # Unwrap 'arguments' dict
        if "arguments" in kwargs and isinstance(kwargs["arguments"], dict) and len(kwargs) == 1:
            kwargs = kwargs["arguments"]
        # Handle single positional dict arg
        if len(args) == 1 and isinstance(args[0], dict) and not kwargs:
            kwargs = dict(args[0])
            args = ()

        # Strip None values
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        try:
            if asyncio.iscoroutinefunction(fn):
                result = await fn(*args, **kwargs)
            else:
                result = fn(*args, **kwargs)

            # Unwrap tuple responses
            if isinstance(result, tuple) and len(result) >= 1:
                result = result[0]
            # Unwrap MCP content blocks
            if isinstance(result, list) and result:
                item = result[0]
                text = (item.get("text") if isinstance(item, dict)
                        else getattr(item, "text", None))
                if text and isinstance(text, str):
                    try:
                        result = json.loads(text)
                    except (ValueError, TypeError):
                        result = text
            # Parse JSON string responses
            if isinstance(result, str):
                try:
                    result = json.loads(result)
                except (ValueError, TypeError):
                    pass
        except Exception as exc:
            success = False
            error_msg = str(exc)
            raise
        finally:
            duration = time.monotonic() - start
            self._call_log.append({
                "name": name,
                "kwargs": self._safe(kwargs),
                "result": self._safe(result) if success else None,
                "error": error_msg,
                "duration": round(duration, 6),
                "success": success,
            })

        return result

    def as_sandbox_globals(self) -> dict:
        sandbox: dict[str, Callable] = {}
        for name in self._tools:
            async def _wrapper(*a, _n=name, **kw):
                return await self.call(_n, *a, **kw)
            _wrapper.__name__ = name
            original = self._tools[name]
            if hasattr(original, '_mcp_schema'):
                _wrapper._mcp_schema = original._mcp_schema
            sandbox[name] = _wrapper
        return sandbox

    def get_call_log(self) -> list[dict[str, Any]]:
        return list(self._call_log)

    def clear_log(self) -> None:
        self._call_log.clear()

    @staticmethod
    def _safe(value: Any) -> Any:
        try:
            json.dumps(value)
            return value
        except (TypeError, ValueError):
            return repr(value)
