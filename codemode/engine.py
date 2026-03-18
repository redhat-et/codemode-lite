"""CodeMode engine — run_code only, no LLM generation."""
import asyncio
import importlib
import logging
import time
from typing import Callable

from .proxy import ToolProxy

logger = logging.getLogger("codemode")

_BACKEND_REGISTRY = {
    "pyodide-wasm": (".backends.pyodide_wasm_backend", "PyodideWasmBackend"),
    "podman": (".backends.podman_backend", "PodmanBackend"),
}


class CodeMode:
    def __init__(
        self,
        tools: dict[str, Callable],
        backend: str = "podman",
        timeout: int = 30,
    ):
        self.tools = tools
        self.backend_name = backend
        self.timeout = timeout
        self.persistent = backend == "podman"
        self._proxy = ToolProxy(tools)
        self._backend = self._create_backend(backend)

        logger.info(
            "CodeMode initialized | backend=%s | tools=%d | timeout=%ds",
            backend, len(tools), timeout,
        )

    def _create_backend(self, backend: str):
        entry = _BACKEND_REGISTRY.get(backend)
        if entry is None:
            raise ValueError(f"Unknown backend '{backend}'. Valid: {sorted(_BACKEND_REGISTRY)}")

        module_path, class_name = entry
        mod = importlib.import_module(module_path, package=__package__)
        cls = getattr(mod, class_name)
        instance = cls()

        if not instance.is_available():
            raise RuntimeError(f"Backend '{backend}' is not available (required service/binary not found).")

        logger.info("Using %s backend", instance.get_name())
        return instance

    async def run_code(self, code: str) -> dict:
        """Execute pre-written code directly (no LLM generation)."""
        logger.info("RUN_CODE | code_length=%d", len(code))

        sandbox_tools = self._proxy.as_sandbox_globals()
        sandbox_tools.update(self._build_discovery_helpers())
        result = await self._backend.execute(
            code, sandbox_tools, timeout=self.timeout,
            persistent=self.persistent,
        )

        logger.info("RUN_CODE | %s in %.2fs", "SUCCESS" if result.success else "FAILED", result.duration)

        if result.success:
            return {
                "success": True,
                "output": result.output,
                "duration": result.duration,
                "backend": self._backend.get_name(),
                "tool_calls": self._proxy.get_call_log(),
            }
        else:
            return {
                "success": False,
                "error": result.error,
                "backend": self._backend.get_name(),
            }

    def _build_discovery_helpers(self) -> dict:
        """Build _discover, _schema, _search functions for the sandbox."""
        tools_ref = self.tools

        def _get_schemas():
            schemas = {}
            for name, fn in tools_ref.items():
                if name.startswith('_'):
                    continue
                if hasattr(fn, '_mcp_schema'):
                    schemas[name] = fn._mcp_schema
                else:
                    schemas[name] = {"name": name, "description": getattr(fn, '__doc__', '') or ""}
            return schemas

        async def _discover():
            schemas = _get_schemas()
            result = []
            for n, s in schemas.items():
                entry = {"name": n, "description": (s.get("description", "") or "")[:120]}
                server = s.get("_server")
                if server:
                    entry["server"] = server
                result.append(entry)
            return result

        async def _schema(name: str):
            schemas = _get_schemas()
            if name not in schemas:
                return {"error": f"Tool '{name}' not found. Available: {list(schemas.keys())}"}
            return schemas[name]

        async def _search(query: str):
            q = query.lower()
            schemas = _get_schemas()
            return [
                {"name": n, "description": (s.get("description", "") or "")[:120],
                 "server": s.get("_server", "")}
                for n, s in schemas.items()
                if q in n.lower() or q in (s.get("description", "") or "").lower()
            ]

        return {"_discover": _discover, "_schema": _schema, "_search": _search}

    async def close(self):
        if hasattr(self._backend, "close") and callable(self._backend.close):
            await self._backend.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
