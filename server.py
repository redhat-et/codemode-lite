#!/usr/bin/env python3
"""Minimal codemode MCP server — run_python only.

Connects to MCP servers (SSE/stdio), loads their tools,
and exposes a single 'run_python' tool that executes
agent-written Python code in a sandbox.

Usage:
  python server.py

  # In .mcp.json
  {"mcpServers": {"codemode": {"command": "python3", "args": ["server.py"]}}}

Environment:
  CODEMODE_BACKEND   - 'podman' or 'pyodide-wasm' (default: podman)
  CODEMODE_MCPS_DIR  - Directory with MCP server configs (default: ~/MCPs)
  CODEMODE_LOG_LEVEL - Logging level (default: WARNING)
  CODEMODE_LOG_FILE  - Log file path (optional)
  CODEMODE_TIMEOUT   - Execution timeout in seconds (default: 120)
"""
import asyncio
import json
import logging
import os
import sys
import time

_log_level = os.environ.get("CODEMODE_LOG_LEVEL", "WARNING")
logging.basicConfig(level=_log_level)

_log_file = os.environ.get("CODEMODE_LOG_FILE")
if _log_file:
    _fh = logging.FileHandler(_log_file)
    _fh.setLevel(_log_level)
    _fh.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s", datefmt="%H:%M:%S"))
    logging.getLogger().addHandler(_fh)

logger = logging.getLogger("codemode-mcp")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from contextlib import AsyncExitStack
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from codemode.engine import CodeMode
from codemode.mcp_adapter import MCPToolLoader

app = Server("codemode-mcp-server")

_cm = None
_tool_names = []
_server_names = []
_exit_stack = AsyncExitStack()
_loader = None


def load_server_configs():
    """Load MCP server configs from env or CODEMODE_MCPS_DIR."""
    env_cfg = os.environ.get("CODEMODE_SERVERS")
    if env_cfg:
        return json.loads(env_cfg)

    configs = []
    mcps_dir = os.path.expanduser(os.environ.get("CODEMODE_MCPS_DIR", "~/MCPs"))
    if not os.path.isdir(mcps_dir):
        return configs

    for f in sorted(os.listdir(mcps_dir)):
        if not f.endswith(".json"):
            continue
        try:
            with open(os.path.join(mcps_dir, f)) as fh:
                data = json.load(fh)
            for name, cfg in data.get("mcpServers", {}).items():
                if cfg.get("type") == "sse" or "url" in cfg:
                    configs.append({"name": name, "type": "sse", "url": cfg["url"]})
                elif "command" in cfg:
                    configs.append({
                        "name": name, "type": "stdio",
                        "command": cfg["command"],
                        "args": cfg.get("args", []),
                        "env": cfg.get("env"),
                    })
        except Exception as e:
            logger.warning("Failed to load %s: %s", f, e)
    return configs


async def init():
    """Connect to all MCP servers and create CodeMode instance."""
    global _cm, _tool_names, _server_names, _loader

    configs = load_server_configs()
    _loader = MCPToolLoader()

    for cfg in configs:
        try:
            if cfg["type"] == "sse":
                _loader.add_sse_server(cfg["name"], cfg["url"])
                logger.info("Added SSE server: %s at %s", cfg["name"], cfg["url"])
            else:
                env = cfg.get("env")
                if env:
                    merged = dict(os.environ)
                    merged.update(env)
                    env = merged
                _loader.add_stdio_server(cfg["name"], cfg["command"], cfg.get("args", []), env=env)
                logger.info("Added stdio server: %s", cfg["name"])
        except Exception as e:
            logger.error("Failed to add %s: %s", cfg["name"], e)

    _server_names = [cfg.name for cfg in _loader._configs]

    await _exit_stack.enter_async_context(_loader)
    tools = await _loader.load_tools()
    _tool_names = sorted(tools.keys())

    logger.info("Loaded %d tools: %s", len(tools), _tool_names)

    _cm = CodeMode(
        tools=tools,
        backend=os.environ.get("CODEMODE_BACKEND", "podman"),
        timeout=int(os.environ.get("CODEMODE_TIMEOUT", "120")),
    )


@app.list_tools()
async def list_tools():
    tools_desc = ", ".join(_tool_names)
    servers_desc = ", ".join(_server_names) if _server_names else "none configured"

    return [
        Tool(
            name="run_python",
            description=(
                f"Execute Python code directly in a sandbox. "
                f"Top-level await supported. "
                f"Call tools via `await tools['name'](param=value)`. "
                f"Currently loaded tools: {tools_desc}. "
                f"Available servers: {servers_desc}. "
                f"Available modules: json, datetime, re, math, collections, itertools, functools, asyncio."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": (
                            "Python code to execute. Top-level await supported. "
                            "Persistent sandbox — variables survive between calls. "
                            "1. DISCOVER: _discover() lists all tools, _schema('name') returns input schema, _search('query') searches tools. "
                            "2. CALL: await tools['tool_name'](param=value). "
                            "3. SERVER PROXIES: await mcp_calendar.list_events(...) groups tools by server."
                            "\n\nIMPORTANT: All functions are async — ALWAYS use await. "
                            "Minimize round trips: get ALL schemas required and call ALL tools to complete tasks ONE code block each. "
                            "ALWAYS print() results — output is only captured via print(). "
                            "Example: `r = await tools['create-event'](...); print(r)`"
                        ),
                    },
                    "servers": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            f"MCP servers to load on demand. Available: {servers_desc}. "
                            "Cached — loading an already-connected server is instant."
                        ),
                    },
                    "timeout": {
                        "type": "integer",
                        "description": "Execution timeout in seconds (default: 30)",
                    },
                },
                "required": ["code"],
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict):
    if name != "run_python":
        return [TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))]

    code = arguments.get("code")
    if not code:
        return [TextContent(type="text", text=json.dumps({"error": "Missing 'code' argument"}))]
    if not _cm:
        return [TextContent(type="text", text=json.dumps({"error": "Codemode not initialized"}))]

    timeout = arguments.get("timeout")
    servers = arguments.get("servers", [])
    original_timeout = _cm.timeout

    logger.info("run_python | code (%d chars):\n%s", len(code), code)

    t0 = time.time()
    try:
        if servers and _loader:
            for server_name in servers:
                try:
                    new_tools = await _loader.load_server(server_name)
                    for tool_name, tool_fn in new_tools.items():
                        _cm.tools[tool_name] = tool_fn
                        _cm._proxy._tools[tool_name] = tool_fn
                    logger.info("On-demand loaded server '%s': %s", server_name, list(new_tools.keys()))
                except Exception as e:
                    logger.warning("Failed to load server '%s': %s", server_name, e)

        if timeout and isinstance(timeout, int):
            _cm.timeout = timeout
        result = await _cm.run_code(code)
        elapsed = time.time() - t0
        output = {
            "success": result.get("success", False),
            "output": result.get("output"),
            "error": result.get("error"),
            "duration": round(elapsed, 2),
            "backend": result.get("backend"),
            "tool_calls": [
                {"name": tc.get("name"), "success": tc.get("success")}
                for tc in result.get("tool_calls", [])
            ],
        }
        return [TextContent(type="text", text=json.dumps(output, indent=2, default=str))]
    except Exception as e:
        elapsed = time.time() - t0
        return [TextContent(type="text", text=json.dumps({
            "success": False, "error": str(e), "duration": round(elapsed, 2),
        }))]
    finally:
        _cm.timeout = original_timeout


async def main():
    async with _exit_stack:
        await init()
        logger.info("Codemode MCP server ready with %d tools from %d servers", len(_tool_names), len(_server_names))
        async with stdio_server() as (read_stream, write_stream):
            await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
