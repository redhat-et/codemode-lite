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
    backend = _cm.backend_name if _cm else "podman"
    is_podman = backend == "podman"

    if is_podman:
        tool_description = (
            f"Execute Python code in an isolated Podman container with access to {len(_tool_names)} tools across {len(_server_names)} servers. "
            f"Top-level await, persistent state between calls. "
            f"Maximize tool calls per run_python call — batch independent and dependent calls together to minimize round trips."
            f"Use asyncio.gather() for independent calls. Chain dependent calls sequentially within the SAME code block. "
            f"Discovery: _discover() lists all tools, _schema('name') shows required/optional params, _search('query') finds tools by keyword. "
            f"Process data inside the sandbox and only print() a compact summary to keep context small. "
            f"Tools: {tools_desc}. Servers: {servers_desc}."
        )
        code_description = (
            "Python code. Top-level await, no wrapper needed. Variables persist between calls.\n\n"
            "Maximize tool calls per code block to minimize round trips."
            "Batch all tool calls into a single code block — use asyncio.gather() for independent calls, sequential await for dependent ones.\n\n"
            "WORKFLOW:\n"
            "1. Discover: schema = await _schema('tool-name') — check required/optional params\n"
            "2. Call: result = await tools['tool-name'](param=value)\n"
            "3. Parallel: a, b = await asyncio.gather(tools['x'](...), tools['y'](...))\n"
            "4. Server proxies: await mcp_calendar.list_events(...)\n"
            "5. Output: print() only a summary — raw data stays in sandbox\n\n"
            "All functions are async — ALWAYS use await. Output is ONLY captured via print().\n"
            "Available: json, datetime, re, math, collections, itertools, functools, asyncio.\n\n"
            "Example — ONE code block, multiple tools, parallel + sequential:\n"
            "# parallel: independent calls\n"
            "cals, repos, search = await asyncio.gather(\n"
            "    tools['list-calendars'](),\n"
            "    tools['search_repositories'](query='test'),\n"
            "    tools['google_search'](q='AI conferences 2026')\n"
            ")\n"
            "# sequential: depends on results above\n"
            "event = await tools['create-event'](calendarId=cals[0]['id'], summary=search['organic'][0]['title'])\n"
            "issue = await tools['create_issue'](owner=repos['items'][0]['owner']['login'], repo='test', title='done')\n"
            "print(json.dumps({'event': event['summary'], 'issue': issue['number']}))"
        )
    else:
        tool_description = (
            f"Execute Python code in an isolated WASM sandbox with access to {len(_tool_names)} tools across {len(_server_names)} servers. "
            f"Code MUST define async def main() returning a dict. Fresh sandbox per call, no persistence. "
            f"Maximize tool calls per run_python call — batch independent and dependent calls together in one main() to minimize round trips."
            f"Use asyncio.gather() for independent calls. Chain dependent calls sequentially within the SAME main(). "
            f"Discovery: tools['_discover']() lists all tools, tools['_schema']('name') shows required/optional params, tools['_search']('query') finds tools by keyword. "
            f"Process data inside main() and return only a compact summary dict to keep context small. "
            f"Tools: {tools_desc}. Servers: {servers_desc}."
        )
        code_description = (
            "Python code. MUST define async def main() that returns a dict.\n\n"
            "Maximize tool calls per main() to minimize round trips."
            "Batch all tool calls into a single main() — use asyncio.gather() for independent calls, sequential await for dependent ones.\n\n"
            "WORKFLOW:\n"
            "1. Discover: schema = await tools['_schema']('tool-name') — check required/optional params\n"
            "2. Call: result = await tools['tool-name'](param=value)\n"
            "3. Parallel: a, b = await asyncio.gather(tools['x'](...), tools['y'](...))\n"
            "4. Return: return {'key': processed_data} — only the returned dict is captured\n\n"
            "All functions are async — ALWAYS use await. Output is ONLY the returned dict from main().\n"
            "Available: json, datetime, re, math, collections, itertools, functools, asyncio.\n\n"
            "Example — ONE main(), multiple tools, parallel + sequential:\n"
            "async def main():\n"
            "    # parallel: independent calls\n"
            "    cals, repos, search = await asyncio.gather(\n"
            "        tools['list-calendars'](),\n"
            "        tools['search_repositories'](query='test'),\n"
            "        tools['google_search'](q='AI conferences 2026')\n"
            "    )\n"
            "    # sequential: depends on results above\n"
            "    event = await tools['create-event'](calendarId=cals[0]['id'], summary=search['organic'][0]['title'])\n"
            "    return {'event': event['summary'], 'repos': repos.get('total_count', 0)}"
        )

    return [
        Tool(
            name="run_python",
            description=tool_description,
            inputSchema={
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": code_description,
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
