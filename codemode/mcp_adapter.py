"""MCP server adapter — connects to MCP servers and wraps tools as async callables."""
import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger("codemode.mcp")

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False


@dataclass
class MCPServerConfig:
    name: str
    transport: str  # "stdio" or "sse"
    command: Optional[str] = None
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    url: Optional[str] = None


class MCPConnection:
    """Manages a connection to a single MCP server via stdio or SSE."""

    def __init__(self, config: MCPServerConfig):
        self.config = config
        self._process: Optional[asyncio.subprocess.Process] = None
        self._request_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._reader_task: Optional[asyncio.Task] = None
        self._tools: dict[str, dict] = {}
        self._session: Optional[Any] = None
        self._message_endpoint: Optional[str] = None

    async def connect(self):
        if self.config.transport == "stdio":
            await self._connect_stdio()
        elif self.config.transport == "sse":
            await self._connect_sse()
        else:
            raise ValueError(f"Unknown transport: {self.config.transport}")
        await self._initialize()

    async def _connect_stdio(self):
        env = {**os.environ, **self.config.env}
        self._process = await asyncio.create_subprocess_exec(
            self.config.command, *self.config.args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        self._reader_task = asyncio.create_task(self._read_stdio_responses())

    async def _connect_sse(self):
        if not HAS_AIOHTTP:
            raise ImportError("aiohttp is required for SSE. Install: pip install aiohttp")

        self._session = aiohttp.ClientSession()
        sse_url = self.config.url
        endpoint_future = asyncio.get_running_loop().create_future()

        async def sse_listener():
            try:
                async with self._session.get(sse_url) as resp:
                    if resp.status != 200:
                        endpoint_future.set_exception(ConnectionError(f"SSE failed: HTTP {resp.status}"))
                        return
                    event_type = None
                    data_lines = []
                    async for line_bytes in resp.content:
                        line = line_bytes.decode("utf-8").rstrip("\n\r")
                        if line.startswith("event:"):
                            event_type = line[len("event:"):].strip()
                        elif line.startswith("data:"):
                            data_lines.append(line[len("data:"):].strip())
                        elif line == "":
                            if event_type and data_lines:
                                data = "\n".join(data_lines)
                                self._handle_sse_event(event_type, data, endpoint_future)
                            event_type = None
                            data_lines = []
            except asyncio.CancelledError:
                pass
            except Exception as e:
                if not endpoint_future.done():
                    endpoint_future.set_exception(e)

        self._reader_task = asyncio.create_task(sse_listener())

        try:
            self._message_endpoint = await asyncio.wait_for(endpoint_future, timeout=10)
        except asyncio.TimeoutError:
            raise ConnectionError(f"Timed out waiting for endpoint from {sse_url}")

        if self._message_endpoint.startswith("/"):
            from urllib.parse import urlparse
            parsed = urlparse(sse_url)
            self._message_endpoint = f"{parsed.scheme}://{parsed.netloc}{self._message_endpoint}"

    def _resolve_jsonrpc_response(self, msg: dict):
        msg_id = msg.get("id")
        if msg_id is not None and msg_id in self._pending:
            future = self._pending.pop(msg_id)
            if "error" in msg:
                future.set_exception(RuntimeError(f"MCP error: {msg['error'].get('message', str(msg['error']))}"))
            else:
                future.set_result(msg.get("result", {}))

    def _handle_sse_event(self, event_type, data, endpoint_future):
        if event_type == "endpoint":
            if not endpoint_future.done():
                endpoint_future.set_result(data)
        elif event_type == "message":
            try:
                msg = json.loads(data)
            except json.JSONDecodeError:
                return
            self._resolve_jsonrpc_response(msg)

    async def _initialize(self):
        await self._send_request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "codemode-lite", "version": "0.1.0"},
        })
        await self._send_notification("notifications/initialized", {})

    async def list_tools(self) -> list[dict]:
        result = await self._send_request("tools/list", {})
        tools = result.get("tools", [])
        for tool in tools:
            self._tools[tool["name"]] = tool
        return tools

    async def call_tool(self, name: str, arguments: dict) -> Any:
        result = await self._send_request("tools/call", {"name": name, "arguments": arguments})

        if result.get("isError"):
            content = result.get("content", [])
            error_text = "".join(c.get("text", "") for c in content if c.get("type") == "text")
            raise RuntimeError(f"Tool '{name}' error: {error_text or result}")

        content = result.get("content", [])
        if not content:
            return result

        if len(content) == 1 and content[0].get("type") == "text":
            text = content[0]["text"]
            try:
                return json.loads(text)
            except (json.JSONDecodeError, TypeError):
                return text

        def _parse(c):
            if c.get("type") == "text":
                try:
                    return json.loads(c["text"])
                except (json.JSONDecodeError, TypeError):
                    return c["text"]
            return c
        return [_parse(c) for c in content]

    async def _send_request(self, method: str, params: dict, _retry: bool = True) -> dict:
        self._request_id += 1
        request_id = self._request_id
        message = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}

        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future

        if self.config.transport == "stdio":
            line = json.dumps(message) + "\n"
            self._process.stdin.write(line.encode())
            await self._process.stdin.drain()
        elif self.config.transport == "sse":
            async with self._session.post(
                self._message_endpoint, json=message,
                headers={"Content-Type": "application/json"},
            ) as resp:
                if resp.status == 503 and _retry:
                    self._pending.pop(request_id, None)
                    await self._reconnect_sse()
                    return await self._send_request(method, params, _retry=False)
                elif resp.status not in (200, 202, 204):
                    self._pending.pop(request_id, None)
                    raise RuntimeError(f"MCP POST failed ({resp.status})")

        return await asyncio.wait_for(future, timeout=30)

    async def _reconnect_sse(self):
        for future in self._pending.values():
            if not future.done():
                future.set_exception(RuntimeError("SSE reconnecting"))
        self._pending.clear()
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        await self._connect_sse()
        await self._initialize()

    async def _send_notification(self, method: str, params: dict):
        message = {"jsonrpc": "2.0", "method": method, "params": params}
        if self.config.transport == "stdio":
            self._process.stdin.write((json.dumps(message) + "\n").encode())
            await self._process.stdin.drain()
        elif self.config.transport == "sse":
            async with self._session.post(
                self._message_endpoint, json=message,
                headers={"Content-Type": "application/json"},
            ) as resp:
                pass

    async def _read_stdio_responses(self):
        try:
            while True:
                line = await self._process.stdout.readline()
                if not line:
                    break
                try:
                    msg = json.loads(line.decode().strip())
                except json.JSONDecodeError:
                    continue
                self._resolve_jsonrpc_response(msg)
        except asyncio.CancelledError:
            pass

    async def close(self):
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        if self._process:
            self._process.stdin.close()
            try:
                self._process.terminate()
                await asyncio.wait_for(self._process.wait(), timeout=5)
            except (asyncio.TimeoutError, ProcessLookupError):
                self._process.kill()
        if self._session:
            await self._session.close()


class MCPToolLoader:
    """Load tools from multiple MCP servers as async callables."""

    def __init__(self):
        self._configs: list[MCPServerConfig] = []
        self._connections: list[MCPConnection] = []
        self._tools: dict[str, Callable] = {}

    def add_stdio_server(self, name, command, args=None, env=None):
        self._configs.append(MCPServerConfig(
            name=name, transport="stdio",
            command=command, args=args or [], env=env or {},
        ))

    def add_sse_server(self, name, url):
        self._configs.append(MCPServerConfig(name=name, transport="sse", url=url))

    async def load_tools(self) -> dict[str, Callable]:
        failed = []
        for config in self._configs:
            conn = MCPConnection(config)
            try:
                await conn.connect()
                tools = await conn.list_tools()
                self._connections.append(conn)
                for tool_def in tools:
                    tool_def["_server"] = config.name
                    self._tools[tool_def["name"]] = self._make_tool_callable(conn, tool_def)
                logger.info("MCP '%s': loaded %d tools", config.name, len(tools))
            except Exception as e:
                logger.error("Failed to connect to '%s': %s", config.name, e)
                failed.append((config.name, e))
                await conn.close()

        if failed and not self._tools:
            raise ConnectionError(f"All MCP servers failed: {', '.join(n for n, _ in failed)}")
        return dict(self._tools)

    def _make_tool_callable(self, conn, tool_def):
        mcp_name = tool_def["name"]
        schema = tool_def.get("inputSchema", {})
        properties = schema.get("properties", {})
        param_names = list(properties.keys())

        async def tool_fn(*args, **kwargs):
            arguments = {}
            if len(args) == 1 and isinstance(args[0], dict) and not kwargs:
                arguments = args[0]
            elif args:
                for i, arg in enumerate(args):
                    if isinstance(arg, dict) and i == 0:
                        arguments.update(arg)
                    elif i < len(param_names):
                        arguments[param_names[i]] = arg
                arguments.update(kwargs)
            else:
                arguments = kwargs
            return await conn.call_tool(mcp_name, arguments)

        tool_fn.__name__ = mcp_name
        tool_fn.__doc__ = tool_def.get("description", "")
        tool_fn._mcp_schema = tool_def
        return tool_fn

    def _get_connection(self, server_name):
        for conn in self._connections:
            if conn.config.name == server_name:
                return conn
        return None

    def get_config(self, server_name):
        for cfg in self._configs:
            if cfg.name == server_name:
                return cfg
        return None

    async def load_server(self, server_name: str) -> dict[str, Callable]:
        existing = self._get_connection(server_name)
        if existing:
            return {n: f for n, f in self._tools.items()
                    if hasattr(f, '_mcp_schema') and f._mcp_schema.get("_server") == server_name}

        config = self.get_config(server_name)
        if not config:
            raise ValueError(f"Server '{server_name}' not configured")

        conn = MCPConnection(config)
        await conn.connect()
        tools = await conn.list_tools()
        self._connections.append(conn)

        new_tools = {}
        for tool_def in tools:
            tool_def["_server"] = server_name
            fn = self._make_tool_callable(conn, tool_def)
            self._tools[tool_def["name"]] = fn
            new_tools[tool_def["name"]] = fn
        return new_tools

    async def close(self):
        for conn in self._connections:
            await conn.close()
        self._connections.clear()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
