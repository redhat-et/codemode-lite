"""Podman-based sandbox backend with persistent container.

Runs Python code inside rootless Podman containers with strict isolation.
Container stays alive between calls — variables persist within a session.

Architecture (ported from mcp-server-code-execution-mode):
- JSON-framed bidirectional protocol over stdin/stdout
- Persistent container with async event loop
- Top-level await via PyCF_ALLOW_TOP_LEVEL_AWAIT
- Persistent global namespace (variables survive between calls)
- Baked server metadata for local discovery (no RPC)
- Server-scoped proxies (mcp_<alias>.tool_name())
- Tool calls proxied via RPC from container to host

Security posture:
- No network access (--network none)
- Read-only filesystem (--read-only)
- All capabilities dropped (--cap-drop ALL)
- No privilege escalation (--security-opt no-new-privileges)
- Unprivileged user (65534:65534)
- Memory and PID limits
"""
import asyncio
import json
import logging
import os
import shutil
import tempfile
import textwrap
import time
from pathlib import Path
from typing import Any

from .base import ExecutionResult, SandboxBackend

logger = logging.getLogger("codemode.backend.podman")

DEFAULT_IMAGE = "python:3.14-slim"
DEFAULT_MEMORY = "512m"
DEFAULT_PIDS = 128
DEFAULT_USER = "65534:65534"


class PodmanBackend(SandboxBackend):
    """Sandbox backend using rootless Podman containers.

    Container stays alive between calls — variables persist in the global
    namespace within a session. Resets when the host process exits.
    """

    def __init__(
        self,
        image: str = DEFAULT_IMAGE,
        memory: str = DEFAULT_MEMORY,
        pids_limit: int = DEFAULT_PIDS,
        user: str = DEFAULT_USER,
        cpus: str | None = None,
        runtime: str | None = None,
    ):
        self._image = image
        self._memory = memory
        self._pids_limit = pids_limit
        self._user = user
        self._cpus = cpus
        self._runtime = runtime or self._detect_runtime()
        # Persistent container state
        self._process: asyncio.subprocess.Process | None = None
        self._ipc_dir: str | None = None

    @staticmethod
    def _detect_runtime() -> str | None:
        for name in ("podman", "docker"):
            path = shutil.which(name)
            if path:
                return path
        return None

    def _build_cmd(self, ipc_dir: str) -> list[str]:
        if not self._runtime:
            raise FileNotFoundError("Neither podman nor docker found in PATH")

        cmd = [
            self._runtime, "run",
            "--rm",
            "--interactive",
            "--network", "none",
            "--read-only",
            "--pids-limit", str(self._pids_limit),
            "--memory", self._memory,
            "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=64m",
            "--tmpfs", "/workspace:rw,noexec,nosuid,nodev,size=128m",
            "--workdir", "/workspace",
            "--env", "HOME=/workspace",
            "--env", "PYTHONUNBUFFERED=1",
            "--env", "PYTHONIOENCODING=utf-8",
            "--env", "PYTHONDONTWRITEBYTECODE=1",
            "--security-opt", "no-new-privileges",
            "--cap-drop", "ALL",
            "--user", self._user,
            f"-v={ipc_dir}:/ipc:ro",
        ]
        if self._cpus:
            cmd.extend(["--cpus", self._cpus])
        cmd.extend([self._image, "python3", "-u", "/ipc/entrypoint.py"])
        return cmd

    @staticmethod
    def _render_entrypoint(tool_names: list[str], server_metadata: str = "[]") -> str:
        """Generate the entrypoint.py that runs inside the container.

        Implements:
        - StreamProxy: redirects stdout/stderr to JSON messages
        - RPC infrastructure: request/response with futures for tool calls
        - Persistent namespace: variables survive between execute commands
        - Top-level await: PyCF_ALLOW_TOP_LEVEL_AWAIT compilation
        - Baked server metadata: _discover/_schema/_search work locally (no RPC)
        - Server-scoped proxies: mcp_<alias>.tool_name() access
        - Main loop: waits for execute commands, runs code, sends done
        """
        tool_names_json = json.dumps(tool_names)
        return textwrap.dedent(f'''\
            import asyncio
            import inspect
            import json
            import sys
            import traceback

            # ---- Stream Proxy ----
            class _StreamProxy:
                def __init__(self, kind):
                    self._kind = kind
                def write(self, data):
                    if data:
                        _send({{"type": self._kind, "data": data}})
                def flush(self):
                    pass
                def isatty(self):
                    return False

            def _send(msg):
                sys.__stdout__.write(json.dumps(msg, separators=(",", ":"), default=str) + "\\n")
                sys.__stdout__.flush()

            sys.stdout = _StreamProxy("stdout")
            sys.stderr = _StreamProxy("stderr")

            # ---- RPC Infrastructure ----
            _PENDING = {{}}
            _COUNTER = 0
            _EXEC_QUEUE = asyncio.Queue()

            async def _rpc_call(payload, timeout=30):
                global _COUNTER
                _COUNTER += 1
                rid = _COUNTER
                fut = asyncio.get_running_loop().create_future()
                _PENDING[rid] = fut
                _send({{"type": "rpc_request", "id": rid, "payload": payload}})
                try:
                    return await asyncio.wait_for(fut, timeout=timeout)
                except asyncio.TimeoutError:
                    _PENDING.pop(rid, None)
                    raise RuntimeError(f"RPC call timed out after {{timeout}}s: {{payload.get('type', '')}}")

            async def _stdin_reader():
                reader = asyncio.StreamReader()
                protocol = asyncio.StreamReaderProtocol(reader)
                await asyncio.get_running_loop().connect_read_pipe(
                    lambda: protocol, sys.__stdin__
                )
                while True:
                    line = await reader.readline()
                    if not line:
                        break
                    try:
                        msg = json.loads(line.decode())
                    except Exception:
                        continue
                    msg_type = msg.get("type")
                    if msg_type == "rpc_response":
                        rid = msg.get("id")
                        if rid in _PENDING:
                            _PENDING.pop(rid).set_result(msg.get("payload", {{}}))
                    elif msg_type == "execute":
                        await _EXEC_QUEUE.put(msg.get("code", ""))

            # ---- Tool Proxies ----
            _TOOL_NAMES = json.loads('{tool_names_json}')

            def _make_tool(name):
                async def _call(**kwargs):
                    r = await _rpc_call({{"type": "call_tool", "tool": name, "arguments": kwargs}})
                    if not r.get("success", True):
                        raise RuntimeError(r.get("error", "Tool call failed"))
                    return r.get("result")
                _call.__name__ = name
                return _call

            class _ToolsDict(dict):
                """Dict that auto-creates RPC proxies for unknown tool names.

                When servers are loaded on demand after the container starts,
                new tools won't be in _TOOL_NAMES. This handles that by
                creating a proxy on first access — the host has the tool loaded.
                """
                def __missing__(self, name):
                    fn = _make_tool(name)
                    self[name] = fn
                    return fn

            tools = _ToolsDict()
            for _tn in _TOOL_NAMES:
                tools[_tn] = _make_tool(_tn)

            # ---- Baked Server Metadata (from bridge pattern) ----
            _SERVER_METADATA = json.loads({server_metadata!r})
            _ALL_SCHEMAS = {{}}
            for _sm in _SERVER_METADATA:
                for _t in _sm.get("tools", []):
                    _ALL_SCHEMAS[_t.get("name", "")] = _t

            # Discovery helpers — use baked JSON first, fall back to RPC
            # for tools loaded after container started (on-demand loading)
            async def _discover():
                r = await _rpc_call({{"type": "discover"}})
                return r if isinstance(r, list) else list(_ALL_SCHEMAS.keys())

            async def _schema(name):
                if name in _ALL_SCHEMAS:
                    return _ALL_SCHEMAS[name]
                # Tool may have been loaded after container started — ask host
                r = await _rpc_call({{"type": "schema", "tool": name}})
                return r

            async def _search(query):
                r = await _rpc_call({{"type": "search", "query": query}})
                return r if isinstance(r, list) else []

            tools["_discover"] = _discover
            tools["_schema"] = _schema
            tools["_search"] = _search

            # ---- Server-Scoped Proxies (mcp_<alias>.tool_name()) ----
            class _MCPProxy:
                def __init__(self, server_name, tool_map):
                    self._server = server_name
                    self._tools = tool_map
                def __getattr__(self, name):
                    if name in self._tools:
                        return self._tools[name]
                    raise AttributeError(f"Server '{{self._server}}' has no tool '{{name}}'")
                def __repr__(self):
                    return f"<MCPProxy '{{self._server}}' tools={{list(self._tools.keys())}}>"

            _server_proxies = {{}}
            for _sm in _SERVER_METADATA:
                _sname = _sm.get("name", "")
                _alias = _sm.get("alias", _sname.lower().replace(" ", "_").replace("-", "_"))
                _stool_map = {{}}
                for _t in _sm.get("tools", []):
                    _tname = _t.get("name", "")
                    if _tname in tools:
                        _stool_map[_tname] = tools[_tname]
                        # Also map alias if different
                        _talias = _tname.replace("-", "_").replace(".", "_")
                        if _talias != _tname:
                            _stool_map[_talias] = tools[_tname]
                _proxy = _MCPProxy(_sname, _stool_map)
                _server_proxies[_alias] = _proxy

            # ---- Safe Modules ----
            import re, math, datetime, collections, itertools, functools

            # ---- Persistent Namespace ----
            _NS = {{
                "__name__": "__sandbox__",
                "tools": tools,
                "asyncio": asyncio,
                "json": json,
                "re": re,
                "math": math,
                "datetime": datetime,
                "collections": collections,
                "itertools": itertools,
                "functools": functools,
                "_discover": _discover,
                "_schema": _schema,
                "_search": _search,
            }}
            # Inject tools as bare names
            for _tn in _TOOL_NAMES:
                _safe = _tn.replace("-", "_").replace(".", "_")
                _NS[_safe] = tools[_tn]
            # Inject server proxies as mcp_<alias>
            for _alias, _proxy in _server_proxies.items():
                _NS[f"mcp_{{_alias}}"] = _proxy

            # ---- Code Execution ----
            async def _execute_code(code):
                try:
                    flags = getattr(
                        __import__("ast"), "PyCF_ALLOW_TOP_LEVEL_AWAIT", 0
                    )
                    compiled = compile(code, "<sandbox>", "exec", flags=flags)
                    result = eval(compiled, _NS, _NS)
                    if inspect.isawaitable(result):
                        await result
                    return True
                except SystemExit:
                    raise
                except BaseException:
                    traceback.print_exc()
                    return False

            # ---- Main Loop ----
            async def _main_loop():
                asyncio.create_task(_stdin_reader())
                while True:
                    code = await _EXEC_QUEUE.get()
                    ok = await _execute_code(code)
                    _send({{"type": "execution_done", "success": ok}})

            if __name__ == "__main__":
                try:
                    asyncio.run(_main_loop())
                except KeyboardInterrupt:
                    pass
        ''')

    async def execute(
        self, code: str, tools: dict, timeout: int = 30, **kwargs
    ) -> ExecutionResult:
        """Execute code in a rootless Podman container.

        Container stays alive between calls. Variables persist in the global
        namespace. Tool calls proxied via JSON-framed RPC over stdin/stdout.
        """
        persistent = True  # always persistent for podman
        start_time = time.monotonic()
        tool_names = list(tools.keys())
        tool_calls: list[dict[str, Any]] = []
        process = None

        try:
            # ---- Build server metadata for baked discovery ----
            servers_by_name: dict[str, dict] = {}
            for tname, tfn in tools.items():
                if tname.startswith('_'):
                    continue
                schema = getattr(tfn, '_mcp_schema', None) or {"name": tname}
                server = schema.get("_server", "default")
                if server not in servers_by_name:
                    alias = server.lower().replace(" ", "_").replace("-", "_")
                    servers_by_name[server] = {"name": server, "alias": alias, "tools": []}
                servers_by_name[server]["tools"].append(schema)
            server_metadata_json = json.dumps(list(servers_by_name.values()), default=str)

            # ---- Start or reuse container ----
            if persistent and self._process and self._process.returncode is None:
                process = self._process
                logger.info("PODMAN | reusing persistent container (pid=%d)", process.pid)
            else:
                # Clean up old IPC dir
                if self._ipc_dir:
                    shutil.rmtree(self._ipc_dir, ignore_errors=True)

                ipc_dir = tempfile.mkdtemp(prefix="codemode_podman_")
                entrypoint = self._render_entrypoint(tool_names, server_metadata_json)
                Path(ipc_dir, "entrypoint.py").write_text(entrypoint)

                cmd = self._build_cmd(ipc_dir)
                logger.info(
                    "PODMAN | launching container (image=%s, memory=%s, pids=%d)",
                    self._image, self._memory, self._pids_limit,
                )

                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                self._process = process
                self._ipc_dir = ipc_dir

            # ---- Send execute command ----
            execute_msg = json.dumps({"type": "execute", "code": code}) + "\n"
            logger.info("PODMAN | → container stdin: execute (%d chars code)", len(code))
            logger.debug("PODMAN | → code: %s", code[:200])
            process.stdin.write(execute_msg.encode("utf-8"))
            await process.stdin.drain()

            # ---- Read output and handle RPC ----
            stdout_chunks: list[str] = []
            stderr_chunks: list[str] = []
            execution_success = True

            async def _handle_output():
                nonlocal execution_success
                async for raw_line in process.stdout:
                    try:
                        msg = json.loads(raw_line.decode())
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        stderr_chunks.append(raw_line.decode(errors="replace"))
                        continue

                    msg_type = msg.get("type")

                    if msg_type == "stdout":
                        stdout_chunks.append(msg.get("data", ""))
                        logger.debug("PODMAN | ← stdout: %s", msg.get("data", "")[:100])

                    elif msg_type == "stderr":
                        stderr_chunks.append(msg.get("data", ""))
                        logger.debug("PODMAN | ← stderr: %s", msg.get("data", "")[:100])

                    elif msg_type == "execution_done":
                        if not msg.get("success", True):
                            execution_success = False
                        logger.info("PODMAN | ← execution_done (success=%s)", msg.get("success", True))
                        break

                    elif msg_type == "rpc_request":
                        payload = msg.get("payload", {})
                        rpc_type = payload.get("type", "")
                        request_id = msg.get("id")

                        if rpc_type == "call_tool":
                            # ---- Tool call RPC ----
                            tool_name = payload.get("tool", "")
                            tool_args = payload.get("arguments", {})

                            logger.info(
                                "PODMAN | ← rpc_request #%s: %s(%s)",
                                request_id, tool_name,
                                json.dumps(tool_args, default=str)[:150],
                            )

                            tool_start = time.monotonic()
                            try:
                                fn = tools.get(tool_name)
                                if fn is None:
                                    raise ValueError(f"Tool '{tool_name}' not found")
                                if asyncio.iscoroutinefunction(fn):
                                    result = await fn(**tool_args)
                                else:
                                    result = fn(**tool_args)

                                tool_duration = time.monotonic() - tool_start
                                tool_calls.append({
                                    "name": tool_name, "kwargs": tool_args,
                                    "result": result, "success": True,
                                    "duration": tool_duration,
                                })

                                logger.info(
                                    "PODMAN | → rpc_response #%s: OK (%.3fs) %s",
                                    request_id, tool_duration,
                                    str(result)[:100],
                                )

                                reply = {
                                    "type": "rpc_response", "id": request_id,
                                    "payload": {"success": True, "result": result},
                                }
                            except Exception as exc:
                                tool_duration = time.monotonic() - tool_start
                                tool_calls.append({
                                    "name": tool_name, "kwargs": tool_args,
                                    "error": str(exc), "success": False,
                                    "duration": tool_duration,
                                })
                                logger.warning(
                                    "PODMAN | → rpc_response #%s: FAIL (%.3fs) %s",
                                    request_id, tool_duration, str(exc)[:150],
                                )
                                reply = {
                                    "type": "rpc_response", "id": request_id,
                                    "payload": {"success": False, "error": str(exc)},
                                }

                        elif rpc_type == "discover":
                            # ---- Discovery RPC (for tools loaded after container start) ----
                            logger.debug("PODMAN | ← rpc_request #%s: discover", request_id)
                            result = []
                            for tname, tfn in tools.items():
                                if tname.startswith('_'):
                                    continue
                                schema = getattr(tfn, '_mcp_schema', None) or {}
                                result.append({
                                    "name": tname,
                                    "description": (schema.get("description", "") or "")[:120],
                                    "server": schema.get("_server", ""),
                                })
                            reply = {
                                "type": "rpc_response", "id": request_id,
                                "payload": result,
                            }

                        elif rpc_type == "schema":
                            # ---- Schema RPC ----
                            tool_name = payload.get("tool", "")
                            logger.debug("PODMAN | ← rpc_request #%s: schema(%s)", request_id, tool_name)
                            fn = tools.get(tool_name)
                            if fn and hasattr(fn, '_mcp_schema'):
                                reply = {
                                    "type": "rpc_response", "id": request_id,
                                    "payload": fn._mcp_schema,
                                }
                            else:
                                reply = {
                                    "type": "rpc_response", "id": request_id,
                                    "payload": {"error": f"Tool '{tool_name}' not found"},
                                }

                        elif rpc_type == "search":
                            # ---- Search RPC ----
                            query = payload.get("query", "").lower()
                            logger.debug("PODMAN | ← rpc_request #%s: search(%s)", request_id, query)
                            result = []
                            for tname, tfn in tools.items():
                                if tname.startswith('_'):
                                    continue
                                schema = getattr(tfn, '_mcp_schema', None) or {}
                                desc = (schema.get("description", "") or "").lower()
                                if query in tname.lower() or query in desc:
                                    result.append({
                                        "name": tname,
                                        "description": (schema.get("description", "") or "")[:120],
                                    })
                            reply = {
                                "type": "rpc_response", "id": request_id,
                                "payload": result,
                            }

                        else:
                            logger.warning("PODMAN | ← unknown rpc type: %s", rpc_type)
                            reply = {
                                "type": "rpc_response", "id": request_id,
                                "payload": {"error": f"Unknown RPC type: {rpc_type}"},
                            }

                        reply_data = json.dumps(reply, default=str).encode("utf-8") + b"\n"
                        try:
                            process.stdin.write(reply_data)
                            await process.stdin.drain()
                        except (BrokenPipeError, ConnectionResetError, OSError):
                            rc = process.returncode
                            stderr_tail = ""
                            try:
                                stderr_tail = (await process.stderr.read(4096)).decode(errors="replace")
                            except Exception:
                                pass
                            raise RuntimeError(
                                f"Container died (exit={rc}, possible OOM with "
                                f"memory limit {self._memory}). stderr: {stderr_tail}"
                            )

            # ---- Execute with timeout ----
            output_task = asyncio.create_task(_handle_output())
            try:
                await asyncio.wait_for(output_task, timeout=timeout)
            except asyncio.TimeoutError:
                output_task.cancel()
                process.kill()
                await process.wait()
                self._process = None
                duration = time.monotonic() - start_time
                return ExecutionResult(
                    success=False,
                    error=f"Execution timed out after {timeout}s",
                    duration=duration,
                    tool_calls=tool_calls,
                )

            duration = time.monotonic() - start_time
            stdout_text = "".join(stdout_chunks)
            stderr_text = "".join(stderr_chunks)

            # Parse output: if it looks like JSON, parse it
            output = stdout_text
            if stdout_text.strip():
                try:
                    output = json.loads(stdout_text)
                except (json.JSONDecodeError, TypeError):
                    output = stdout_text

            return ExecutionResult(
                success=execution_success,
                output=output,
                error=stderr_text if stderr_text.strip() else None,
                duration=duration,
                tool_calls=tool_calls,
            )

        except FileNotFoundError:
            duration = time.monotonic() - start_time
            return ExecutionResult(
                success=False,
                error="Container runtime not found. Install podman or docker.",
                duration=duration,
            )
        except Exception as exc:
            duration = time.monotonic() - start_time
            # Kill process on error to avoid zombies
            if process and process.returncode is None:
                process.kill()
                await process.wait()
            self._process = None
            logger.exception("PODMAN | unexpected error")
            return ExecutionResult(
                success=False,
                error=f"{type(exc).__name__}: {exc}",
                duration=duration,
                tool_calls=tool_calls,
            )

    async def close(self) -> None:
        """Kill the container process and clean up the IPC directory."""
        if self._process and self._process.returncode is None:
            self._process.kill()
            await self._process.wait()
        self._process = None
        if self._ipc_dir:
            shutil.rmtree(self._ipc_dir, ignore_errors=True)
            self._ipc_dir = None

    def is_available(self) -> bool:
        if not self._runtime:
            return False
        try:
            import subprocess
            result = subprocess.run(
                [self._runtime, "version", "--format", "json"],
                capture_output=True, timeout=10,
            )
            return result.returncode == 0
        except Exception:
            return False

    def get_name(self) -> str:
        runtime_name = os.path.basename(self._runtime) if self._runtime else "podman"
        return f"podman ({runtime_name})"
