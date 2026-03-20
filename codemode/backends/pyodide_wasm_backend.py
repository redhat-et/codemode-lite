"""Actual Pyodide (WASM) sandbox backend.

Runs Python code inside Pyodide WebAssembly via Node.js subprocess.
Provides true memory isolation -- code runs in a separate WASM instance.

Requires:
    - Node.js installed and on PATH
    - npm package 'pyodide' installed: npm install pyodide

Tool calls cross the JS-Python bridge:
    Sandbox Python -> JS -> stdout JSON -> this backend -> real tool -> stdin JSON -> JS -> Sandbox Python
"""

import asyncio
import json
import logging
import subprocess
import time
from pathlib import Path
from typing import Any

from .base import ExecutionResult, SandboxBackend

logger = logging.getLogger("codemode.pyodide_wasm")

# Path to the Node.js runner script (lives alongside this module)
_RUNNER_PATH = str(Path(__file__).parent / "pyodide_runner.js")


class PyodideWasmBackend(SandboxBackend):
    """Sandbox backend using actual Pyodide WASM via Node.js.

    Each execution spawns a fresh Node.js process with Pyodide,
    providing true memory isolation through WebAssembly.

    Protocol (newline-delimited JSON over stdin/stdout):

        1. Backend spawns ``node pyodide_runner.js``
        2. Runner prints ``{"type": "ready"}`` once Pyodide is loaded.
        3. Backend sends ``{"type": "execute", "code": "...", "tools": [...]}``
        4. Runner may print one or more ``{"type": "tool_call", "id": N, "name": "...", "args": {...}}``
           -- backend resolves each by calling the real tool, then writes
              ``{"type": "tool_result", "id": N, "result": ...}`` back on stdin.
        5. Runner prints ``{"type": "result", "success": true/false, "output": ..., "error": ...}``
    """

    def __init__(self, node_path: str = "node", timeout: int = 30) -> None:
        """Initialise the Pyodide WASM backend.

        Args:
            node_path: Path (or name) of the Node.js binary.
            timeout:   Default execution timeout in seconds.
        """
        self._node_path = node_path
        self._default_timeout = timeout

    # ------------------------------------------------------------------
    # SandboxBackend interface
    # ------------------------------------------------------------------

    def get_name(self) -> str:
        return "pyodide-wasm"

    def is_available(self) -> bool:
        """Check if Node.js and the ``pyodide`` npm package are reachable."""
        import shutil

        if not shutil.which(self._node_path):
            logger.debug("Node.js binary not found on PATH (%s)", self._node_path)
            return False

        # Verify that the pyodide npm package can be required
        # Use the project root (two levels up from this file) as cwd
        # so Node.js can find node_modules/ installed there
        runner_dir = str(Path(__file__).parent.parent.parent)
        try:
            result = subprocess.run(
                [self._node_path, "-e", "require('pyodide')"],
                capture_output=True,
                timeout=10,
                cwd=runner_dir,
            )
            if result.returncode != 0:
                logger.debug(
                    "pyodide npm package not available: %s",
                    result.stderr.decode()[:200],
                )
            return result.returncode == 0
        except Exception as exc:
            logger.debug("is_available check failed: %s", exc)
            return False

    async def execute(
        self, code: str, tools: dict, timeout: int = 30, **kwargs
    ) -> ExecutionResult:
        """Execute Python code inside a Pyodide WASM sandbox.

        Args:
            code:    Python source code (should define ``async def main()``).
            tools:   Mapping of tool-name to async/sync callable.
            timeout: Maximum wall-clock seconds for the entire execution
                     (including Pyodide boot and all tool calls).

        Returns:
            An ``ExecutionResult`` describing the outcome.
        """
        start = time.monotonic()
        tool_names = list(tools.keys())
        tool_calls: list[dict[str, Any]] = []
        process: asyncio.subprocess.Process | None = None

        try:
            # ---- 1. Spawn Node.js with the Pyodide runner ----------------
            # Set cwd to project root so Node.js can find node_modules/pyodide
            project_root = str(Path(__file__).parent.parent.parent)
            process = await asyncio.create_subprocess_exec(
                self._node_path,
                _RUNNER_PATH,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=project_root,
            )

            logger.info(
                "PYODIDE WASM | spawned Node.js process (pid=%d)", process.pid
            )

            # ---- 2. Wait for the "ready" signal --------------------------
            # Pyodide boot can be slow on first run (downloads WASM); allow
            # up to 60 s just for initialisation.
            boot_timeout = max(timeout, 60)
            ready_line = await asyncio.wait_for(
                process.stdout.readline(), timeout=boot_timeout
            )
            if not ready_line:
                stderr_out = await _drain_stderr(process)
                return ExecutionResult(
                    success=False,
                    error=f"Pyodide runner exited before ready. stderr: {stderr_out}",
                    duration=time.monotonic() - start,
                )

            ready = _parse_json_line(ready_line)
            if ready is None or ready.get("type") != "ready":
                return ExecutionResult(
                    success=False,
                    error=f"Pyodide failed to initialise: {ready_line.decode().strip()[:300]}",
                    duration=time.monotonic() - start,
                )

            boot_elapsed = time.monotonic() - start
            logger.info(
                "PYODIDE WASM | runtime ready (%.2fs boot)", boot_elapsed
            )

            # ---- 3. Send the execute command -----------------------------
            logger.info("PYODIDE WASM | → container stdin: execute (%d chars code)", len(code))
            logger.debug("PYODIDE WASM | → code: %s", code[:200])
            execute_cmd = json.dumps(
                {"type": "execute", "code": code, "tools": tool_names}
            ) + "\n"
            process.stdin.write(execute_cmd.encode())
            await process.stdin.drain()

            # ---- 4. Tool-call proxy loop ---------------------------------
            # Collect concurrent tool_call messages and execute them
            # in parallel using asyncio.gather, so asyncio.gather()
            # inside the generated code actually runs tools concurrently.
            remaining = timeout - (time.monotonic() - start)
            pending_calls: list[dict] = []

            while remaining > 0:
                # Read next line (or batch — short timeout for batching)
                line = await asyncio.wait_for(
                    process.stdout.readline(), timeout=remaining
                )
                if not line:
                    break

                msg = _parse_json_line(line)
                if msg is None:
                    logger.warning(
                        "PYODIDE WASM | non-JSON line from runner: %s",
                        line.decode().strip()[:200],
                    )
                    remaining = timeout - (time.monotonic() - start)
                    continue

                msg_type = msg.get("type")

                if msg_type == "tool_call":
                    pending_calls.append(msg)

                    # Check if more tool_calls are immediately available
                    # (asyncio.gather fires multiple calls at once)
                    while True:
                        try:
                            next_line = await asyncio.wait_for(
                                process.stdout.readline(), timeout=0.05
                            )
                            if not next_line:
                                break
                            next_msg = _parse_json_line(next_line)
                            if next_msg and next_msg.get("type") == "tool_call":
                                pending_calls.append(next_msg)
                            elif next_msg and next_msg.get("type") == "result":
                                # Edge case: result came before we processed calls
                                pending_calls.clear()
                                msg = next_msg
                                msg_type = "result"
                                break
                            else:
                                break
                        except asyncio.TimeoutError:
                            break  # No more pending calls in the buffer

                    if msg_type == "result":
                        pass  # Fall through to result handler below
                    elif pending_calls:
                        # Execute all pending tool calls in parallel
                        if len(pending_calls) > 1:
                            logger.info(
                                "PYODIDE WASM | %d concurrent tool calls → parallel execution",
                                len(pending_calls),
                            )

                        async def _exec_one(call_msg):
                            return await self._handle_tool_call(
                                call_msg, tools, tool_calls, timeout=remaining
                            )

                        results = await asyncio.gather(
                            *[_exec_one(c) for c in pending_calls],
                            return_exceptions=True,
                        )

                        # Send all results back
                        for r in results:
                            if isinstance(r, Exception):
                                r = {"type": "tool_result", "id": 0,
                                     "result": {"error": str(r)}}
                            response = json.dumps(r, default=str) + "\n"
                            process.stdin.write(response.encode())
                        await process.stdin.drain()
                        pending_calls.clear()

                        remaining = timeout - (time.monotonic() - start)
                        continue

                # -- result: final output from Pyodide ---------------------
                if msg_type == "result":
                    duration = time.monotonic() - start
                    success = msg.get("success", False)
                    logger.info("PYODIDE WASM | ← execution_done (%s in %.2fs)",
                                "OK" if success else "FAIL", duration)

                    if success:
                        return ExecutionResult(
                            success=True,
                            output=msg.get("output"),
                            duration=duration,
                            tool_calls=tool_calls,
                        )
                    else:
                        error_msg = msg.get("error", "Unknown error")
                        return ExecutionResult(
                            success=False,
                            error=error_msg,
                            duration=duration,
                            tool_calls=tool_calls,
                        )

                else:
                    logger.debug(
                        "PYODIDE WASM | ignoring unknown message type: %s",
                        msg_type,
                    )

                remaining = timeout - (time.monotonic() - start)

            # -- Fell through: either timeout or stdout closed unexpectedly
            stderr_out = await _drain_stderr(process)
            duration = time.monotonic() - start

            if remaining <= 0:
                _kill_process(process)
                return ExecutionResult(
                    success=False,
                    error=f"Pyodide execution timed out after {timeout}s",
                    duration=duration,
                    tool_calls=tool_calls,
                )

            return ExecutionResult(
                success=False,
                error=(
                    f"Pyodide process exited without result. "
                    f"stderr: {stderr_out[:500]}"
                ),
                duration=duration,
                tool_calls=tool_calls,
            )

        except asyncio.TimeoutError:
            duration = time.monotonic() - start
            _kill_process(process)
            logger.error(
                "PYODIDE WASM | TIMEOUT after %.2fs (limit %ds)",
                duration,
                timeout,
            )
            return ExecutionResult(
                success=False,
                error=f"Pyodide execution timed out after {timeout}s",
                duration=duration,
                tool_calls=tool_calls,
            )

        except Exception as exc:
            duration = time.monotonic() - start
            _kill_process(process)
            logger.exception("PYODIDE WASM | unexpected error")
            return ExecutionResult(
                success=False,
                error=f"Pyodide backend error: {exc}",
                duration=duration,
                tool_calls=tool_calls,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _handle_tool_call(
        self,
        msg: dict,
        tools: dict,
        tool_calls: list,
        *,
        timeout: float,
    ) -> dict:
        """Resolve a single tool-call request from the runner.

        Calls the real tool implementation, records the result in
        *tool_calls*, and returns the JSON-serialisable response dict
        to send back to the runner.
        """
        tool_name = msg["name"]
        tool_args = msg.get("args", {})
        call_id = msg["id"]

        logger.info(
            "PYODIDE WASM | ← rpc_request #%s: %s(%s)",
            call_id, tool_name,
            json.dumps(tool_args, default=str)[:150],
        )

        tool_start = time.monotonic()

        try:
            fn = tools.get(tool_name)
            if fn is None:
                raise ValueError(f"Tool '{tool_name}' not found")

            # Invoke the real tool (async or sync)
            # Extract positional args if the WASM proxy sent them
            positional = tool_args.pop("__positional_args__", None) or []
            if asyncio.iscoroutinefunction(fn):
                result = await asyncio.wait_for(
                    fn(*positional, **tool_args), timeout=timeout
                )
            else:
                result = fn(*positional, **tool_args)

            tool_duration = time.monotonic() - tool_start

            tool_calls.append(
                {
                    "name": tool_name,
                    "args": tool_args,
                    "result": result,
                    "duration": tool_duration,
                    "success": True,
                }
            )

            logger.info(
                "PYODIDE WASM | → rpc_response #%s: OK (%.3fs) %s",
                call_id, tool_duration, str(result)[:100],
            )

            return {
                "type": "tool_result",
                "id": call_id,
                "result": result,
            }

        except Exception as exc:
            tool_duration = time.monotonic() - tool_start

            tool_calls.append(
                {
                    "name": tool_name,
                    "args": tool_args,
                    "error": str(exc),
                    "duration": tool_duration,
                    "success": False,
                }
            )

            logger.warning(
                "PYODIDE WASM | → rpc_response #%s: FAIL (%.3fs) %s",
                call_id, tool_duration, str(exc)[:150],
            )

            return {
                "type": "tool_result",
                "id": call_id,
                "result": {"error": str(exc)},
            }


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _parse_json_line(line: bytes) -> dict | None:
    """Attempt to parse a single stdout line as JSON.

    Returns ``None`` if parsing fails.
    """
    try:
        return json.loads(line.decode().strip())
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _kill_process(process: asyncio.subprocess.Process | None) -> None:
    """Kill the subprocess if it is still running."""
    if process is None:
        return
    try:
        process.kill()
    except ProcessLookupError:
        pass  # already exited


async def _drain_stderr(process: asyncio.subprocess.Process) -> str:
    """Read whatever is available on stderr without blocking forever."""
    try:
        data = await asyncio.wait_for(process.stderr.read(), timeout=2)
        return data.decode(errors="replace")
    except (asyncio.TimeoutError, Exception):
        return "<stderr unavailable>"
