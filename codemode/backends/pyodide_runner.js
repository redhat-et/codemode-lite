/**
 * pyodide_runner.js - Runs Python code inside actual Pyodide (WebAssembly).
 *
 * This script is spawned as a subprocess by the Python host process.
 * It loads the Pyodide WASM runtime, receives Python code + tool definitions
 * via stdin (JSON), executes the code inside Pyodide, and proxies tool calls
 * back to the Python host over stdin/stdout.
 *
 * Protocol (JSON-RPC style over stdin/stdout, one JSON object per line):
 *
 * INCOMING (Python host -> this script's stdin):
 *   {"type": "execute", "code": "async def main(): ...", "tools": ["tool-name", ...], "timeout": 30}
 *   {"type": "tool_result", "id": 1, "result": {...}}
 *
 * OUTGOING (this script's stdout -> Python host):
 *   {"type": "ready"}
 *   {"type": "tool_call", "id": 1, "name": "tool-name", "args": {...}}
 *   {"type": "result", "success": true, "output": ..., "stdout": "...", "duration": 1.23}
 *   {"type": "result", "success": false, "error": "...", "duration": 0.5}
 *
 * Usage:
 *   node pyodide_runner.js
 *
 * Requires: npm install pyodide
 */

const { loadPyodide } = require("pyodide");
const readline = require("readline");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Write a JSON object as a single line to stdout.
 * Avoids console.log which adds extra newlines and formatting.
 */
function writeLine(obj) {
    process.stdout.write(JSON.stringify(obj) + "\n");
}

/**
 * Write a diagnostic/debug message to stderr (never interferes with protocol).
 */
function debug(msg) {
    process.stderr.write("[pyodide_runner] " + msg + "\n");
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
    // ------------------------------------------------------------------
    // 1. Load Pyodide WASM runtime
    // ------------------------------------------------------------------
    let pyodide;
    try {
        pyodide = await loadPyodide();
    } catch (err) {
        writeLine({
            type: "result",
            success: false,
            error: "Failed to load Pyodide: " + (err.message || String(err)),
            duration: 0,
        });
        process.exit(1);
    }

    // Signal to the host that we are ready to accept commands.
    writeLine({ type: "ready" });

    // ------------------------------------------------------------------
    // 2. Set up line-oriented stdin reader
    // ------------------------------------------------------------------
    const rl = readline.createInterface({
        input: process.stdin,
        terminal: false,
    });

    // Queue of lines received on stdin. We resolve waiting promises as
    // lines arrive, and buffer any lines that arrive before someone waits.
    const lineBuffer = [];
    const lineWaiters = [];

    rl.on("line", (line) => {
        if (lineWaiters.length > 0) {
            const resolve = lineWaiters.shift();
            resolve(line);
        } else {
            lineBuffer.push(line);
        }
    });

    rl.on("close", () => {
        // stdin closed — resolve any pending waiters with null so they can
        // detect EOF and exit gracefully.
        while (lineWaiters.length > 0) {
            const resolve = lineWaiters.shift();
            resolve(null);
        }
    });

    /**
     * Returns a promise that resolves to the next line from stdin,
     * or null if stdin has been closed.
     */
    function readLine() {
        if (lineBuffer.length > 0) {
            return Promise.resolve(lineBuffer.shift());
        }
        return new Promise((resolve) => {
            lineWaiters.push(resolve);
        });
    }

    // ------------------------------------------------------------------
    // 3. Wait for the "execute" command
    // ------------------------------------------------------------------
    const rawLine = await readLine();
    if (rawLine === null) {
        // stdin closed before we got a command
        process.exit(0);
    }

    let input;
    try {
        input = JSON.parse(rawLine);
    } catch (parseErr) {
        writeLine({
            type: "result",
            success: false,
            error: "Invalid JSON on stdin: " + (parseErr.message || String(parseErr)),
            duration: 0,
        });
        process.exit(1);
    }

    if (input.type !== "execute") {
        writeLine({
            type: "result",
            success: false,
            error: "Expected 'execute' command, got '" + input.type + "'",
            duration: 0,
        });
        process.exit(1);
    }

    const code = input.code;
    const toolNames = input.tools || [];
    const timeoutSec = input.timeout || 30;

    // ------------------------------------------------------------------
    // 4. Build the tool-call bridge (JS <-> Python host)
    //
    // When Python code inside Pyodide calls a tool, the flow is:
    //   Python (Pyodide) -> JS bridge function -> stdout JSON tool_call
    //   -> Python host reads, invokes real MCP tool, writes tool_result
    //   -> stdin JSON tool_result -> JS resolves promise -> Pyodide Python
    // ------------------------------------------------------------------
    let nextCallId = 0;
    const pendingCalls = {}; // id -> resolve function

    /**
     * Called by the tool bridge when Python code calls a tool.
     * Sends a tool_call to stdout and returns a promise that resolves
     * when the host sends back a tool_result with the matching id.
     */
    async function callTool(name, argsJson) {
        const id = ++nextCallId;
        const args = argsJson ? JSON.parse(argsJson) : {};

        writeLine({ type: "tool_call", id, name, args });

        return new Promise((resolve, reject) => {
            // Set up a per-call timeout so we don't hang forever
            const timer = setTimeout(() => {
                delete pendingCalls[id];
                reject(new Error(
                    "Tool call '" + name + "' (id=" + id + ") timed out after " + timeoutSec + "s"
                ));
            }, timeoutSec * 1000);

            pendingCalls[id] = (result) => {
                clearTimeout(timer);
                resolve(JSON.stringify(result));
            };
        });
    }

    // Background loop: read tool_result messages from stdin and dispatch
    // them to the corresponding pending promise.
    function startToolResultReader() {
        (async () => {
            while (true) {
                const line = await readLine();
                if (line === null) break; // stdin closed

                try {
                    const msg = JSON.parse(line);
                    if (msg.type === "tool_result" && msg.id != null && pendingCalls[msg.id]) {
                        pendingCalls[msg.id](msg.result);
                        delete pendingCalls[msg.id];
                    }
                } catch (_) {
                    // Ignore malformed lines
                }
            }
        })();
    }

    startToolResultReader();

    // Register the bridge as a JS module importable from Python inside Pyodide
    pyodide.registerJsModule("_tool_bridge", {
        call: callTool,
        names: toolNames,
    });

    // ------------------------------------------------------------------
    // 5. Execute the Python code inside Pyodide
    // ------------------------------------------------------------------
    const startTime = Date.now();

    // Build the wrapper code that:
    //   a) Sets up tool proxy functions in a `tools` dict
    //   b) Captures stdout from print() calls
    //   c) Runs the user code (which must define async def main())
    //   d) Stores the result in _result
    const wrappedCode = `
import json as _json
import sys as _sys
import io as _io

# ------------------------------------------------------------------
# Capture stdout so print() output can be returned to the host
# ------------------------------------------------------------------
_captured_stdout = _io.StringIO()
_original_stdout = _sys.stdout
_sys.stdout = _captured_stdout

# ------------------------------------------------------------------
# Build async tool proxies
# ------------------------------------------------------------------
import _tool_bridge

async def _make_tool_caller(_name):
    async def _call(*args, **kwargs):
        if args:
            kwargs['__positional_args__'] = list(args)
        result_json = await _tool_bridge.call(_name, _json.dumps(kwargs))
        result_str = result_json if isinstance(result_json, str) else str(result_json)
        return _json.loads(result_str)
    return _call

tools = {}
for _tn in _tool_bridge.names:
    _tn_str = str(_tn)
    tools[_tn_str] = await _make_tool_caller(_tn_str)

# ------------------------------------------------------------------
# Block JS interop — prevents escaping WASM sandbox via Node.js APIs
# (from js import require → require('fs'), require('child_process'), etc.)
# ------------------------------------------------------------------
for _blocked in ('js', 'pyodide_js', 'pyodide'):
    _sys.modules[_blocked] = None

# ------------------------------------------------------------------
# Make common stdlib modules available as globals
# (matches restricted exec backend which injects SAFE_MODULES into namespace)
# ------------------------------------------------------------------
import asyncio, re, math, datetime, collections, itertools, functools, json

# ------------------------------------------------------------------
# Execute user code
# ------------------------------------------------------------------
${code}

# ------------------------------------------------------------------
# Call main() and capture result
# ------------------------------------------------------------------
_result = await main()

# Restore stdout
_sys.stdout = _original_stdout
_captured_stdout_text = _captured_stdout.getvalue()
`;

    try {
        // Create a timeout race
        const executionPromise = pyodide.runPythonAsync(wrappedCode);
        const timeoutPromise = new Promise((_, reject) => {
            setTimeout(() => {
                reject(new Error("Execution timed out after " + timeoutSec + "s"));
            }, timeoutSec * 1000);
        });

        await Promise.race([executionPromise, timeoutPromise]);

        // Extract result
        const rawResult = pyodide.globals.get("_result");
        let output;
        if (rawResult == null) {
            output = null;
        } else if (rawResult.toJs) {
            try {
                output = rawResult.toJs({ dict_converter: Object.fromEntries });
            } catch (_) {
                // Fallback: convert via JSON round-trip inside Python
                try {
                    const jsonStr = pyodide.runPython("_json.dumps(_result)");
                    output = JSON.parse(jsonStr);
                } catch (__) {
                    output = String(rawResult);
                }
            }
        } else {
            output = rawResult;
        }

        // Extract captured stdout
        let capturedStdout = "";
        try {
            capturedStdout = pyodide.globals.get("_captured_stdout_text");
            if (capturedStdout && capturedStdout.toJs) {
                capturedStdout = capturedStdout.toJs();
            }
            capturedStdout = capturedStdout || "";
        } catch (_) {
            capturedStdout = "";
        }

        const duration = (Date.now() - startTime) / 1000;
        writeLine({
            type: "result",
            success: true,
            output: output,
            stdout: capturedStdout,
            duration: duration,
        });
    } catch (err) {
        const duration = (Date.now() - startTime) / 1000;
        let errorMsg = err.message || String(err);

        // If it's a PythonError, try to extract just the traceback
        if (err.type) {
            errorMsg = err.type + ": " + errorMsg;
        }

        writeLine({
            type: "result",
            success: false,
            error: errorMsg,
            stdout: "",
            duration: duration,
        });
    }

    // ------------------------------------------------------------------
    // 6. Cleanup and exit
    // ------------------------------------------------------------------
    rl.close();
    process.exit(0);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
main().catch((err) => {
    writeLine({
        type: "result",
        success: false,
        error: "Fatal error: " + (err.message || String(err)),
        duration: 0,
    });
    process.exit(1);
});
