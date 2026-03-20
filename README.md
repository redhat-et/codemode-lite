# codemode-lite

An MCP server that lets AI agents execute Python code inside a secure sandbox. Instead of exposing 30+ individual tools to the agent, it exposes one tool — `run_python` — and the agent writes Python code that calls all the tools from inside the sandbox.

## Prerequisites

### Required
- **Python 3.11+**
- **pip packages**: `mcp`, `aiohttp`

### Backend (pick one)

**Podman** (recommended) — persistent container, top-level await, server-scoped proxies
```bash
# macOS
brew install podman
podman machine init
podman machine start

# Linux
sudo apt install podman   # or dnf, pacman, etc.
```

**Pyodide WASM** — ephemeral WASM sandbox, no container needed
```bash
# Install in the codemode-lite directory (required)
cd codemode-lite && npm install pyodide
```

### MCP servers
You need at least one MCP server running for codemode to connect to. These are the external services (Calendar, GitHub, etc.) whose tools become available inside the sandbox.

## Setup

### 1. Install dependencies

```bash
cd codemode-lite
pip install -r requirements.txt
```

### 2. Configure MCP servers

Create a directory for your MCP server configs (default: `~/MCPs`):

```bash
mkdir -p ~/MCPs
```

Add JSON config files — one server per file or multiple in one file:

```bash
# SSE server
cat > ~/MCPs/calendar.json << 'EOF'
{
  "mcpServers": {
    "Calendar": {
      "type": "sse",
      "url": "http://localhost:3001/sse"
    }
  }
}
EOF

# stdio server
cat > ~/MCPs/converter.json << 'EOF'
{
  "mcpServers": {
    "Converter": {
      "command": "python3",
      "args": ["-m", "unit_converter"]
    }
  }
}
EOF

# Multiple servers in one file
cat > ~/MCPs/all.json << 'EOF'
{
  "mcpServers": {
    "Calendar": {"type": "sse", "url": "http://localhost:3001/sse"},
    "GitHub": {"type": "sse", "url": "http://localhost:3003/sse"},
    "Serper": {"type": "sse", "url": "http://localhost:3002/sse"}
  }
}
EOF
```

### 3. Add to your MCP client

Add codemode-lite to your `.mcp.json` (Claude Code, Cursor, etc.):

```json
{
  "mcpServers": {
    "codemode-lite": {
      "command": "python3",
      "args": ["/path/to/codemode-lite/server.py"],
      "env": {
        "CODEMODE_BACKEND": "podman",
        "CODEMODE_MCPS_DIR": "~/MCPs",
        "CODEMODE_LOG_FILE": "/tmp/codemode-lite.log",
        "CODEMODE_LOG_LEVEL": "INFO"
      }
    }
  }
}
```

### 4. Restart your MCP client

The agent will see one tool: `run_python`.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `CODEMODE_BACKEND` | `podman` | `podman` or `pyodide-wasm` |
| `CODEMODE_MCPS_DIR` | `~/MCPs` | Directory with MCP server JSON configs |
| `CODEMODE_TIMEOUT` | `120` | Execution timeout in seconds |
| `CODEMODE_LOG_FILE` | — | Log file path (optional) |
| `CODEMODE_LOG_LEVEL` | `WARNING` | Python logging level |
| `CODEMODE_SERVERS` | — | JSON array of server configs (overrides MCPS_DIR) |

## How It Works

1. MCP client starts `server.py` as a child process
2. Server reads `*.json` files from `$CODEMODE_MCPS_DIR`, connects to each MCP server
3. Server exposes a single `run_python` tool with all discovered tool names in the description
4. Agent writes Python code → server sends it to the sandbox → tool calls proxy back to real MCP servers via RPC → result returned to agent

```
Agent LLM
  │ writes Python code
  ▼
run_python(code="...")
  │
  ▼
Codemode MCP Server
  │ sends code to sandbox
  ▼
┌─────────────────────────────┐
│  Podman Container           │
│  (or Pyodide WASM)          │
│                             │
│  tools['list-events'](...)  │──── RPC ────► Calendar MCP :3001
│  tools['search_repos'](...)  │──── RPC ────► GitHub MCP :3003
│  print(results)             │
└─────────────────────────────┘
  │
  ▼
Result returned to agent
```

## Usage

Once configured, the agent writes code like this:

```python
# Top-level await — no wrapper function needed (Podman backend)
import json

# Discover available tools
tools_list = await _discover()
print(tools_list)

# Get a tool's schema before calling it
schema = await _schema('create-event')
print(schema)

# Call tools
events = await tools['list-events'](calendarId='primary')
repos = await tools['search_repositories'](query='my-project')

# Parallel execution
import asyncio
results = await asyncio.gather(
    tools['list-events'](calendarId='primary'),
    tools['search_repositories'](query='test')
)

# Variables persist between calls (Podman only)
print(json.dumps(events, indent=2))
```

### Discovery helpers

Available inside the sandbox:

| Function | Description |
|---|---|
| `await _discover()` | List all available tools with name and description |
| `await _schema('tool-name')` | Get the full input schema for a tool |
| `await _search('keyword')` | Search tools by name or description |

### Server-scoped proxies (Podman only)

```python
# Instead of tools['list-events'], group by server:
await mcp_calendar.list_events(calendarId='primary')
await mcp_github.search_repositories(query='test')
```

## Backend Comparison

| | Podman | Pyodide WASM |
|---|---|---|
| Persistent state | Yes — variables survive between calls | No — fresh every call |
| Top-level await | Yes | No — needs `async def main()` |
| Boot time | ~2s first call, instant after | ~0.7s every call |
| Isolation | Kernel-level container | WASM memory boundary |
| Server proxies | Yes — `mcp_calendar.*()` | No |
| Discovery | Baked metadata (zero-RPC) + RPC fallback | RPC only |
| Requires | Podman or Docker | Node.js + npm pyodide |

## Container Security (Podman)

```
--network none                    no network access
--read-only                       immutable filesystem
--cap-drop ALL                    all Linux capabilities dropped
--security-opt no-new-privileges  no privilege escalation
--user 65534:65534                unprivileged user
--memory 512m                     memory limit
--pids-limit 128                  process limit
```

Credentials never enter the sandbox. Tool calls are proxied via RPC — the sandbox sends a request, the host calls the real MCP server, and sends back the result.

## Logs

```bash
# Watch live logs
tail -f /tmp/codemode-lite.log

# Logs show: server startup, tool discovery, code execution, RPC calls, errors
```

## File Structure

```
codemode-lite/
├── server.py                          MCP server, tool registration
├── requirements.txt                   mcp + aiohttp
├── codemode/
│   ├── engine.py                      CodeMode class, run_code(), discovery helpers
│   ├── proxy.py                       Tool call routing, logging, response unwrapping
│   ├── mcp_adapter.py                 MCPToolLoader — connects to MCP servers (SSE + stdio)
│   └── backends/
│       ├── base.py                    SandboxBackend interface, ExecutionResult
│       ├── podman_backend.py          Rootless Podman container with persistent namespace
│       ├── pyodide_wasm_backend.py    Pyodide WASM via Node.js
│       └── pyodide_runner.js          Node.js Pyodide bootstrap script
```
