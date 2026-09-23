# gridshell

Everything needed to self-host a [GridShell](https://gridshell.app) backend and drive a live, open Google Sheets spreadsheet from Python or an AI CLI agent, in one `pip install`:

- **`gridshell-server`**: the terminal-server itself: a PTY + Sheets bridge + MCP relay, spoken over plain WebSocket (`/terminal`, `/host`, `/mcp`).
- **`gridshell-mcp`**: an MCP stdio server that relays tool calls to a running `gridshell-server`'s `/mcp` endpoint. Register this with any MCP-compatible agent (Claude Code, Antigravity, Codex, OpenCode, etc.).
- **`SheetsClient`**: a Python client library speaking the same `/mcp` WebSocket protocol directly, for scripts and REPLs - no MCP SDK, no stdio, no LLM involved.

## Requirements

- Python 3.10+ (forced by the `mcp` SDK dependency, which has never supported below 3.10).
- The Sheets side wired up separately: this package doesn't include the Google Sheets add-on itself (Apps Script, installed via the Sheets sidebar) - only the backend it talks to.

## Install

```bash
pip install gridshell
```

This installs the `gridshell` library plus the `gridshell-server`/`gridshell-mcp` console commands.

## Running the server

```bash
gridshell-server [--port N] [--host HOST] [--idle-hours H] [--buffer-mb M]
                  [--wss --cert-path FILE --key-path FILE] [--auth-token TOKEN]
                  [--no-auth-token] [--regenerate-token] [--copy-token] [--allow-root]
                  [--allow-origin HOST_SUFFIX[,HOST_SUFFIX...]] [--max-message-mb M]
```

Defaults to `ws://localhost:3000`. Full flag reference, deployment shapes, and the security model: [Server](https://gridshell.app/library/server/).

A token is required by default, even for plain local use. Left unset, one is auto-generated on first run, persisted to `~/.gridshell/token`, and copied to your clipboard rather than printed to the console - paste it once into the Sidebar's **Auth token** field. No clipboard available (a headless box, an SSH session)? The server starts normally regardless and prints the file path instead - read it from there. `--no-auth-token` disables the check entirely - not recommended unless access is restricted some other way.

Reachable locally with zero setup by default. Reaching it from another machine needs either a reverse proxy in front (recommended) or `--host 0.0.0.0`/`--wss` for direct exposure - see [Server: Security & deployment](https://gridshell.app/library/server/#security-deployment) for all three deployment shapes and how to harden a remote one.

**A word of caution with untrusted agents or documents**: the spawned shell inherits this process's full environment, including any secrets you've set as environment variables. An agent that reads instructions from an untrusted document, or one you don't fully trust generally, shouldn't be pointed at a server whose environment holds anything sensitive - see [Shell privileges](https://gridshell.app/library/server/#shell-privileges).

Stopping the server with Ctrl+C (or a plain `kill` on Linux/Mac) cleanly terminates every shell it's currently running. A hard crash or a forceful kill (Task Manager "End Task", `taskkill /F`, `kill -9`) does not - no code runs in that case, so a shell that was active at that moment can keep running as an orphaned process, invisible to a freshly started server. If you hit this, find and end it manually (Task Manager / `ps` + `kill`) - there's no in-app way to recover it.

## Registering the MCP relay

```bash
gridshell-mcp [--port N] [--wss] [--auth-token TOKEN]
```

`--port` must match the `gridshell-server` you're relaying to (default 3000). No `--auth-token` is needed when launched from inside a document's own embedded terminal: the token, the session id, and its per-shell key are all picked up automatically from the environment.

- `getValues` - read a range.
- `setValues` - write a 2D array of values into a range.
- `appendRow` - append one row to the end of a sheet's data.
- `runBatch` - everything else: formulas, formatting, charts, multiple operations in one call.
- `runBatchGuide` - reference only, no arguments: returns `runBatch`'s full usage guide (builder-value pattern, existing-object lookups, worked examples).

Full tool descriptions, the `runBatch` chain grammar, and a config-file-based registration example (for clients like OpenCode that don't support a CLI `add` command): [MCP](https://gridshell.app/library/mcp/).

To add the `gridshell-mcp` server to your MCP client, follow the instructions for your client. For example for Claude Code:

```bash
claude mcp add gridshell-sheets -- gridshell-mcp --port 3010
```

**If you want to add your own tools**, don't edit the installed copy of `gridshell/mcp_grid.py` in place - a future `pip install --upgrade gridshell` overwrites it and your tool disappears. Copy the file out to your own location, edit that copy, and point your MCP config entry at it instead.

## Using the Python client

No configuration is needed to target the right document or authenticate when running from inside a document's own embedded terminal - pass `port=` only if the server isn't on the default 3000, and `SheetsClient()` picks up which document to talk to, and the credentials to use, from the environment automatically. Outside of that (a standalone script, a different machine), see [Python Client](https://gridshell.app/library/python-client/) for the full set of constructor options.

```python
from gridshell import SheetsClient

grid = SheetsClient(port=3012)          # or host=, wss=True, session=<id>
grid.get_values("A1:C10")
grid.set_values([[1, 2], [3, 4]], range="A1:B2")
grid.set_values([1, 2, 3], range="A1:C1")      # a flat list is also accepted, as one row
grid.append_row(["Widget", 12, "2026-09-06"])
grid.close()
```

Or as a context manager:

```python
with SheetsClient(port=3012) as grid:
    rows = grid.get_values("A1:C10")
```

### Working with the current selection

Omit `range` to read or write whatever's currently selected in the sheet, instead of naming an A1 range:

```python
grid.get_values()                 # current selection's values
grid.set_values([[1, 2, 3]])       # overwrite the current selection
```

`sheet=` isn't valid together with an omitted `range` - the selection is always on whichever sheet is currently active in the UI, so "the selection on a different sheet" isn't a coherent request. Pass an explicit `range` (and optionally `sheet`) to target a specific sheet instead.

### Anything beyond the built-in methods

`run_batch()` is the generic structured-chain escape hatch - the same grammar `gridshell-mcp` describes to an LLM (`chain`/`__chain`/`index`/`chartId` steps resolved starting from `SpreadsheetApp`), for anything the dedicated methods above don't cover (formulas, formatting, charts, multiple ops in one call, ...):

```python
grid.run_batch([
    {"chain": [
        {"method": "getActiveSheet", "args": []},
        {"method": "getRange", "args": ["D14"]},
        {"method": "setFormula", "args": ["=D14*(1+Assumptions!$B$5)"]},
    ]},
])
```

`call(tool, params)` goes one level further - invoke any tool by name, including ones added to the server's tool list after this library shipped:

```python
grid.call("someNewTool", {"foo": "bar"})
```

## Errors

Raised by any client built on the shared `Connection` (today, that's just `SheetsClient`, but none of these are Sheets-specific):

- `GridShellConnectionError`: couldn't connect, or the connection dropped mid-call.
- `GridShellTimeoutError`: no response within the call's timeout.
- `GridShellError`: the server (or the spreadsheet it relayed to) returned an error, e.g. a deny-list block or a failed `runBatch` step. The message is passed through unchanged.

All three subclass `GridShellException`.

## Scope

`SheetsClient` is deliberately narrow, on purpose: one raw escape hatch (`call`) plus a handful of convenience methods on top, no plugin/registration system. Compose your own functions out of these primitives for anything more specific - same philosophy as `runBatch` itself.

Only Sheets is implemented. `SheetsClient` is named explicitly (not a bare `Client`) so an Excel host can be added later, sharing the same `/mcp` transport, without a breaking rename of this one.

## Development

```bash
pip install -e ".[dev]"
pytest
```

`tests/test_sheets_client.py` runs against a fake in-process `/mcp`-shaped WebSocket server - no real `gridshell-server` or spreadsheet needed. `tests/test_conformance.py` exercises `gridshell.server` itself as a black box over real WebSocket connections, against a fake PTY (no real shell) - validates the `/terminal` + `/host` + `/mcp` wire protocol against the JSON Schemas in `schemas/`. `tests/test_mcp_grid.py` covers `build_runtime_config()` and the MCP tool-argument validation in `mcp_grid.py`.

## More

This package (`gridshell`) is the backend for the GridShell Sheets add-on, and is open source under the [MIT License](LICENSE) - so you can read exactly what it does, adapt it to your own security requirements, or build your own tools on top of it. The add-on itself, distributed only through the Google Workspace Marketplace, is a separate, closed-source component not included in this repository - see [License](https://gridshell.app/legal/license/) for its terms. See [Contributing](CONTRIBUTING.md) before opening a pull request, and [Security Policy](SECURITY.md) to report a vulnerability privately.

GridShell is provided as-is, with no warranty of any kind - see the [Terms of Service](https://gridshell.app/legal/terms-of-service/) for the full terms. Full documentation, including the Sheets add-on side: [docs-site](https://gridshell.app/).

Code of conduct is pending - not yet drafted.
