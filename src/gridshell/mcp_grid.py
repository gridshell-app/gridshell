"""GridShell's MCP relay - a stdio MCP server for any MCP-compatible agent
(Claude Code, Antigravity, Codex, OpenCode etc.). Installed automatically with
`pip install gridshell`, alongside server.py and the SheetsClient Python
client. Run via the `gridshell-mcp` console command.

Connects to server.py's /mcp, forwards MCP tool calls, correlates
responses by id. Point this at whatever port/protocol/token server.py was
started with via --port/--wss/--auth-token (and PORT/AUTH_TOKEN env vars)
- server.py requires a token by default (auto-generated if --auth-token
isn't passed). Run from inside a spreadsheet's embedded terminal,
this needs no manual token configuration at all. server.py hands the
running shell GRIDSHELL_AUTH_TOKEN automatically, the same auto-pickup as
GRIDSHELL_SESSION/GRIDSHELL_SESSION_KEY. For example:

  claude mcp add gridshell-sheets -- gridshell-mcp --port 3010

If you want to add your own tools, don't edit this installed copy in
place, a future `pip install --upgrade gridshell` overwrites it and your
tool disappears. Copy this file out to your own location, edit that copy,
and point your MCP config entry at it instead.

Dependencies are installed automatically with `pip install gridshell`.
"""
import asyncio
import json
import os
import random
import string
import sys

import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed


USAGE = """gridshell-mcp [--port N] [--wss] [--auth-token TOKEN]

  --port N            Port the target gridshell-server is listening on
                      (default 3000, or $PORT)
  --wss               Connect over wss:// instead of ws://
  --auth-token TOKEN  Required whenever the target server was started with
                      --auth-token (or $AUTH_TOKEN)
  --help, -h          Show this message and exit
"""


def resolve_flags():
    argv = sys.argv[1:]
    use_wss = False
    # Hand-rolled: a handful of flags in a small copy-and-edit script,
    # argparse would be overkill for something meant to stay this simple.
    value_flags = {"--port": "port", "--auth-token": "auth_token"}
    values = {}
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in ("--help", "-h"):
            print(USAGE)
            sys.exit(0)
        elif arg == "--wss":
            use_wss = True
        elif arg in value_flags:
            if i + 1 >= len(argv):
                sys.exit("%s requires a value" % arg)
            values[value_flags[arg]] = argv[i + 1]
            i += 1
        else:
            sys.exit("Unknown option: %s" % arg)
        i += 1
    if "port" in values:
        try:
            port = int(values["port"])
        except ValueError:
            sys.exit("--port must be a number, got %r" % values["port"])
    else:
        port = int(os.environ.get("PORT", 3000))
    auth_token = (
        values.get("auth_token")
        or os.environ.get("AUTH_TOKEN")
        # Auto-pickup for a client launched inside a GridShell shell.
        or os.environ.get("GRIDSHELL_AUTH_TOKEN")
    )
    return port, use_wss, auth_token


def build_runtime_config():
    # Called from run(), not at import. Importing this module could
    # parse sys.argv and sys.exit() on a bogus one. Matches
    # server.py's own resolve_flags(), called from run() there too.
    port, use_wss, auth_token = resolve_flags()
    # Set by server.py when launched inside a spreadsheet's embedded terminal
    # - routes calls to that document. Unset when run standalone.
    session_id = os.environ.get("GRIDSHELL_SESSION", "")
    # Proves this process owns session_id, not just names it. See server.py's
    # spawn_process()/handle_mcp().
    session_key = os.environ.get("GRIDSHELL_SESSION_KEY", "")
    scheme = "wss" if use_wss else "ws"
    # No query string - credentials travel as the first message instead (see
    # HostConnection.ensure_connected), not the URL.
    url = "%s://localhost:%d/mcp" % (scheme, port)
    connect_auth = {"token": auth_token, "session": session_id, "sessionKey": session_key}
    return url, connect_auth


# Bootstrap-only fallback, until the real BATCH_TIME_BUDGET_MS is fetched.
DEFAULT_TIMEOUT_MS = 5 * 60 * 1000
# Margin so the timeoutMs sent to the server clears runBatch's real budget.
BUDGET_MARGIN_MS = 30 * 1000
# Extra margin for this process's own asyncio.wait_for, so it doesn't race
# the server's own timeout message.
CLIENT_MARGIN_MS = 20 * 1000
# Matches server.py's DEFAULT_MAX_MESSAGE_BYTES, see transport.py's own
# copy of this comment for why it reads $MAX_MESSAGE_MB too.
MAX_WS_MESSAGE_BYTES = int(float(os.environ.get("MAX_MESSAGE_MB", 64)) * 1024 * 1024)


def generate_id():
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=16))


class HostConnection:
    """Owns the persistent /mcp WebSocket connection - connects lazily,
    reconnects automatically on close, and correlates responses to calls by
    id."""

    def __init__(self, url, connect_auth):
        self.url = url
        self.connect_auth = connect_auth
        self.ws = None
        self.pending = {}  # id -> asyncio.Future
        self.effective_timeout_ms = DEFAULT_TIMEOUT_MS
        self._connect_lock = asyncio.Lock()

    async def ensure_connected(self):
        if self.ws is not None:
            return
        async with self._connect_lock:
            if self.ws is not None:
                return
            self.ws = await ws_connect(self.url, max_size=MAX_WS_MESSAGE_BYTES)
            # Credentials go first, before anything else, see server.py's router().
            await self.ws.send(json.dumps(self.connect_auth))
            sys.stderr.write("[mcp-grid] Connected to terminal server\n")
            asyncio.create_task(self._read_loop())
            asyncio.create_task(self._fetch_batch_time_budget())

    async def _read_loop(self):
        ws = self.ws
        close_message = "Connection to terminal server was closed"
        try:
            async for raw in ws:
                try:
                    m = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                fut = self.pending.pop(m.get("id"), None)
                if fut is not None and not fut.done():
                    fut.set_result(m)
        except Exception as exc:
            sys.stderr.write("[mcp-grid] WebSocket error: %s\n" % exc)
            # A rejected/missing token closes with code 1008 and a specific
            # reason (server.py's router()). Surface it instead of a
            # generic message that points at the socket layer, not the token.
            rcvd = getattr(exc, "rcvd", None) if isinstance(exc, ConnectionClosed) else None
            if rcvd is not None and rcvd.reason:
                close_message = "Rejected by the server: %s" % rcvd.reason
        finally:
            sys.stderr.write("[mcp-grid] Disconnected, retrying in 3s\n")
            if self.ws is ws:
                self.ws = None
            for fut in list(self.pending.values()):
                if not fut.done():
                    fut.set_exception(RuntimeError(close_message))
            self.pending.clear()
            asyncio.create_task(self._reconnect_forever())

    async def _reconnect_forever(self):
        # A single failed attempt could kill retries silently (the
        # exception goes nowhere). Loops until a connection actually lands.
        while self.ws is None:
            await asyncio.sleep(3)
            try:
                await self.ensure_connected()
            except Exception as exc:
                sys.stderr.write("[mcp-grid] Reconnect failed (%s), retrying in 3s\n" % exc)

    async def _fetch_batch_time_budget(self):
        # Replaces the DEFAULT_TIMEOUT_MS guess with the server's real budget.
        try:
            budget_ms = await self.call("getBatchTimeBudgetMs", {}, DEFAULT_TIMEOUT_MS)
        except Exception as exc:
            sys.stderr.write("[mcp-grid] Could not fetch batch time budget, using default: %s\n" % exc)
            return
        if isinstance(budget_ms, (int, float)) and budget_ms > 0:
            self.effective_timeout_ms = budget_ms + BUDGET_MARGIN_MS

    async def call(self, tool_name, params, timeout_ms_override=None):
        await self.ensure_connected()
        if self.ws is None:
            raise RuntimeError("Not connected to terminal server")
        server_timeout_ms = timeout_ms_override if timeout_ms_override is not None else self.effective_timeout_ms
        call_id = generate_id()
        fut = asyncio.get_running_loop().create_future()
        self.pending[call_id] = fut
        await self.ws.send(json.dumps({
            "id": call_id, "tool": tool_name, "params": params or {}, "timeoutMs": server_timeout_ms,
        }))
        try:
            m = await asyncio.wait_for(fut, timeout=(server_timeout_ms + CLIENT_MARGIN_MS) / 1000)
        except asyncio.TimeoutError:
            self.pending.pop(call_id, None)
            raise RuntimeError("Timeout waiting for response from Sheets")
        if "error" in m:
            raise RuntimeError(m["error"])
        return m.get("result")


# ── Tool definitions ────────────────────────────────────────────────────────
#
# runBatch is a general-purpose tool: it runs one or more SpreadsheetApp
# method calls, submitted as data rather than code. The other tools below
# are shortcuts for the highest-frequency single ops.

RUNBATCH_DESCRIPTION = (
    "Run one or more operations against the active Google Sheets spreadsheet, as data, not code: each op is a *chain* of method calls resolved in order starting from SpreadsheetApp. Example - set D14's formula:\n"
    '  {"ops": [{"chain": [{"method": "getActiveSheet", "args": []}, {"method": "getRange", "args": ["D14"]}, {"method": "setFormula", "args": ["=D14*(1+Assumptions!$B$5)"]}]}]}\n'
    "Submit many ops in one call, each run in order against the live sheet. A read op's result comes back in that op's slot; a write op's result is typically null. On partial failure, retry only from `nextIndex`, not the whole batch - `sideEffectsApplied`/`stepIndex`/`lastValue` tell you what already applied within the failed chain before assuming it was a no-op. No loops/arbitrary JS - express repetition as multiple ops, and prefer one whole-range call (getRange(\"A2:F26\").setValues([[...]])) over one op per cell/row.\n"
    "Conditional formatting, charts, and pivot tables need a value *built* separately and passed in as an argument - via a { \"__chain\": [...] } marker, a DIFFERENT key from a top-level op's own plain \"chain\" - and some chart methods (e.g. setChartType) need a { \"__enum\": \"...\" } marker instead of a plain string. Call the runBatchGuide tool (no arguments) for the full pattern and worked examples before your first attempt at any of these, or after any runBatch error you don't understand.\n"
    "At most 500 ops per call."
)

RUNBATCH_GUIDE = (
    "# runBatch usage guide: building values as arguments, existing-object lookups, enums, and worked examples for conditional formatting, charts, and pivot tables.\n\n"
    "## Building a value as an argument (__chain)\n"
    "Some methods (conditional formatting, charts, pivot tables) need a value built separately and passed in as an argument, not just the chain's own progressing target - e.g. a range object handed into addRange(), or a rule object handed into setConditionalFormatRules([...]). Use a { \"__chain\": [...] } marker in place of that argument - it's resolved as its own chain first, and the result is passed in as that argument. Nestable as deep as the builder pattern needs.\n\n"
    "THIS IS NOT THE SAME KEY as a top-level op's own \"chain\" (no underscores). Mixing them up is the most common mistake: an argument object with \"chain\" instead of \"__chain\" is rejected with a clear error rather than silently doing nothing.\n\n"
    "setConditionalFormatRules/getConditionalFormatRules are sheet-level (replace/read the whole rule list), not range-level.\n\n"
    "Example - highlight B2:B10 red where the value exceeds 100:\n"
    '  {"ops": [{"chain": [{"method": "getActiveSheet", "args": []}, {"method": "setConditionalFormatRules", "args": [[{"__chain": [{"method": "newConditionalFormatRule", "args": []}, {"method": "whenNumberGreaterThan", "args": [100]}, {"method": "setBackground", "args": ["#ff0000"]}, {"method": "setRanges", "args": [[{"__chain": [{"method": "getActiveSheet", "args": []}, {"method": "getRange", "args": ["B2:B10"]}]}]]}, {"method": "build", "args": []}]}]]}]}]}\n\n'
    "## Acting on an existing object (index / chartId)\n"
    "To act on a specific existing object already on the sheet (not one this call is creating), a chain step can be {\"index\": N} instead of {\"method\",\"args\"} - it indexes into the array the previous step returned, the same way sheet.getCharts()[0] would in ordinary code. There is no cross-call handle for these objects: list-and-inspect, then act by index in a follow-up call (re-fetch the array each time - indices aren't stable if something reorders it in between).\n\n"
    "Example - read the title of the second chart on the active sheet:\n"
    '  {"ops": [{"chain": [{"method": "getActiveSheet", "args": []}, {"method": "getCharts", "args": []}, {"index": 1}, {"method": "getOptions", "args": []}, {"method": "get", "args": ["title"]}]}]}\n\n'
    "Charts specifically also have a real, stable id - {\"chartId\": \"...\"} finds the chart with that id in the previous step's array (more robust than index, since a chart's id doesn't change if others are added/removed/reordered). Read a chart's id with getChartId(). To modify an existing chart in place: resolve it (by index or chartId), call .modify(), make changes, then .build() - that alone does NOT save the change, chain a final getActiveSheet().updateChart(...) step with the built chart as its argument.\n\n"
    "Example - retitle an existing chart found by id:\n"
    '  {"ops": [{"chain": [{"method": "getActiveSheet", "args": []}, {"method": "updateChart", "args": [{"__chain": [{"method": "getActiveSheet", "args": []}, {"method": "getCharts", "args": []}, {"chartId": "123456789"}, {"method": "modify", "args": []}, {"method": "setOption", "args": ["title", "Updated title"]}, {"method": "build", "args": []}]}]}]}]}\n\n'
    "(Cosmetic chart options - colors, fonts, stroke/fill - are Pro-tier-reserved and rejected on the free tier; structural options like the title above are not.)\n\n"
    "## Enum constants (__enum)\n"
    "A few methods need an actual enum constant, not a JSON-representable value - e.g. EmbeddedChartBuilder.setChartType() rejects a plain string like \"COLUMN\" or even \"Charts.ChartType.COLUMN\" (still just a string either way). Use a { \"__enum\": \"Charts.ChartType.COLUMN\" } marker (double underscore, same convention as __chain) in place of that argument - a bare \"enum\" key without the underscores is rejected with a clear error rather than silently doing nothing.\n\n"
    "Supported paths:\n"
    "- Charts.ChartType.*: AREA, BAR, BUBBLE, CANDLESTICK, COLUMN, COMBO, GAUGE, GEO, HISTOGRAM, LINE, ORG, PIE, RADAR, SCATTER, SPARKLINE, STEPPED_AREA, TABLE, TIMELINE, TREEMAP, WATERFALL\n"
    "- SpreadsheetApp.PivotTableSummarizeFunction.*: SUM, COUNTA, COUNT, COUNTUNIQUE, AVERAGE, MAX, MIN, MEDIAN, PRODUCT, STDEV, STDEVP, VAR, VARP, CUSTOM\n\n"
    "An unrecognized path is rejected with a clear error rather than silently resolving to something else.\n\n"
    "Example - insert a new column chart of B1:C13 at cell E2:\n"
    '  {"ops": [{"chain": [{"method": "getActiveSheet", "args": []}, {"method": "insertChart", "args": [{"__chain": [{"method": "getActiveSheet", "args": []}, {"method": "newChart", "args": []}, {"method": "setChartType", "args": [{"__enum": "Charts.ChartType.COLUMN"}]}, {"method": "addRange", "args": [{"__chain": [{"method": "getActiveSheet", "args": []}, {"method": "getRange", "args": ["B1:C13"]}]}]}, {"method": "setPosition", "args": [2, 5, 0, 0]}, {"method": "build", "args": []}]}]}]}]}\n\n'
    "## Pivot tables\n"
    "Pivot tables are built across several ops in one call, since each of PivotTable's own methods (addRowGroup, addPivotValue, ...) returns a different object, not the pivot table itself - re-fetch it via getPivotTables()[0] in each following op.\n\n"
    "Example - pivot A1:C6 (headers in row 1) at E1, grouped by column 1 and summing column 3:\n"
    '  {"ops": ['
    '{"chain": [{"method": "getActiveSheet", "args": []}, {"method": "getRange", "args": ["A1:C6"]}, {"method": "createPivotTable", "args": [{"__chain": [{"method": "getActiveSheet", "args": []}, {"method": "getRange", "args": ["E1"]}]}]}]}, '
    '{"chain": [{"method": "getActiveSheet", "args": []}, {"method": "getPivotTables", "args": []}, {"index": 0}, {"method": "addRowGroup", "args": [1]}]}, '
    '{"chain": [{"method": "getActiveSheet", "args": []}, {"method": "getPivotTables", "args": []}, {"index": 0}, {"method": "addPivotValue", "args": [3, {"__enum": "SpreadsheetApp.PivotTableSummarizeFunction.SUM"}]}]}'
    ']}\n'
)

TOOLS = [
    types.Tool(
        name="runBatch",
        description=RUNBATCH_DESCRIPTION,
        input_schema={
            "type": "object",
            "properties": {
                "ops": {
                    "type": "array",
                    "description": "Operations to run in order, in a single call.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "chain": {
                                "type": "array",
                                "description": "Steps resolved in sequence starting from SpreadsheetApp. Each step is a method call ({method, args}), or, to reach into an array the previous step returned, an index lookup ({index}) or a chart id lookup ({chartId}).",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "method": {"type": "string", "description": "Method name to call on the current target."},
                                        "args": {"type": "array", "description": "Arguments for this call, in order."},
                                        "index": {"type": "number", "description": "Index into the array the previous step returned (e.g. into getCharts()'s result)."},
                                        "chartId": {"type": "string", "description": "Find the chart with this id in the array the previous step returned (read via getChartId())."},
                                    },
                                },
                            },
                        },
                        "required": ["chain"],
                    },
                },
            },
            "required": ["ops"],
        },
    ),
    types.Tool(
        name="runBatchGuide",
        description="Full runBatch usage guide: the __chain/__enum builder-value pattern, acting on existing objects (index/chartId), and worked examples for conditional formatting, charts, and pivot tables. Call this (no arguments) before your first attempt at any of those, or after any runBatch error you don't understand - it returns the guide text directly, no arguments needed and no live sheet call made.",
        input_schema={"type": "object", "properties": {}},
    ),
    types.Tool(
        name="getValues",
        description="Read values from a range on the active spreadsheet (or a named sheet). Equivalent to runBatch's getRange(range).getValues() chain - a lighter-weight shortcut for the single most common read. Use runBatch instead for anything beyond a plain read (formulas, number formats, multiple ranges in one call, etc.).",
        input_schema={
            "type": "object",
            "properties": {
                "range": {"type": "string", "description": 'A1 notation, e.g. "A1:C10".'},
                "sheet": {"type": "string", "description": "Sheet name. Defaults to the active sheet if omitted."},
            },
            "required": ["range"],
        },
    ),
    types.Tool(
        name="setValues",
        description="Write a 2D array of values into a range on the active spreadsheet (or a named sheet). Equivalent to runBatch's getRange(range).setValues(values) chain - a lighter-weight shortcut for the single most common write. Use runBatch instead for formulas, formatting, or anything combined with other operations in one call.",
        input_schema={
            "type": "object",
            "properties": {
                "range": {"type": "string", "description": 'A1 notation, e.g. "A1:C10" - must match the shape of values.'},
                "values": {
                    "type": "array",
                    "description": "2D array of rows, e.g. [[1,2],[3,4]].",
                    "items": {"type": "array"},
                },
                "sheet": {"type": "string", "description": "Sheet name. Defaults to the active sheet if omitted."},
            },
            "required": ["range", "values"],
        },
    ),
    types.Tool(
        name="appendRow",
        description='Append one row of values to the end of the active sheet\'s data (or a named sheet) - Sheets figures out where the data currently ends and adds the row right after it. Equivalent to runBatch\'s appendRow(values) chain - a shortcut since finding "the next empty row" would otherwise take an extra read op.',
        input_schema={
            "type": "object",
            "properties": {
                "values": {"type": "array", "description": 'One row\'s values in order, e.g. ["Widget", 12, "2026-09-03"].'},
                "sheet": {"type": "string", "description": "Sheet name. Defaults to the active sheet if omitted."},
            },
            "required": ["values"],
        },
    ),
]

# Set by main() before the server handles any tool calls.
connection = None
_TOOLS_BY_NAME = {tool.name: tool for tool in TOOLS}

# The MCP SDK's low-level Server doesn't validate arguments against a
# tool's input_schema on its own. Shallow and dependency-free: checks
# required fields and each top-level property's declared type. Doesn't
# recurse into op/chain shape, as the client
# already rejects a malformed op with a proper envelope.
_JSON_TYPE_CHECKS = {
    "string": lambda v: isinstance(v, str),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
    "array": lambda v: isinstance(v, list),
    "object": lambda v: isinstance(v, dict),
}


def _validate_against_schema(schema, args):
    if not isinstance(args, dict):
        return "arguments must be a JSON object, got %s" % type(args).__name__
    for field in schema.get("required", []):
        if field not in args:
            return "missing required field %r" % field
    properties = schema.get("properties", {})
    for field, value in args.items():
        prop_schema = properties.get(field)
        if prop_schema is None:
            continue
        expected_type = prop_schema.get("type")
        check = _JSON_TYPE_CHECKS.get(expected_type)
        if check is not None and not check(value):
            return "field %r must be of type %r, got %s" % (field, expected_type, type(value).__name__)
    return None


async def on_list_tools(ctx, params):
    return types.ListToolsResult(tools=TOOLS)


async def on_call_tool(ctx, params):
    name = params.name
    args = params.arguments or {}
    # Static reference text, not a spreadsheet operation - answered locally,
    # no live connection/session needed.
    if name == "runBatchGuide":
        return types.CallToolResult(content=[types.TextContent(type="text", text=RUNBATCH_GUIDE)])
    tool = _TOOLS_BY_NAME.get(name)
    if tool is not None:
        validation_error = _validate_against_schema(tool.input_schema, args)
        if validation_error is not None:
            return types.CallToolResult(
                content=[types.TextContent(type="text", text="Error: %s" % validation_error)],
                is_error=True,
            )
    try:
        result = await connection.call(name, args)
        if result is None:
            text = "Done"
        elif isinstance(result, str):
            text = result
        else:
            text = json.dumps(result, indent=2)
        return types.CallToolResult(content=[types.TextContent(type="text", text=text)])
    except Exception as exc:
        return types.CallToolResult(content=[types.TextContent(type="text", text="Error: %s" % exc)], is_error=True)


async def main():
    global connection
    url, connect_auth = build_runtime_config()
    connection = HostConnection(url, connect_auth)
    server = Server(
        "gridshell-mcp",
        version="1.0.0b1",
        on_list_tools=on_list_tools,
        on_call_tool=on_call_tool,
    )
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def run():
    """Entry point for the `gridshell-mcp` console command."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()
