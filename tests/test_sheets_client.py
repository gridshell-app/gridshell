"""Conformance-style tests against a fake /mcp server, deterministic, no
real server/Sheets needed. Exercises the real request/response
envelope shape ({id, tool, params, timeoutMs} in, {id, result}/{id, error}
out).
"""

import json
import threading

import pytest
from websockets.sync.server import serve

from gridshell import GridShellConnectionError, GridShellError, GridShellTimeoutError, SheetsClient
from gridshell.transport import Connection


def _fake_handler(behaviors):
    """Returns a per-connection handler. `behaviors` maps a tool name to
    either a literal result value, or a callable(params) -> result, or the
    string "error:<message>", or "timeout" (never respond)."""

    def handler(ws):
        # First message on every connection is the auth frame
        # (see server.py's router() comment) and not a tool call, so it's
        # consumed and discarded here before the real request/response
        # loop starts, matching what a real server.py connection now does.
        try:
            ws.recv()
        except Exception:
            return
        for raw in ws:
            msg = json.loads(raw)
            tool = msg["tool"]
            behavior = behaviors.get(tool, "unhandled")
            if behavior == "timeout":
                continue
            if isinstance(behavior, str) and behavior.startswith("error:"):
                ws.send(json.dumps({"id": msg["id"], "error": behavior[len("error:") :]}))
                continue
            result = behavior(msg.get("params", {})) if callable(behavior) else behavior
            ws.send(json.dumps({"id": msg["id"], "result": result}))

    return handler


@pytest.fixture
def fake_server():
    servers = []

    def start(behaviors):
        server = serve(_fake_handler(behaviors), "localhost", 0)
        servers.append(server)
        port = server.socket.getsockname()[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return port

    yield start

    for server in servers:
        server.shutdown()


def test_token_rejection_surfaces_the_documented_message():
    # _describe_close() used to run on the send path only, the recv path
    # (the one that actually fires, since a send completes before the
    # server's close is processed) hardcoded a generic message instead.
    def handler(ws):
        try:
            ws.recv()  # auth frame
        except Exception:
            return
        ws.close(code=1008, reason="invalid or missing token")

    server = serve(handler, "localhost", 0)
    port = server.socket.getsockname()[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        grid = SheetsClient(port=port, default_timeout=2)
        with pytest.raises(GridShellConnectionError, match="Rejected by the server: invalid or missing token"):
            grid.get_values("A1:B2")
    finally:
        server.shutdown()


def test_get_values_explicit_range(fake_server):
    port = fake_server({"getValues": [[1, 2], [3, 4]]})
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        assert grid.get_values("A1:B2") == [[1, 2], [3, 4]]
    finally:
        grid.close()


def test_get_values_selection_routes_through_run_batch(fake_server):
    def run_batch(params):
        assert params["ops"][0]["chain"][0]["method"] == "getActiveRange"
        assert params["ops"][0]["chain"][1]["method"] == "getValues"
        # Real client's runBatch envelope shape, not a plain list.
        return {"status": "complete", "completed": 1, "total": 1, "results": [{"ok": True, "value": [["selected"]]}]}

    port = fake_server({"runBatch": run_batch})
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        assert grid.get_values() == [["selected"]]
    finally:
        grid.close()


def test_get_values_selection_raises_on_partial_batch(fake_server):
    port = fake_server(
        {
            "runBatch": {
                "status": "partial",
                "reason": "error",
                "completed": 0,
                "total": 1,
                "nextIndex": 0,
                "results": [],
                "error": {"index": 0, "message": "Nothing is selected", "stepIndex": 0, "sideEffectsApplied": False},
            }
        }
    )
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        with pytest.raises(GridShellError, match="Nothing is selected"):
            grid.get_values()
    finally:
        grid.close()


def test_set_values_selection_handles_missing_value_key(fake_server):
    # A fluent write (setValues) resolves to a Range object, which
    # the client can't serialize. JSON.stringify then
    # drops that "value" key entirely rather than sending it as null.
    port = fake_server(
        {"runBatch": {"status": "complete", "completed": 1, "total": 1, "results": [{"ok": True}]}}
    )
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        grid.set_values([["hello"]])  # must not raise KeyError
    finally:
        grid.close()


def test_get_values_selection_rejects_sheet_arg(fake_server):
    port = fake_server({})
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        with pytest.raises(ValueError):
            grid.get_values(sheet="Sheet2")
    finally:
        grid.close()


def test_set_values_and_append_row(fake_server):
    seen = {}

    def set_values(params):
        seen["set_values"] = params
        return None

    def append_row(params):
        seen["append_row"] = params
        return None

    port = fake_server({"setValues": set_values, "appendRow": append_row})
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        grid.set_values([[1, 2]], range="A1:B1", sheet="Data")
        grid.append_row([1, 2, 3], sheet="Data")
    finally:
        grid.close()
    assert seen["set_values"] == {"range": "A1:B1", "values": [[1, 2]], "sheet": "Data"}
    assert seen["append_row"] == {"values": [1, 2, 3], "sheet": "Data"}


def test_append_row_rejects_nested_list(fake_server):
    # Regression test: append_row([[3, 4]]) used to silently write
    # "[Ljava.lang.Object;@..." instead of erroring.
    port = fake_server({})
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        with pytest.raises(ValueError, match="flat list"):
            grid.append_row([[3, 4]])
    finally:
        grid.close()


def test_append_row_rejects_bare_string(fake_server):
    # Regression test: a string is iterable character-by-character, so
    # without this check it slips past the nested-list check above (no
    # element of a string is itself a list/tuple) and gets sent to the wire
    # as a bare string where an array is expected.
    port = fake_server({})
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        with pytest.raises(ValueError, match="not a list of values"):
            grid.append_row("hello")
    finally:
        grid.close()


def test_set_values_rejects_bare_string(fake_server):
    # Regression test: grid.set_values("hello from python") used to
    # silently explode into one cell per character (17 columns for a
    # 17-character string) instead of raising, since _normalize_2d's
    # flat-list acceptance didn't distinguish a real flat list from a
    # string being iterated character-by-character.
    port = fake_server({})
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        with pytest.raises(ValueError, match="not a list of values"):
            grid.set_values("hello from python", range="A1")
    finally:
        grid.close()


def test_set_values_accepts_flat_list_as_one_row(fake_server):
    seen = {}

    def set_values(params):
        seen["params"] = params
        return None

    port = fake_server({"setValues": set_values})
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        grid.set_values([1, 2, 3], range="A1:C1")
    finally:
        grid.close()
    assert seen["params"] == {"range": "A1:C1", "values": [[1, 2, 3]]}


def test_set_values_rejects_mixed_shape(fake_server):
    port = fake_server({})
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        with pytest.raises(ValueError, match="mix of rows"):
            grid.set_values([1, [2, 3]], range="A1:B1")
    finally:
        grid.close()


def test_server_error_raises_gridshell_error(fake_server):
    port = fake_server({"getValues": "error:Blocked by deny list"})
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        with pytest.raises(GridShellError, match="Blocked by deny list"):
            grid.get_values("A1:B2")
    finally:
        grid.close()


def test_call_escape_hatch_reaches_unlisted_tool(fake_server):
    port = fake_server({"someNewTool": {"ok": True}})
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        assert grid.call("someNewTool", {"x": 1}) == {"ok": True}
    finally:
        grid.close()


def test_session_defaults_from_gridshell_session_env_var(monkeypatch):
    # A script launched from inside a document's embedded terminal should
    # target that document automatically via this env var.
    monkeypatch.setenv("GRIDSHELL_SESSION", "sheet-abc123")
    conn = Connection(port=3012)
    assert conn.session == "sheet-abc123"
    assert conn._connect_auth["session"] == "sheet-abc123"


def test_explicit_session_overrides_env_var(monkeypatch):
    monkeypatch.setenv("GRIDSHELL_SESSION", "sheet-abc123")
    conn = Connection(port=3012, session="sheet-explicit")
    assert conn.session == "sheet-explicit"


def test_no_session_and_no_env_var_sends_empty_session(monkeypatch):
    monkeypatch.delenv("GRIDSHELL_SESSION", raising=False)
    conn = Connection(port=3012)
    assert conn._connect_auth["session"] == ""


def test_url_never_carries_session_or_token(monkeypatch):
    # Credentials travel as the first message on the connection
    # (see connect()/_connect_auth), never in the URL. A reverse proxy in
    # front of server.py logs the full request line by default, which
    # would put a shared secret (or the session id) in plain text in the
    # proxy's access log.
    monkeypatch.delenv("GRIDSHELL_SESSION", raising=False)
    conn = Connection(port=3012, session="sheet-abc123", token="sec ret&weird")
    assert conn.url == "ws://localhost:3012/mcp"
    assert conn._connect_auth == {"token": "sec ret&weird", "session": "sheet-abc123", "sessionKey": ""}


def test_no_token_sends_empty_string_in_auth_frame():
    conn = Connection(port=3012)
    assert conn.token == ""
    assert conn._connect_auth["token"] == ""


def test_token_has_no_auth_token_env_var_fallback(monkeypatch):
    # Unlike GRIDSHELL_AUTH_TOKEN (below), a plain AUTH_TOKEN (the env var
    # server.py itself reads to configure its own required token) isn't
    # meant to be picked up client-side. A script running next to a
    # server it isn't targeting shouldn't silently inherit that server's
    # own config var.
    monkeypatch.delenv("GRIDSHELL_AUTH_TOKEN", raising=False)
    monkeypatch.setenv("AUTH_TOKEN", "should-not-be-picked-up")
    conn = Connection(port=3012)
    assert conn.token == ""


def test_token_defaults_from_gridshell_auth_token_env_var(monkeypatch):
    # Set by server.py in a spawned shell's environment alongside
    # GRIDSHELL_SESSION/GRIDSHELL_SESSION_KEY whenever a token is
    # configured - lets a script run from inside that shell authenticate
    # with zero manual configuration, same pattern as session.
    monkeypatch.setenv("GRIDSHELL_AUTH_TOKEN", "shell-token")
    conn = Connection(port=3012)
    assert conn.token == "shell-token"


def test_explicit_token_overrides_env_var(monkeypatch):
    monkeypatch.setenv("GRIDSHELL_AUTH_TOKEN", "shell-token")
    conn = Connection(port=3012, token="explicit-token")
    assert conn.token == "explicit-token"


def test_timeout_raises_gridshell_timeout_error(fake_server):
    port = fake_server({"getValues": "timeout"})
    grid = SheetsClient(port=port, default_timeout=0.2)
    try:
        with pytest.raises(GridShellTimeoutError):
            grid.get_values("A1:B2")
    finally:
        grid.close()


def test_fetches_batch_time_budget_once_on_connect(fake_server):
    calls = []

    def budget(params):
        calls.append(params)
        return 4500

    port = fake_server({"getBatchTimeBudgetMs": budget, "getValues": [[1]]})
    grid = SheetsClient(port=port, default_timeout=2)
    try:
        grid.get_values("A1")
        grid.get_values("A1")
    finally:
        grid.close()
    assert len(calls) == 1
    assert grid._conn.default_timeout == pytest.approx(4.5 + 30)
