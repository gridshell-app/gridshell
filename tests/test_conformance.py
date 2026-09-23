"""Conformance suite for the /terminal + /host + /mcp wire protocol.
Tests server.py as a black box over real WebSocket connections (never
touches TerminalHub/HostBridge internals beyond what create_app()
exposes for assertions), so it
validates both a future reimplementation AND that a refactor didn't
silently change behavior. Every /terminal message received is also
checked against schemas/terminal-server-message.schema.json, and the
/host + /mcp envelopes against their own schemas, so a shape regression
fails here too.

Uses a fake PtyProcess (monkeypatched over pywinpty), not a real shell. 
Mirrors pywinpty's own interface (spawn/write/setwinsize/terminate/read), 
see server.py's own _shell_display_path/spawn_process for the real interface 
this stands infor.

Run: pip install -e ".[dev]"
     pytest tests/test_conformance.py
"""
import asyncio
import json
import os
import queue
import subprocess
import sys
import threading
import time

import jsonschema
import pytest
from websockets.asyncio.server import serve
from websockets.sync.client import connect as ws_connect

from gridshell import server as server_module

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "..", "schemas")


def load_schema(name):
    with open(os.path.join(SCHEMA_DIR, name), "r", encoding="utf-8") as f:
        return json.load(f)


def wait_until(condition, timeout=2.0, interval=0.01):
    # Polls instead of a fixed sleep. Resolves as soon as the condition is
    # true (usually much faster than a worst-case-sized sleep) while still
    # tolerating a slow/loaded machine up to timeout.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(interval)
    raise AssertionError("condition not met within %.1fs" % timeout)


SERVER_MESSAGE_SCHEMA = load_schema("terminal-server-message.schema.json")
REQUEST_ENVELOPE_SCHEMA = load_schema("request-envelope.schema.json")
RESPONSE_ENVELOPE_SCHEMA = load_schema("response-envelope.schema.json")


def assert_valid(schema, msg, label):
    validator = jsonschema.Draft202012Validator(schema)
    errors = list(validator.iter_errors(msg))
    if errors:
        raise AssertionError("Invalid %s: %r - %s" % (label, msg, errors))


# ── Fake PTY adapter ─────────────────────────────────────────────────────
class FakePtyProcess:
    """Mirrors pywinpty's PtyProcess interface. Echoes whatever's written
    straight back as output (via a blocking queue read() the reader thread
    polls) - enough to exercise session-level logic without a real shell."""

    instances = []  # exposed so tests can assert on kill()/resize() calls

    def __init__(self):
        self.killed = False
        self.resize_calls = []
        self._queue = queue.Queue()
        FakePtyProcess.instances.append(self)

    @classmethod
    def spawn(cls, *args, **kwargs):
        return cls()

    def write(self, data):
        self._queue.put(data)

    def setwinsize(self, rows, cols):
        self.resize_calls.append((cols, rows))

    def terminate(self, force=False):
        self.killed = True
        self._queue.put(None)  # unblocks a pending read() with EOFError
        return True

    def read(self, size=4096):
        item = self._queue.get()
        if item is None:
            raise EOFError("closed")
        return item


@pytest.fixture(autouse=True)
def reset_fake_instances():
    FakePtyProcess.instances = []
    yield


# ── Test server harness ─────────────────────────────────────────────────
class ServerHandle:
    def __init__(self, idle_hours=12, buffer_mb=8, required_token=None,
                 allowed_origin_suffixes=(), max_message_bytes=None):
        self.idle_hours = idle_hours
        self.buffer_mb = buffer_mb
        self.required_token = required_token
        self.allowed_origin_suffixes = allowed_origin_suffixes
        self.max_message_bytes = max_message_bytes
        self.port = None
        self.terminal_hub = None
        self.host_bridge = None
        self._loop = None
        self._ws_server = None
        self._thread = None
        self._sockets = []

    def start(self):
        ready = threading.Event()
        error_box = []

        def run():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            async def boot():
                router, hub, bridge = server_module.create_app(
                    self.idle_hours, self.buffer_mb,
                    required_token=self.required_token,
                    allowed_origin_suffixes=self.allowed_origin_suffixes,
                    max_message_bytes=self.max_message_bytes,
                )
                self.terminal_hub = hub
                self.host_bridge = bridge
                # max_size also has to reach serve() itself (mirrors
                # main()), create_app() alone doesn't enforce the wire limit.
                serve_kwargs = {}
                if self.max_message_bytes is not None:
                    serve_kwargs["max_size"] = self.max_message_bytes
                self._ws_server = await serve(router, "localhost", 0, **serve_kwargs)
                self.port = self._ws_server.sockets[0].getsockname()[1]
                ready.set()
                await self._ws_server.wait_closed()

            try:
                loop.run_until_complete(boot())
            except Exception as exc:
                error_box.append(exc)
                ready.set()
            finally:
                loop.close()

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        if not ready.wait(timeout=5):
            raise RuntimeError("test server did not start in time")
        if error_box:
            raise error_box[0]

    def connect(self, path, origin=None, auth=None):
        # Sends the auth frame (token/session/sessionKey) as the first
        # message after connecting - the server rejects any connection
        # without one. auth=None sends an empty frame. Send is
        # best-effort: an Origin-rejected connection may already be
        # closed server-side, those tests assert on recv()/close_code.
        ws = self.connect_raw(path, origin=origin)
        try:
            ws.send(json.dumps(auth if auth is not None else {}))
        except Exception:
            pass
        return ws

    def connect_raw(self, path, origin=None):
        # No auth frame sent - for tests exercising the auth-frame gate
        # itself (malformed/missing first message).
        kwargs = {"legacy": True}
        if origin is not None:
            kwargs["origin"] = origin
        ws = ws_connect("ws://localhost:%d%s" % (self.port, path), **kwargs)
        self._sockets.append(ws)
        return ws

    def close(self):
        for ws in self._sockets:
            try:
                ws.close()
            except Exception:
                pass
        if self._ws_server is not None and self._loop is not None:
            # Server.close() is synchronous (schedules the actual shutdown;
            # boot()'s own await ws_server.wait_closed() is what actually
            # resolves once that finishes), call_soon_threadsafe, not
            # run_coroutine_threadsafe, since there's no coroutine here.
            self._loop.call_soon_threadsafe(self._ws_server.close)
        if self._thread is not None:
            self._thread.join(timeout=5)


@pytest.fixture
def server(monkeypatch):
    monkeypatch.setattr(server_module, "PtyProcess", FakePtyProcess)
    handle = ServerHandle()
    handle.start()
    yield handle
    handle.close()


@pytest.fixture
def make_server(monkeypatch):
    # Factory version of `server` above, for tests that need a
    # non-default ServerHandle (a token, an origin allow-list, a message
    # size cap). Closes every handle it created, same as `server` does.
    monkeypatch.setattr(server_module, "PtyProcess", FakePtyProcess)
    handles = []

    def _make(**kwargs):
        handle = ServerHandle(**kwargs)
        handle.start()
        handles.append(handle)
        return handle

    yield _make
    for handle in handles:
        handle.close()


# ── /terminal test client ────────────────────────────────────────────────
class TerminalClient:
    def __init__(self, ws):
        self.ws = ws
        self.messages = []

    def send(self, obj):
        self.ws.send(json.dumps(obj))

    def wait_for(self, predicate, timeout=2.0, description="message"):
        for m in self.messages:
            if predicate(m):
                return m
        end = time.time() + timeout
        while time.time() < end:
            try:
                raw = self.ws.recv(timeout=min(0.2, max(0.05, end - time.time())))
            except TimeoutError:
                continue
            except Exception:
                break
            m = json.loads(raw)
            assert_valid(SERVER_MESSAGE_SCHEMA, m, "/terminal server message")
            self.messages.append(m)
            if predicate(m):
                return m
        raise TimeoutError("Timed out waiting for %s" % description)

    def wait_for_type(self, type_, timeout=2.0):
        return self.wait_for(lambda m: m.get("type") == type_, timeout, 'type "%s"' % type_)


# ── /terminal tests ───────────────────────────────────────────────────────

def test_spawns_fresh_process_and_echoes_input_as_output(server):
    client = TerminalClient(server.connect("/terminal", auth={"session": "echo-test"}))
    client.send({"type": "resize", "cols": 80, "rows": 24})
    client.wait_for_type("spawned")
    client.send({"type": "input", "data": "hello"})
    output = client.wait_for(
        lambda m: m.get("type") == "output" and m.get("data") == "hello",
        description='output "hello"',
    )
    assert output["data"] == "hello"
    assert len(FakePtyProcess.instances) == 1


def test_reconnecting_to_same_session_replays_buffered_output(server):
    auth = {"session": "replay-test", "hostKey": "hk-replay-test"}
    client_a = TerminalClient(server.connect("/terminal", auth=auth))
    client_a.send({"type": "resize", "cols": 80, "rows": 24})
    client_a.wait_for_type("spawned")
    client_a.send({"type": "input", "data": "hello-buffer"})
    client_a.wait_for(lambda m: m.get("type") == "output" and m.get("data") == "hello-buffer")
    client_a.ws.close()

    client_b = TerminalClient(server.connect("/terminal", auth=auth))
    client_b.wait_for_type("attached")
    replay = client_b.wait_for(
        lambda m: m.get("type") == "output" and "hello-buffer" in m.get("data", ""),
        description='replay output containing "hello-buffer"',
    )
    assert "hello-buffer" in replay["data"]
    # Only one process was ever spawned - this was a reattach, not a fresh spawn.
    assert len(FakePtyProcess.instances) == 1


def test_reattach_replay_bigger_than_max_message_is_chunked_not_dropped(make_server):
    # buffer_mb and max_message_mb are independent flags, a buffer bigger
    # than one message can carry used to go out as a single oversized send
    # that both ends silently swallowed. Each input piece here stays under
    # max_message_bytes itself; only the accumulated buffer exceeds it.
    handle = make_server(buffer_mb=8, max_message_bytes=256)
    auth = {"session": "chunked-replay-test", "hostKey": "hk-chunked"}
    client_a = TerminalClient(handle.connect("/terminal", auth=auth))
    client_a.send({"type": "resize", "cols": 80, "rows": 24})
    client_a.wait_for_type("spawned")

    pieces = ["p%02d-%s" % (i, "x" * 90) for i in range(20)]  # ~2000 bytes total
    for p in pieces:
        client_a.send({"type": "input", "data": p})
    client_a.wait_for(lambda m: m.get("type") == "output" and m.get("data") == pieces[-1])
    client_a.ws.close()

    client_b = TerminalClient(handle.connect("/terminal", auth=auth))
    client_b.wait_for_type("attached")
    expected = "".join(pieces)
    collected = ""
    deadline = time.monotonic() + 2.0
    while len(collected) < len(expected) and time.monotonic() < deadline:
        m = json.loads(client_b.ws.recv(timeout=1))
        assert m["type"] == "output"
        collected += m["data"]
    assert collected == expected


def test_live_output_send_queue_is_bounded_and_drops_oldest(server):
    # reader_loop hands live output to a bounded deque, drained by a
    # single sender task - under backpressure it drops the oldest unsent
    # chunk instead of growing without limit. deque(maxlen=...)'s own
    # guarantee, exercised directly here rather than simulating real
    # network slowness.
    auth = {"session": "backpressure-test"}
    client = TerminalClient(server.connect("/terminal", auth=auth))
    client.send({"type": "resize", "cols": 80, "rows": 24})
    client.wait_for_type("spawned")

    session = server.terminal_hub.sessions["backpressure-test"]
    assert session["send_queue"].maxlen == server_module.OUTPUT_QUEUE_MAX_CHUNKS

    with session["send_queue_lock"]:
        for i in range(server_module.OUTPUT_QUEUE_MAX_CHUNKS + 50):
            session["send_queue"].append("chunk-%d" % i)
        remaining = list(session["send_queue"])
    assert len(remaining) == server_module.OUTPUT_QUEUE_MAX_CHUNKS
    assert remaining[0] == "chunk-50"
    assert remaining[-1] == "chunk-%d" % (server_module.OUTPUT_QUEUE_MAX_CHUNKS + 49)


def test_second_connection_to_same_session_evicts_first(server):
    auth = {"session": "evict-test", "hostKey": "hk-evict-test"}
    client_a = TerminalClient(server.connect("/terminal", auth=auth))
    client_a.send({"type": "resize", "cols": 80, "rows": 24})
    client_a.wait_for_type("spawned")

    client_b = TerminalClient(server.connect("/terminal", auth=auth))
    client_a.wait_for_type("evicted")
    client_b.wait_for_type("attached")
    assert len(FakePtyProcess.instances) == 1  # still the same one process


def test_terminal_reattach_requires_matching_host_key(server):
    # Reattaching over /terminal requires the same host_key /host already
    # requires, a session id alone isn't proof of ownership.
    auth = {"session": "hostkey-test", "hostKey": "the-real-key"}
    client_a = TerminalClient(server.connect("/terminal", auth=auth))
    client_a.send({"type": "resize", "cols": 80, "rows": 24})
    client_a.wait_for_type("spawned")

    ws_wrong = server.connect("/terminal", auth={"session": "hostkey-test", "hostKey": "guessed"})
    with pytest.raises(Exception):
        ws_wrong.recv(timeout=2)
    assert ws_wrong.close_code == 1008

    ws_missing = server.connect("/terminal", auth={"session": "hostkey-test"})
    with pytest.raises(Exception):
        ws_missing.recv(timeout=2)
    assert ws_missing.close_code == 1008

    client_b = TerminalClient(server.connect("/terminal", auth=auth))
    client_b.wait_for_type("attached")
    assert len(FakePtyProcess.instances) == 1  # reattached, not a fresh spawn


def test_session_cap_is_per_document_not_global(server):
    # MAX_CONCURRENT_SESSIONS enforced within a docId. Two shells in doc A
    # must not block doc B's first shell.
    client_a1 = TerminalClient(server.connect("/terminal", auth={"session": "docA-1", "docId": "docA"}))
    client_a1.send({"type": "resize", "cols": 80, "rows": 24})
    client_a1.wait_for_type("spawned")

    client_a2 = TerminalClient(server.connect("/terminal", auth={"session": "docA-2", "docId": "docA"}))
    client_a2.send({"type": "resize", "cols": 80, "rows": 24})
    client_a2.wait_for_type("spawned")

    # Doc A is now at its cap (2), a third shell in doc A is refused...
    ws_a3 = server.connect("/terminal", auth={"session": "docA-3", "docId": "docA"})
    ws_a3.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    with pytest.raises(Exception):
        ws_a3.recv(timeout=2)
    assert ws_a3.close_code == 1013

    # ...but doc B, a different document, is unaffected.
    client_b1 = TerminalClient(server.connect("/terminal", auth={"session": "docB-1", "docId": "docB"}))
    client_b1.send({"type": "resize", "cols": 80, "rows": 24})
    client_b1.wait_for_type("spawned")
    assert len(FakePtyProcess.instances) == 3


def test_session_cap_groups_connections_with_no_doc_id_together(server):
    # A client bypassing the sidebar (no docId at all) is still capped.
    for i in range(2):
        client = TerminalClient(server.connect("/terminal", auth={"session": "nodoc-%d" % i}))
        client.send({"type": "resize", "cols": 80, "rows": 24})
        client.wait_for_type("spawned")

    ws3 = server.connect("/terminal", auth={"session": "nodoc-2"})
    ws3.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    with pytest.raises(Exception):
        ws3.recv(timeout=2)
    assert ws3.close_code == 1013


def test_session_cap_is_latched_from_the_first_connections_declared_value(server):
    # A doc's first /terminal connection declares its own cap (mirroring
    # client's cap); the server enforces that value, not the
    # hardcoded default, for the rest of this doc's connections.
    client1 = TerminalClient(server.connect(
        "/terminal", auth={"session": "capdoc-1", "docId": "capdoc", "declaredCap": 1}))
    client1.send({"type": "resize", "cols": 80, "rows": 24})
    client1.wait_for_type("spawned")

    # Cap is 1, already at it - refused even though the hardcoded default is 2.
    ws2 = server.connect("/terminal", auth={"session": "capdoc-2", "docId": "capdoc", "declaredCap": 1})
    ws2.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    with pytest.raises(Exception):
        ws2.recv(timeout=2)
    assert ws2.close_code == 1013
    assert "1" in ws2.close_reason


def test_session_cap_latch_ignores_a_later_connections_declared_value(server):
    # Simulates code running inside an already-spawned shell trying to
    # raise its own doc's cap after the fact. The latch must ignore it,
    # since this connection necessarily postdates the doc's first one.
    client1 = TerminalClient(server.connect(
        "/terminal", auth={"session": "latch-1", "docId": "latchdoc", "declaredCap": 1}))
    client1.send({"type": "resize", "cols": 80, "rows": 24})
    client1.wait_for_type("spawned")

    ws2 = server.connect(
        "/terminal", auth={"session": "latch-2", "docId": "latchdoc", "declaredCap": 999})
    ws2.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    with pytest.raises(Exception):
        ws2.recv(timeout=2)
    assert ws2.close_code == 1013  # still capped at 1, not 999


def test_session_cap_falls_back_to_the_default_when_no_cap_is_declared(server):
    # An older/non-standard client that omits declaredCap gets the
    # hardcoded MAX_CONCURRENT_SESSIONS default, same as before this doc
    # had its own latched value.
    for i in range(2):
        client = TerminalClient(server.connect(
            "/terminal", auth={"session": "nocap-%d" % i, "docId": "nocapdoc"}))
        client.send({"type": "resize", "cols": 80, "rows": 24})
        client.wait_for_type("spawned")

    ws3 = server.connect("/terminal", auth={"session": "nocap-2", "docId": "nocapdoc"})
    ws3.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    with pytest.raises(Exception):
        ws3.recv(timeout=2)
    assert ws3.close_code == 1013


def test_idle_session_killed_after_window_then_reconnect_spawns_fresh(server):
    # A tiny idleHours value converts to a few ms of real grace period. No
    # fake/mocked clock needed, since idle timeout is a real per-connection
    # parameter.
    path = "/terminal?idleHours=0.00003"  # ~108ms
    auth = {"session": "idle-test"}
    client_a = TerminalClient(server.connect(path, auth=auth))
    client_a.send({"type": "resize", "cols": 80, "rows": 24})
    client_a.wait_for_type("spawned")
    client_a.ws.close()

    wait_until(lambda: FakePtyProcess.instances[0].killed is True)

    client_b = TerminalClient(server.connect(path, auth=auth))
    client_b.send({"type": "resize", "cols": 80, "rows": 24})
    client_b.wait_for_type("spawned")  # not "attached" proves the old session is gone
    assert len(FakePtyProcess.instances) == 2


def test_terminate_deletes_session_immediately_bypassing_idle_grace(server):
    auth = {"session": "terminate-test"}  # default (12h) idle window would never elapse here
    client_a = TerminalClient(server.connect("/terminal", auth=auth))
    client_a.send({"type": "resize", "cols": 80, "rows": 24})
    client_a.wait_for_type("spawned")
    client_a.send({"type": "terminate"})
    # The client waits for this before treating the kill as confirmed, 
    # also doubles as the synchronization point here.
    client_a.wait_for_type("terminated")

    client_b = TerminalClient(server.connect("/terminal", auth=auth))
    client_b.send({"type": "resize", "cols": 80, "rows": 24})
    client_b.wait_for_type("spawned")  # fresh spawn, immediately - no wait for any idle window
    assert len(FakePtyProcess.instances) == 2
    assert FakePtyProcess.instances[0].killed is True


def test_terminate_with_wrong_host_key_is_rejected_and_never_acked(server):
    # A terminate attempt is a reattach first. A wrong hostKey must be
    # rejected before the "terminate" message is ever reached, so the PTY
    # survives and no "terminated" ack is ever sent. This is the case
    # the client relies on to avoid removing a shell's row over a live PTY 
    # it couldn't actually confirm was killed.
    auth = {"session": "terminate-hostkey-test", "hostKey": "the-real-key"}
    client_a = TerminalClient(server.connect("/terminal", auth=auth))
    client_a.send({"type": "resize", "cols": 80, "rows": 24})
    client_a.wait_for_type("spawned")

    ws_wrong = server.connect(
        "/terminal", auth={"session": "terminate-hostkey-test", "hostKey": "guessed"})
    ws_wrong.send(json.dumps({"type": "terminate"}))
    with pytest.raises(Exception):
        ws_wrong.recv(timeout=2)
    assert ws_wrong.close_code == 1008

    # The original session is untouched, a fresh reattach with the real
    # key still finds it running, not respawned.
    client_b = TerminalClient(server.connect("/terminal", auth=auth))
    client_b.wait_for_type("attached")
    assert len(FakePtyProcess.instances) == 1
    assert FakePtyProcess.instances[0].killed is False


# ── /host + /mcp tests ────────────────────────────────────────────────────
#
# An explicit /mcp ?session=... connection is gated on a matching
# sessionKey, minted only when a /terminal session actually exists - so
# every test below connecting /mcp with an explicit session first spawns
# one, matching how a real gridshell-mcp process is always launched.

def host_auth(session):
    # Deterministic (not server-minted, unlike session_key) - a client mints
    # this one, so tests can reconstruct it without reading server state.
    return {"session": session, "hostKey": "hk-" + session}


def spawn_terminal_session(handle, session):
    """Spawns a fake shell for `session` via /terminal so its real,
    server-minted session_key exists in terminal_hub.sessions, and returns
    (ws, session_key). Leaves the /terminal connection open, some tests
    reuse it (e.g. to send "terminate")."""
    auth = dict(host_auth(session))
    ws = handle.connect("/terminal", auth=auth)
    ws.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    msg = json.loads(ws.recv(timeout=2))
    assert msg["type"] == "spawned"
    return ws, handle.terminal_hub.sessions[session]["session_key"]


def mcp_auth(session, session_key):
    return {"session": session, "sessionKey": session_key}


def test_relays_request_response_envelope_end_to_end(server):
    session = "mcp-relay-test"
    _term_ws, session_key = spawn_terminal_session(server, session)
    host_ws = server.connect("/host", auth=host_auth(session))
    mcp_ws = server.connect("/mcp", auth=mcp_auth(session, session_key))

    mcp_ws.send(json.dumps({"id": "call1", "tool": "testTool", "params": {"foo": 1}}))
    req = json.loads(host_ws.recv(timeout=3))
    assert_valid(REQUEST_ENVELOPE_SCHEMA, req, "/host request envelope")

    host_ws.send(json.dumps({"id": req["id"], "result": "echoed:" + req["tool"]}))
    response = json.loads(mcp_ws.recv(timeout=3))
    assert_valid(RESPONSE_ENVELOPE_SCHEMA, response, "/mcp response envelope")
    assert response["id"] == "call1"
    assert response["result"] == "echoed:testTool"


def test_queued_call_times_out_when_no_host_client_ever_connects(server):
    # An explicit session with no client connected is queued, not failed
    # immediately (see HostBridge's class docstring), it only resolves via
    # its own per-call timer if no client ever shows up. A short timeoutMs
    # keeps this test fast without changing the real default.
    session = "no-such-session"
    _term_ws, session_key = spawn_terminal_session(server, session)
    mcp_ws = server.connect("/mcp", auth=mcp_auth(session, session_key))
    mcp_ws.send(json.dumps({"id": "call2", "tool": "testTool", "timeoutMs": 200}))
    response = json.loads(mcp_ws.recv(timeout=2))
    assert_valid(RESPONSE_ENVELOPE_SCHEMA, response, "/mcp response envelope")
    assert response["id"] == "call2"
    assert isinstance(response.get("error"), str)


def test_queued_call_is_delivered_once_a_host_client_later_connects(server):
    # Same "no client yet" starting state as above, but a client for this
    # exact session connects before the timeout - the queued call must
    # reach it (flushed in HostBridge.handle), not be lost.
    session = "late-connect-test"
    _term_ws, session_key = spawn_terminal_session(server, session)
    mcp_ws = server.connect("/mcp", auth=mcp_auth(session, session_key))
    mcp_ws.send(json.dumps({"id": "call3", "tool": "testTool", "timeoutMs": 5000}))

    host_ws = server.connect("/host", auth=host_auth(session))
    req = json.loads(host_ws.recv(timeout=3))
    assert_valid(REQUEST_ENVELOPE_SCHEMA, req, "/host request envelope")
    assert req["tool"] == "testTool"

    host_ws.send(json.dumps({"id": req["id"], "result": "late-echo"}))
    response = json.loads(mcp_ws.recv(timeout=3))
    assert_valid(RESPONSE_ENVELOPE_SCHEMA, response, "/mcp response envelope")
    assert response["id"] == "call3"
    assert response["result"] == "late-echo"


def test_in_flight_call_fails_fast_when_its_bridge_disconnects_mid_response(server):
    # A call already dispatched to a live client (not queued -- see the
    # "queued" tests above) that then disconnects before answering must
    # fail immediately, not sit out the full relay timeout -- its outcome
    # is unknown (it may have already executed on the Apps Script side),
    # so it's failed, not retried.
    session = "in-flight-disconnect-test"
    _term_ws, session_key = spawn_terminal_session(server, session)
    mcp_ws = server.connect("/mcp", auth=mcp_auth(session, session_key))
    host_ws = server.connect("/host", auth=host_auth(session))

    mcp_ws.send(json.dumps({"id": "call5", "tool": "testTool", "timeoutMs": 5000}))
    req = json.loads(host_ws.recv(timeout=3))  # confirms it was actually dispatched (sent=True)
    assert_valid(REQUEST_ENVELOPE_SCHEMA, req, "/host request envelope")

    host_ws.close()  # disconnect before ever answering

    response = json.loads(mcp_ws.recv(timeout=2))  # well under the 5s timeoutMs
    assert_valid(RESPONSE_ENVELOPE_SCHEMA, response, "/mcp response envelope")
    assert response["id"] == "call5"
    assert "outcome unknown" in response["error"]


def test_flushed_call_fails_fast_when_that_same_connection_then_disconnects(server):
    # A call that started queued (no client yet), got flushed to a client
    # that then connects, must still be covered by the in-flight fast-fail
    # if that connection disconnects before responding -- not just a call
    # that was sent to a client from the start.
    session = "flush-then-disconnect-test"
    _term_ws, session_key = spawn_terminal_session(server, session)
    mcp_ws = server.connect("/mcp", auth=mcp_auth(session, session_key))
    mcp_ws.send(json.dumps({"id": "call6", "tool": "testTool", "timeoutMs": 5000}))

    host_ws = server.connect("/host", auth=host_auth(session))
    req = json.loads(host_ws.recv(timeout=3))  # confirms the flush actually delivered it
    assert_valid(REQUEST_ENVELOPE_SCHEMA, req, "/host request envelope")

    host_ws.close()  # disconnect before ever answering

    response = json.loads(mcp_ws.recv(timeout=2))  # well under the 5s timeoutMs
    assert_valid(RESPONSE_ENVELOPE_SCHEMA, response, "/mcp response envelope")
    assert response["id"] == "call6"
    assert "outcome unknown" in response["error"]


def test_queued_call_fails_fast_when_its_shell_is_explicitly_terminated(server):
    # Killing a shell (/terminal "terminate") should fail any calls queued
    # for its session immediately, rather than leaving them to wait out the
    # full timeout for a reconnect that will now never happen.
    session = "kill-while-queued-test"
    term_ws, session_key = spawn_terminal_session(server, session)
    mcp_ws = server.connect("/mcp", auth=mcp_auth(session, session_key))
    mcp_ws.send(json.dumps({"id": "call4", "tool": "testTool", "timeoutMs": 5000}))

    # Waiting on "the call registered as pending" via a poll would mean
    # reaching into HostBridge.pending directly, which this suite
    # deliberately avoids (see the module docstring). A short fixed sleep
    # is the black-box-safe option here.
    time.sleep(0.1)
    term_ws.send(json.dumps({"type": "terminate"}))

    response = json.loads(mcp_ws.recv(timeout=2))  # well under the 5s timeoutMs
    assert_valid(RESPONSE_ENVELOPE_SCHEMA, response, "/mcp response envelope")
    assert response["id"] == "call4"
    assert response["error"] == "Shell was closed."


# ── Origin checks ────────────────────────────────────────────────────────
# A WebSocket handshake is accepted at the protocol level before router()
# runs, so a rejection here is a close frame the client sees only once it
# tries to read/write, not a failed connect(), see router()'s own comment.

def test_origin_from_apps_script_is_allowed_by_default(server):
    ws = server.connect("/terminal", origin="https://n-abc123.script.googleusercontent.com",
                         auth={"session": "origin-ok"})
    ws.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    msg = json.loads(ws.recv(timeout=2))
    assert msg["type"] == "spawned"


def test_origin_from_disallowed_host_is_rejected(server):
    ws = server.connect("/terminal", origin="https://evil.example", auth={"session": "origin-bad"})
    with pytest.raises(Exception):
        ws.recv(timeout=2)
    assert ws.close_code == 1008


def test_origin_absent_is_allowed_non_browser_client(server):
    # Every other test in this file connects with no Origin header at all
    # (gridshell-mcp/SheetsClient send none), but this one
    # asserts it explicitly as the documented behavior _is_allowed_origin
    # relies on, not an accident of the test client's defaults.
    ws = server.connect("/terminal", auth={"session": "origin-absent"})
    ws.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    msg = json.loads(ws.recv(timeout=2))
    assert msg["type"] == "spawned"


def test_allow_origin_flag_extends_the_allowlist(make_server):
    handle = make_server(allowed_origin_suffixes=(".example.com",))
    ws = handle.connect("/terminal", origin="https://myfrontend.example.com",
                         auth={"session": "origin-extra"})
    ws.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    msg = json.loads(ws.recv(timeout=2))
    assert msg["type"] == "spawned"


def test_allowed_origin_suffix_has_a_dot_boundary_and_allows_the_bare_host(make_server):
    # A ".example.com" suffix must not also match "evilexample.com",
    # but should match both "example.com" itself and "sub.example.com".
    is_allowed = server_module._is_allowed_origin
    suffixes = (".example.com",)
    assert is_allowed("https://evilexample.com", suffixes) is False
    assert is_allowed("https://example.com", suffixes) is True
    assert is_allowed("https://sub.example.com", suffixes) is True


# ── Auth token ─────────────────────────────────────────────────────────
# Sent as the first message on the connection, see router()'s
# comment, not a query string or an Authorization header. A reverse proxy
# never sees it, since it only ever logs the handshake request line.

def test_correct_token_is_accepted(make_server):
    handle = make_server(required_token="s3cret")
    ws = handle.connect("/terminal", auth={"token": "s3cret", "session": "tok-ok"})
    ws.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    msg = json.loads(ws.recv(timeout=2))
    assert msg["type"] == "spawned"


def test_wrong_token_is_rejected_1008(make_server):
    handle = make_server(required_token="s3cret")
    ws = handle.connect("/terminal", auth={"token": "nope", "session": "tok-wrong"})
    with pytest.raises(Exception):
        ws.recv(timeout=2)
    assert ws.close_code == 1008


def test_missing_token_is_rejected_1008(make_server):
    handle = make_server(required_token="s3cret")
    ws = handle.connect("/terminal", auth={"session": "tok-missing"})
    with pytest.raises(Exception):
        ws.recv(timeout=2)
    assert ws.close_code == 1008


def test_non_ascii_token_round_trips(make_server):   
    handle = make_server(required_token="pa55wörd")
    ws = handle.connect("/terminal", auth={"token": "pa55wörd", "session": "tok-nonascii"})
    ws.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    msg = json.loads(ws.recv(timeout=2))
    assert msg["type"] == "spawned"


def test_token_required_on_host_and_mcp_too(make_server):
    handle = make_server(required_token="s3cret")
    for path in ("/host", "/mcp"):
        ws = handle.connect(path, auth={"session": "tok-allpaths"})
        with pytest.raises(Exception):
            ws.recv(timeout=2)
        assert ws.close_code == 1008


# ── Auth frame gate itself ──────────────────────────────────────────────

def test_malformed_first_message_is_rejected_1008(server):
    ws = server.connect_raw("/terminal")
    ws.send("not json")
    with pytest.raises(Exception):
        ws.recv(timeout=2)
    assert ws.close_code == 1008


def test_no_first_message_times_out_and_is_rejected_1008(server):
    ws = server.connect_raw("/terminal")
    with pytest.raises(Exception):
        ws.recv(timeout=7)  # past router()'s own 5s auth-frame wait
    assert ws.close_code == 1008


# ── Message size limit ────────────────────────────────────────────────────

def test_oversized_frame_fails_the_call_fast_with_a_clear_reason_not_timeout(make_server):
    # A response over max_size closes the /host connection with code 1009
    # before any of it is parsed. _fail_in_flight should recognize that
    # and say so, rather than the generic "outcome unknown" wording (which
    # is actively misleading here: for a read, nothing was ever delivered).
    handle = make_server(max_message_bytes=256)
    session = "oversized-test"
    _term_ws, session_key = spawn_terminal_session(handle, session)
    mcp_ws = handle.connect("/mcp", auth=mcp_auth(session, session_key))
    mcp_ws.send(json.dumps({"id": "big1", "tool": "testTool", "timeoutMs": 5000}))

    host_ws = handle.connect("/host", auth=host_auth(session))
    req = json.loads(host_ws.recv(timeout=3))
    assert_valid(REQUEST_ENVELOPE_SCHEMA, req, "/host request envelope")

    # A reply comfortably over the 256-byte cap configured above.
    host_ws.send(json.dumps({"id": req["id"], "result": "x" * 1000}))

    response = json.loads(mcp_ws.recv(timeout=2))  # well under the 5s timeoutMs
    assert_valid(RESPONSE_ENVELOPE_SCHEMA, response, "/mcp response envelope")
    assert response["id"] == "big1"
    assert "too large" in response["error"]
    assert "outcome unknown" not in response["error"]


# ── Session key gating on explicit /mcp sessions ─────────────────────────
# Naming a session id is not, by itself, proof of owning it. An agent with
# shell access elsewhere on the machine could read another shell's
# GRIDSHELL_SESSION out of its environment and simply claim it. The
# no-session-id "sole connected client" fallback (mcp.md) is a separate,
# unaffected code path, it never takes a session id at all.

def test_mcp_correct_session_key_is_accepted(server):
    session = "key-ok-test"
    spawn_terminal_session(server, session)
    session_key = server.terminal_hub.sessions[session]["session_key"]
    host_ws = server.connect("/host", auth=host_auth(session))
    mcp_ws = server.connect("/mcp", auth=mcp_auth(session, session_key))

    mcp_ws.send(json.dumps({"id": "keyok1", "tool": "testTool"}))
    req = json.loads(host_ws.recv(timeout=3))
    assert_valid(REQUEST_ENVELOPE_SCHEMA, req, "/host request envelope")
    host_ws.send(json.dumps({"id": req["id"], "result": "ok"}))
    response = json.loads(mcp_ws.recv(timeout=3))
    assert_valid(RESPONSE_ENVELOPE_SCHEMA, response, "/mcp response envelope")
    assert response["result"] == "ok"


def test_mcp_wrong_session_key_is_rejected_1008(server):
    session = "key-wrong-test"
    spawn_terminal_session(server, session)
    mcp_ws = server.connect("/mcp", auth={"session": session, "sessionKey": "not-the-real-key"})
    with pytest.raises(Exception):
        mcp_ws.recv(timeout=2)
    assert mcp_ws.close_code == 1008


def test_mcp_missing_session_key_is_rejected_1008(server):
    session = "key-missing-test"
    spawn_terminal_session(server, session)
    mcp_ws = server.connect("/mcp", auth={"session": session})
    with pytest.raises(Exception):
        mcp_ws.recv(timeout=2)
    assert mcp_ws.close_code == 1008


def test_mcp_unknown_session_is_rejected_even_with_a_guessed_key(server):
    # No /terminal was ever spawned under this session id, so no
    # session_key exists to match against. Any guess is rejected the same
    # as a wrong one. 
    mcp_ws = server.connect("/mcp", auth={"session": "never-spawned-test", "sessionKey": "guess"})
    with pytest.raises(Exception):
        mcp_ws.recv(timeout=2)
    assert mcp_ws.close_code == 1008


# ── Host-key gating on /host ───────────────────────────────────────────────
# /host requires the same kind of per-spawn secret /mcp already
# checks, except this one is minted client-side and handed to the
# server via /terminal's auth frame, see server.py's router()/HostBridge.

def test_host_wrong_hostkey_is_rejected_1008(server):
    session = "host-key-wrong-test"
    spawn_terminal_session(server, session)
    host_ws = server.connect("/host", auth={"session": session, "hostKey": "not-the-real-key"})
    with pytest.raises(Exception):
        host_ws.recv(timeout=2)
    assert host_ws.close_code == 1008


def test_host_missing_hostkey_is_rejected_1008(server):
    session = "host-key-missing-test"
    spawn_terminal_session(server, session)
    host_ws = server.connect("/host", auth={"session": session})
    with pytest.raises(Exception):
        host_ws.recv(timeout=2)
    assert host_ws.close_code == 1008


def test_host_unspawned_session_gets_a_retryable_close_not_1008(server):
    # No /terminal connection has spawned this session yet, not a
    # key-mismatch, must stay retryable (1013) or the sidebar gives up.
    host_ws = server.connect("/host", auth={"session": "never-spawned-host-test", "hostKey": "guess"})
    with pytest.raises(Exception):
        host_ws.recv(timeout=2)
    assert host_ws.close_code == 1013


def test_host_correct_hostkey_is_accepted(server):
    session = "host-key-ok-test"
    spawn_terminal_session(server, session)
    mcp_ws = server.connect("/mcp", auth=mcp_auth(session, server.terminal_hub.sessions[session]["session_key"]))
    host_ws = server.connect("/host", auth=host_auth(session))

    mcp_ws.send(json.dumps({"id": "hk1", "tool": "testTool"}))
    req = json.loads(host_ws.recv(timeout=3))
    assert_valid(REQUEST_ENVELOPE_SCHEMA, req, "/host request envelope")
    host_ws.send(json.dumps({"id": req["id"], "result": "ok"}))
    response = json.loads(mcp_ws.recv(timeout=3))
    assert response["result"] == "ok"


def test_host_duplicate_registration_evicts_the_old_connection(server):
    # The new connection must close the old one explicitly.
    session = "host-duplicate-test"
    _term_ws, session_key = spawn_terminal_session(server, session)
    old_ws = server.connect("/host", auth=host_auth(session))
    new_ws = server.connect("/host", auth=host_auth(session))
    with pytest.raises(Exception):
        old_ws.recv(timeout=2)
    # Not 1008, that code tells the sidebar to stop retrying; this is a
    # routine handover the old tab should keep retrying to reclaim.
    assert old_ws.close_code == 4000

    mcp_ws = server.connect("/mcp", auth=mcp_auth(session, session_key))
    mcp_ws.send(json.dumps({"id": "dup1", "tool": "testTool", "timeoutMs": 2000}))
    req = json.loads(new_ws.recv(timeout=3))  # must route to the new connection, not the evicted one
    assert_valid(REQUEST_ENVELOPE_SCHEMA, req, "/host request envelope")


# ── Unusable cwd surfaces a warning instead of silently discarding it ──────

def test_bad_cwd_sends_a_warning_and_falls_back_to_the_default(make_server):
    handle = make_server()
    ws = handle.connect("/terminal?cwd=not-an-absolute-path", auth={"session": "bad-cwd"})
    ws.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    json.loads(ws.recv(timeout=2))  # spawned
    warning = json.loads(ws.recv(timeout=2))
    assert_valid(SERVER_MESSAGE_SCHEMA, warning, "/terminal warning message")
    assert warning["type"] == "warning"
    assert "not-an-absolute-path" in warning["message"]


def test_good_cwd_sends_no_warning(make_server, tmp_path):
    handle = make_server()
    ws = handle.connect("/terminal?cwd=" + str(tmp_path), auth={"session": "good-cwd"})
    ws.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    json.loads(ws.recv(timeout=2))  # spawned
    ws.send(json.dumps({"type": "input", "data": "x"}))  # forces another round trip
    output = json.loads(ws.recv(timeout=2))
    assert output["type"] == "output"  # not "warning" - none was sent


# ── Query-param ceilings ───────────────────────────────────────────────────

def test_buffer_mb_param_is_clamped_to_the_configured_ceiling(make_server):
    handle = make_server(buffer_mb=1)
    ws = handle.connect("/terminal?bufferMb=999999", auth={"session": "clamp-buf"})
    ws.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    json.loads(ws.recv(timeout=2))
    session = handle.terminal_hub.sessions["clamp-buf"]
    assert session["buffer_max_bytes"] == 1 * 1024 * 1024


def test_idle_hours_param_is_clamped_to_the_configured_ceiling(make_server):
    handle = make_server(idle_hours=1)
    ws = handle.connect("/terminal?idleHours=876000", auth={"session": "clamp-idle"})
    ws.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    json.loads(ws.recv(timeout=2))
    session = handle.terminal_hub.sessions["clamp-idle"]
    assert session["idle_kill_ms"] == 1 * 60 * 60 * 1000


def test_idle_hours_zero_means_explicit_zero_grace_not_no_override(make_server):
    # ?idleHours=0 used to fall through to the server's own default, an
    # asymmetry with --idle-hours 0 on the CLI (which means immediate
    # kill).
    handle = make_server(idle_hours=1)
    ws = handle.connect("/terminal?idleHours=0", auth={"session": "zero-idle"})
    ws.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    json.loads(ws.recv(timeout=2))
    session = handle.terminal_hub.sessions["zero-idle"]
    assert session["idle_kill_ms"] == 0


def test_idle_hours_negative_falls_back_to_the_server_default(make_server):
    handle = make_server(idle_hours=1)
    ws = handle.connect("/terminal?idleHours=-5", auth={"session": "neg-idle"})
    ws.send(json.dumps({"type": "resize", "cols": 80, "rows": 24}))
    json.loads(ws.recv(timeout=2))
    session = handle.terminal_hub.sessions["neg-idle"]
    assert session["idle_kill_ms"] == 1 * 60 * 60 * 1000


# ── CLI refuse-to-start guards + --help ────────────────────────────────────
# subprocess-level: these guards fire in main(), before resolve_flags()'s
# caller ever gets a router to test in-process, so a real CLI invocation is
# the only way to exercise them.

def _run_cli(*args, timeout=10):
    return subprocess.run(
        [sys.executable, "-m", "gridshell.server"] + list(args),
        capture_output=True, text=True, timeout=timeout,
    )


@pytest.mark.parametrize("args, exits_nonzero, must_contain, must_not_contain", [
    # A bare --wss with explicit token opted-out
    pytest.param(
        ("--wss", "--no-auth-token", "--cert-path", "x", "--key-path", "y"),
        True, ["no-auth-token"], [],
        id="wss-with-no-auth-token-flag",
    ),
    pytest.param(
        # A real port, not 0. --port has its own positive-minimum
        # guard (see the numeric-flags test below), which would otherwise
        # fire first and mask the guard this case actually exercises.
        ("--host", "0.0.0.0", "--port", "54329", "--no-auth-token"),
        True, ["no-auth-token"], [],
        id="non-loopback-host-with-no-auth-token-flag",
    ),
    pytest.param(
        ("--wss", "--auth-token", "s3cret"),
        True, ["cert-path"], [],
        id="wss-without-cert-paths",
    ),
    pytest.param(
        ("--help",),
        False, ["--port", "--no-auth-token", "--regenerate-token", "--copy-token", "--allow-root"], [],
        id="help-flag",
    ),
    pytest.param(
        ("--port", "abc"),
        True, ["--port", "abc"], ["Traceback"],
        id="bad-numeric-flag-value",
    ),
    pytest.param(
        ("--regenerate-token", "--auth-token", "x"),
        True, ["regenerate-token"], [],
        id="regenerate-token-with-explicit-auth-token",
    ),
    pytest.param(
        ("--regenerate-token", "--no-auth-token"),
        True, ["regenerate-token"], [],
        id="regenerate-token-with-no-auth-token-flag",
    ),
    # A 0 --max-message-mb rejects every frame and a negative
    # --idle-hours kills a shell the instant its dialog disconnects with
    # no error at all. --idle-hours 0 itself stays valid (explicit zero
    # grace, a documented case), only negative values are rejected.
    pytest.param(
        ("--max-message-mb", "0"),
        True, ["--max-message-mb"], [],
        id="non-positive-max-message-mb",
    ),
    pytest.param(
        ("--buffer-mb", "-5"),
        True, ["--buffer-mb"], [],
        id="negative-buffer-mb",
    ),
    pytest.param(
        ("--idle-hours", "-1"),
        True, ["--idle-hours"], [],
        id="negative-idle-hours",
    ),
    pytest.param(
        ("--port", "0"),
        True, ["--port"], [],
        id="non-positive-port",
    ),
])
def test_cli_guard(args, exits_nonzero, must_contain, must_not_contain):
    result = _run_cli(*args)
    assert (result.returncode != 0) is exits_nonzero
    combined = result.stdout + result.stderr
    for s in must_contain:
        assert s in combined
    for s in must_not_contain:
        assert s not in combined


# ── Default auth token: auto-generate + persist ───────────────────────────
# Unit-tested directly against the helper rather than via subprocess - no
# real file I/O risk on the developer's actual ~/.gridshell/token this way,
# and it's a much faster/more direct way to cover the persistence logic
# than launching a real long-running server subprocess would be.

def test_default_token_is_generated_and_persisted(tmp_path, monkeypatch):
    token_path = tmp_path / "token"
    monkeypatch.setenv("GRIDSHELL_TOKEN_PATH", str(token_path))
    token1 = server_module._load_or_create_default_token(None, False, False)
    assert token1
    assert token_path.read_text(encoding="utf-8").strip() == token1
    token2 = server_module._load_or_create_default_token(None, False, False)
    assert token2 == token1  # reused across "launches", not regenerated


def test_regenerate_flag_produces_and_persists_a_new_value(tmp_path, monkeypatch):
    token_path = tmp_path / "token"
    monkeypatch.setenv("GRIDSHELL_TOKEN_PATH", str(token_path))
    token1 = server_module._load_or_create_default_token(None, False, False)
    token2 = server_module._load_or_create_default_token(None, False, True)
    assert token2 != token1
    assert token_path.read_text(encoding="utf-8").strip() == token2


def test_no_auth_token_flag_disables_default_generation(tmp_path, monkeypatch):
    token_path = tmp_path / "token"
    monkeypatch.setenv("GRIDSHELL_TOKEN_PATH", str(token_path))
    assert server_module._load_or_create_default_token(None, True, False) is None
    assert not token_path.exists()


def test_explicit_token_is_returned_unchanged_and_not_persisted(tmp_path, monkeypatch):
    token_path = tmp_path / "token"
    monkeypatch.setenv("GRIDSHELL_TOKEN_PATH", str(token_path))
    assert server_module._load_or_create_default_token("mine", False, False) == "mine"
    assert not token_path.exists()


# ── Clipboard-or-refuse disclosure (never stdout) ─────────────────────────
# resolve_flags() is called in-process here (sys.argv/env monkeypatched)
# rather than via subprocess: a "starts successfully" case can't be driven
# through _run_cli at all (main() blocks forever on await asyncio.Future()),
# and monkeypatching _try_clipboard_copy needs an in-process call anyway to
# stay deterministic and avoid touching the real OS clipboard during tests.

def test_fresh_token_with_no_clipboard_still_starts(tmp_path, monkeypatch, capsys):
    token_path = tmp_path / "token"
    monkeypatch.setenv("GRIDSHELL_TOKEN_PATH", str(token_path))
    monkeypatch.setattr(sys, "argv", ["gridshell-server"])
    monkeypatch.setattr(server_module, "_try_clipboard_copy", lambda text: False)
    flags = server_module.resolve_flags()
    token = flags[6]
    assert token
    assert token_path.read_text(encoding="utf-8").strip() == token
    out = capsys.readouterr().out
    assert token not in out
    assert "clipboard unavailable" in out.lower()


def test_token_persist_failure_with_no_clipboard_refuses_to_start(tmp_path, monkeypatch):
    monkeypatch.setenv("GRIDSHELL_TOKEN_PATH", str(tmp_path / "token"))
    monkeypatch.setattr(sys, "argv", ["gridshell-server"])
    monkeypatch.setattr(server_module, "_try_clipboard_copy", lambda text: False)
    monkeypatch.setattr(server_module, "_load_or_create_default_token", lambda *a, **k: "in-memory-only")
    monkeypatch.setattr(server_module, "_persisted_token_exists", lambda: False)
    with pytest.raises(SystemExit) as exc_info:
        server_module.resolve_flags()
    assert "retry" in str(exc_info.value).lower()


def test_fresh_token_with_clipboard_prints_confirmation_not_the_value(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("GRIDSHELL_TOKEN_PATH", str(tmp_path / "token"))
    monkeypatch.setattr(sys, "argv", ["gridshell-server"])
    monkeypatch.setattr(server_module, "_try_clipboard_copy", lambda text: True)
    flags = server_module.resolve_flags()
    token = flags[6]
    assert token
    out = capsys.readouterr().out
    assert token not in out
    assert "copied to clipboard" in out.lower()


def test_reused_token_with_no_clipboard_still_starts(tmp_path, monkeypatch, capsys):
    token_path = tmp_path / "token"
    monkeypatch.setenv("GRIDSHELL_TOKEN_PATH", str(token_path))
    existing = server_module._load_or_create_default_token(None, False, False)
    monkeypatch.setattr(sys, "argv", ["gridshell-server"])
    monkeypatch.setattr(server_module, "_try_clipboard_copy", lambda text: False)
    flags = server_module.resolve_flags()
    assert flags[6] == existing
    out = capsys.readouterr().out
    assert existing not in out
    assert "clipboard unavailable" in out.lower()


def test_explicit_token_never_touches_clipboard(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["gridshell-server", "--auth-token", "mine"])
    monkeypatch.setattr(
        server_module, "_try_clipboard_copy",
        lambda text: (_ for _ in ()).throw(AssertionError("should not be called")),
    )
    flags = server_module.resolve_flags()
    assert flags[6] == "mine"
    assert "mine" not in capsys.readouterr().out


def test_copy_token_flag_success_exits_zero_without_printing_value(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("GRIDSHELL_TOKEN_PATH", str(tmp_path / "token"))
    monkeypatch.setattr(sys, "argv", ["gridshell-server", "--copy-token"])
    monkeypatch.setattr(server_module, "_try_clipboard_copy", lambda text: True)
    with pytest.raises(SystemExit) as exc_info:
        server_module.resolve_flags()
    assert exc_info.value.code == 0
    out = capsys.readouterr().out
    assert "copied to clipboard" in out.lower()


def test_copy_token_flag_no_clipboard_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.setenv("GRIDSHELL_TOKEN_PATH", str(tmp_path / "token"))
    monkeypatch.setattr(sys, "argv", ["gridshell-server", "--copy-token"])
    monkeypatch.setattr(server_module, "_try_clipboard_copy", lambda text: False)
    with pytest.raises(SystemExit) as exc_info:
        server_module.resolve_flags()
    assert exc_info.value.code != 0


def test_copy_token_flag_with_no_auth_token_exits_nonzero(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["gridshell-server", "--copy-token", "--no-auth-token"])
    with pytest.raises(SystemExit) as exc_info:
        server_module.resolve_flags()
    assert exc_info.value.code != 0


def test_try_clipboard_copy_respects_no_clipboard_test_seam(monkeypatch):
    monkeypatch.setenv("GRIDSHELL_NO_CLIPBOARD", "1")
    assert server_module._try_clipboard_copy("anything") is False


# ── Root/Administrator refusal guard ──────────────────────────────────────

def test_refuses_to_start_as_root_without_allow_root(monkeypatch):
    monkeypatch.setattr(server_module, "_running_as_root_or_admin", lambda: True)
    with pytest.raises(SystemExit) as exc_info:
        asyncio.run(server_module.main(
            port=0, idle_hours=12, buffer_mb=8, cert_path=None, key_path=None,
            use_wss=False, auth_token="tok", host="localhost", allow_origins=[],
            max_message_mb=None, allow_root=False,
        ))
    assert "root" in str(exc_info.value).lower() or "administrator" in str(exc_info.value).lower()


def test_starts_as_root_with_allow_root(monkeypatch):
    monkeypatch.setattr(server_module, "_running_as_root_or_admin", lambda: True)
    monkeypatch.setattr(server_module, "PtyProcess", FakePtyProcess)

    async def run_and_stop():
        task = asyncio.ensure_future(server_module.main(
            port=0, idle_hours=12, buffer_mb=8, cert_path=None, key_path=None,
            use_wss=False, auth_token="tok", host="localhost", allow_origins=[],
            max_message_mb=None, allow_root=True,
        ))
        await asyncio.sleep(0.2)
        assert not task.done()  # still running - didn't exit/raise
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run_and_stop())


def test_mcp_sole_connected_client_fallback_needs_no_session_key(server):
    session = "fallback-test"
    spawn_terminal_session(server, session)
    host_ws = server.connect("/host", auth=host_auth(session))
    mcp_ws = server.connect("/mcp")  # no session, no sessionKey (default empty auth)

    mcp_ws.send(json.dumps({"id": "fb1", "tool": "testTool"}))
    req = json.loads(host_ws.recv(timeout=3))
    assert_valid(REQUEST_ENVELOPE_SCHEMA, req, "/host request envelope")
    host_ws.send(json.dumps({"id": req["id"], "result": "fallback-ok"}))
    response = json.loads(mcp_ws.recv(timeout=3))
    assert_valid(RESPONSE_ENVELOPE_SCHEMA, response, "/mcp response envelope")
    assert response["result"] == "fallback-ok"
