"""GridShell terminal server - the /terminal, /host, and /mcp WebSocket
endpoints a self-hosted GridShell backend exposes. Installed automatically
with `pip install gridshell`, alongside the SheetsClient Python client and
mcp_grid.py. Run via the `gridshell-server` console command, or
`python -m gridshell.server` / this file directly.

  /terminal  xterm.js <-> PTY (session reattach, idle-kill, output buffer)
  /host      Host command bridge (Sheets sidebar/dialog connects here)
  /mcp       MCP server relay (mcp_grid.py connects here)

PTY backend: pywinpty (ConPTY) on Windows, ptyprocess on Linux/Mac.

Dependencies are installed automatically with `pip install gridshell`
(websockets, plus pywinpty on Windows or ptyprocess on Linux/Mac).

Run:  gridshell-server --help  (see USAGE below for the full flag list)
Default port 3000. --max-message-mb (or MAX_MESSAGE_MB env var) defaults to
64, the max size of a single WebSocket message on /terminal, /host, and /mcp.

--host (or HOST env var) defaults to "localhost" - binds loopback-only
unless set explicitly. Pass --host 0.0.0.0 (or a specific interface) for
standalone direct exposure with no reverse proxy in front; a reverse-proxy
deployment forwards to this process over loopback either way, so it
doesn't need this flag.

A token is required by default, even for plain local use. If --auth-token
(or AUTH_TOKEN) isn't set, one is auto-generated on first run, persisted to
~/.gridshell/token (or $GRIDSHELL_TOKEN_PATH), and reused on every future
launch. Never printed to the console - copied to the clipboard instead
(--copy-token repeats this), or read the file directly if no clipboard is
available. --no-auth-token disables the check entirely, not recommended
since /terminal spawns a real interactive shell with no other access
control. --regenerate-token forces a fresh value.

Three deployment shapes: local trusted use (auto-generated token, zero
setup); behind a reverse proxy terminating TLS (a token, no cert needed
here); standalone direct exposure (--wss plus a token; --wss with
--no-auth-token refuses to start). The token travels as each connection's
first message, not in the URL, so it never lands in a plaintext access log.

A WebSocket handshake bypasses the browser same-origin policy, so
loopback-only binding alone doesn't stop another page in the same browser
from connecting directly. Every connection's Origin header is checked
against an allow-list (*.googleusercontent.com, plus --allow-origin); a
request with no Origin header (gridshell-mcp, SheetsClient, curl) is
allowed through. Secondary layer only - the token is what actually gates
access.

Refuses to start as root/Administrator by default (--allow-root
overrides). /terminal spawns a shell at this process's own privilege
level, so running elevated hands that shell the same elevated privileges.
"""
import asyncio
import atexit
import hmac
import json
import os
import re
import secrets
import shutil
import signal
import ssl
import subprocess
import sys
import threading
from collections import deque
from datetime import datetime
from urllib.parse import parse_qs, urlparse

import websockets
from websockets.asyncio.server import serve


def log(*args):
    # Timestamped - local time + offset, not UTC, for whoever's running the server.
    print(datetime.now().astimezone().isoformat(timespec="seconds"), *args)


if sys.platform == "win32":
    from winpty import PtyProcess
else:
    # PtyProcessUnicode gives str in/out on read()/write(), matching pywinpty.
    from ptyprocess import PtyProcessUnicode as PtyProcess

# Matches CSI/OSC escape sequences, used to detect a chunk that's pure
# terminal negotiation (device-attribute queries, title-setting), no real
# content. See _is_pure_negotiation below.
_ANSI_SEQ_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b\][^\x07]*\x07|\x1b[()][A-Za-z0-9]")


def _is_pure_negotiation(chunk):
    return _ANSI_SEQ_RE.sub("", chunk) == ""


# The specific DA1/DA2/cursor-position query sequences xterm.js
# auto-answers - narrower than _ANSI_SEQ_RE above.
_AUTO_ANSWERED_QUERY_RE = re.compile(r"\x1b\[>?0?c|\x1b\[6n")


def _ingest_into_buffer(this_session, data):
    # Strip one-time startup negotiation from the replay buffer. Replaying
    # it makes xterm.js re-answer it, which PSReadLine can misparse as a
    # keystroke. Only checked pre-handshake: a permanent whole-chunk filter
    # would also strip legitimate TUI redraw chunks.
    if not this_session["handshake_done"]:
        if _is_pure_negotiation(data):
            return
        this_session["handshake_done"] = True
    else:
        # A TUI's own later DA1 probe would otherwise sit in the buffer and
        # get re-answered on every reattach. Strips just the query bytes.
        data = _AUTO_ANSWERED_QUERY_RE.sub("", data)
        if not data:
            return
    # Runs on the reader thread, buffer_lock guards this against attach()'s
    # "".join(buffer) on the event loop.
    with this_session["buffer_lock"]:
        this_session["buffer"].append(data)
        this_session["buffer_bytes"] += len(data.encode("utf-8"))
        while this_session["buffer_bytes"] > this_session["buffer_max_bytes"] and len(this_session["buffer"]) > 1:
            dropped = this_session["buffer"].pop(0)
            this_session["buffer_bytes"] -= len(dropped.encode("utf-8"))


def _split_for_max_message(text, max_bytes):
    # A replay buffer bigger than --max-message-mb would otherwise go out
    # as one oversized send, silently dropped by both ends - split into
    # multiple sends instead. Conservative budget: JSON escaping can expand
    # 1 character up to 12 bytes (a surrogate-pair \uXXXX\uXXXX), divided
    # here rather than measured and retried.
    envelope_overhead = 32  # {"type":"output","data":"..."}
    budget = max(1, (max_bytes - envelope_overhead) // 12)
    return [text[i:i + budget] for i in range(0, len(text), budget)]

DEFAULT_IDLE_KILL_MS = 12 * 60 * 60 * 1000
DEFAULT_BUFFER_MAX_BYTES = 8 * 1024 * 1024
# Caps live-output chunks queued for a slow client. Reader_loop reads at
# most 4096 bytes per chunk, so this bounds pending memory to ~1MB per
# session regardless of how far behind the client falls. Independent of
# the replay buffer above, which keeps its own cap for reattach scrollback.
OUTPUT_QUEUE_MAX_CHUNKS = 256
# Raised well above the websockets default (1 MiB), a normal Sheets range
# easily exceeds that.
DEFAULT_MAX_MESSAGE_BYTES = 64 * 1024 * 1024


def shell_command():
    if sys.platform == "win32":
        return "powershell.exe"
    return os.environ.get("SHELL", "/bin/bash")


def _spawn_argv():
    # pywinpty accepts a bare string; ptyprocess requires a real list/tuple.
    cmd = shell_command()
    return cmd if sys.platform == "win32" else [cmd]


def _shell_display_path():
    # Full resolved path, not bare command name - the client 
    # only collapses paths that have a separator.
    return shutil.which(shell_command()) or shell_command()


def parse_hours_param(qs, name, ceiling_ms):
    # 0 means explicit zero grace, matching --idle-hours 0 on the CLI.
    # Negative/unparsable/missing all mean "no override".
    # Clamped to ceiling_ms - an untrusted page shouldn't be able to opt a
    # session out of idle-kill by passing a huge value.
    raw = qs.get(name, [None])[0]
    if raw is None:
        return None
    try:
        hours = float(raw)
    except ValueError:
        return None
    if hours < 0:
        return None
    return min(hours * 60 * 60 * 1000, ceiling_ms)


def parse_mb_param(qs, name, ceiling_bytes):
    # Clamped for the same reason as parse_hours_param - bounds per-session
    # memory. Unlike there, 0 means "no override" here, not an explicit
    # zero, a zero-size buffer isn't meaningful.
    raw = qs.get(name, [None])[0]
    if raw is None:
        return None
    try:
        mb = float(raw)
    except ValueError:
        return None
    return min(mb * 1024 * 1024, ceiling_bytes) if mb > 0 else None


# v1 free-tier cap, mirrors the client's own Pro-tier-reserved limit - not
# a CLI flag for the same reason. Enforced per doc_id (see
# _sessions_for_doc); used as the fallback default when a doc's first
# connection didn't declare its own cap (see TerminalHub.doc_caps).
MAX_CONCURRENT_SESSIONS = 2


class TerminalHub:
    """Owns the /terminal session table, sessions outlive any one socket,
    keyed by ?session= (or an anon key if omitted)."""

    def __init__(self, defaults):
        self.sessions = {}  # key -> session dict, see new_session's own shape below
        self.anon_counter = 0
        self.defaults = defaults
        # Set by create_app(). Notified on "terminate" so pending /host
        # calls for this session fail fast instead of timing out.
        self.host_bridge = None
        # Set by create_app(), lets spawn_process() hand the token to a
        # client launched inside the shell (GRIDSHELL_AUTH_TOKEN below).
        self.required_token = None
        # Overwritten by create_app() with the real configured ceiling.
        # This default only matters if TerminalHub is ever built directly.
        self.max_message_bytes = DEFAULT_MAX_MESSAGE_BYTES
        # doc_id -> the session cap latched from that doc's first /terminal
        # connection (see _effective_cap_for_doc). Resets on server restart,
        # same as self.sessions.
        self.doc_caps = {}

    def _sessions_for_doc(self, doc_id):
        return sum(1 for s in self.sessions.values() if s.get("doc_id", "") == doc_id)

    def _effective_cap_for_doc(self, doc_id, declared_cap):
        # Latched from whichever /terminal connection for this doc_id
        # arrives first, for the rest of this server run. Later
        # connections cannot change it. A client that omits
        # or sends a malformed value falls back to the hardcoded default.
        if doc_id not in self.doc_caps:
            if isinstance(declared_cap, int) and not isinstance(declared_cap, bool) and declared_cap > 0:
                self.doc_caps[doc_id] = declared_cap
            else:
                self.doc_caps[doc_id] = MAX_CONCURRENT_SESSIONS
        return self.doc_caps[doc_id]

    async def handle(self, ws, session_id, host_key="", doc_id="", declared_cap=None):
        loop = asyncio.get_running_loop()
        qs = parse_qs(urlparse(ws.request.path).query)
        cwd_param = (qs.get("cwd") or [None])[0]
        spawn_cwd = None
        cwd_warning = None
        if cwd_param:
            if os.path.isabs(cwd_param) and os.path.isdir(cwd_param):
                spawn_cwd = os.path.normpath(cwd_param)
            else:
                # A relative path, typo, missing directory, wrong-OS path,
                # or a path to a file all fall back to the server's
                # default silently otherwise - told to the client once,
                # right after spawn.
                cwd_warning = "Working directory %r isn't usable on this server - started in the default location instead." % cwd_param
        idle_kill_ms_param = parse_hours_param(qs, "idleHours", self.defaults["idle_kill_ms"])
        buffer_max_bytes_param = parse_mb_param(qs, "bufferMb", self.defaults["buffer_max_bytes"])

        if session_id:
            key = session_id
        else:
            self.anon_counter += 1
            key = "anon-%d-%d" % (self.anon_counter, int(loop.time() * 1000))

        pending_cols = [80]
        pending_rows = [24]
        session = self.sessions.get(key)
        log_prefix = "[terminal:%s]" % key

        async def attach(sess):
            nonlocal session
            session = sess
            if idle_kill_ms_param is not None:
                sess["idle_kill_ms"] = idle_kill_ms_param
            if buffer_max_bytes_param is not None:
                sess["buffer_max_bytes"] = buffer_max_bytes_param
            if sess["idle_timer"] is not None:
                sess["idle_timer"].cancel()
                sess["idle_timer"] = None
            old_ws = sess["ws"]
            if old_ws is not None and old_ws is not ws:
                try:
                    await old_ws.send(json.dumps({"type": "evicted"}))
                    await old_ws.close()
                except Exception:
                    pass
            # send_lock keeps this atomic with reader_loop's live forwarding,
            # so a live chunk can't interleave with this replay. buffer_lock
            # separately guards the "".join against the reader thread.
            with sess["buffer_lock"]:
                replay = "".join(sess["buffer"]) if sess["buffer"] else None
            async with sess["send_lock"]:
                sess["ws"] = ws
                try:
                    await ws.send(json.dumps({"type": "attached"}))
                    if replay is not None:
                        for piece in _split_for_max_message(replay, self.max_message_bytes):
                            await ws.send(json.dumps({"type": "output", "data": piece}))
                except Exception as exc:
                    # This log line is the only signal anything went wrong -
                    # the client never learns its scrollback replay didn't
                    # (fully) arrive.
                    log(log_prefix, "failed to send attach/replay:", exc)
            log(log_prefix, "attached to session:", key)

        if session:
            # Reattach requires the same host_key /host already requires -
            # a session id alone isn't proof of ownership. Not checked on a
            # fresh spawn (session is None here); nothing exists yet to
            # compare against.
            expected_key = session.get("host_key")
            if not expected_key or not hmac.compare_digest(
                (host_key or "").encode("utf-8"), expected_key.encode("utf-8")
            ):
                log(log_prefix, "rejected: invalid or missing host key")
                await ws.close(code=1008, reason="invalid or missing host key")
                return
            await attach(session)

        async def send_output(this_session, data):
            async with this_session["send_lock"]:
                target_ws = this_session["ws"]
                if target_ws is not None:
                    try:
                        await target_ws.send(json.dumps({"type": "output", "data": data}))
                    except Exception:
                        pass

        async def drain_send_queue(this_session):
            # One of these runs at a time per session (send_pending guards
            # that), draining in order, rather than one coroutine per
            # chunk piling up unbounded when the client is slower than
            # the shell.
            while True:
                with this_session["send_queue_lock"]:
                    if not this_session["send_queue"]:
                        this_session["send_pending"] = False
                        return
                    data = this_session["send_queue"].popleft()
                await send_output(this_session, data)

        def reader_loop(proc, this_session):
            while not this_session["stop_reader"].is_set():
                try:
                    data = proc.read(4096)
                except EOFError:
                    break
                except Exception as exc:
                    log(log_prefix, "reader error:", exc)
                    break
                if not data:
                    continue
                _ingest_into_buffer(this_session, data)
                with this_session["send_queue_lock"]:
                    # Bounded deque(maxlen=...) - appending past capacity
                    # silently drops the oldest unsent output instead of
                    # growing memory without bound. Live output only; the
                    # replay buffer above keeps its own separate cap.
                    this_session["send_queue"].append(data)
                    already_pending = this_session["send_pending"]
                    this_session["send_pending"] = True
                if already_pending:
                    continue
                try:
                    asyncio.run_coroutine_threadsafe(drain_send_queue(this_session), loop)
                except RuntimeError:
                    break
            # Only act if this session is still current under key (not
            # already replaced by a respawn or deleted by "terminate").
            if self.sessions.get(key) is this_session:
                cur_ws = this_session["ws"]
                if cur_ws is not None:
                    try:
                        asyncio.run_coroutine_threadsafe(cur_ws.close(), loop)
                    except RuntimeError:
                        pass
                del self.sessions[key]
                log(log_prefix, "shell exited")

        def spawn_process():
            nonlocal session
            env = dict(os.environ)
            session_key = None
            if session_id:
                session_key = secrets.token_urlsafe(32)
                env["GRIDSHELL_SESSION"] = session_id
                # Per-spawn secret - session_id alone isn't a credential
                # (readable from another shell's environ); handle_mcp()
                # requires this to match.
                env["GRIDSHELL_SESSION_KEY"] = session_key
            if self.required_token:
                # Auto-pickup for a client launched inside this shell, no
                # manual token config needed.
                env["GRIDSHELL_AUTH_TOKEN"] = self.required_token
            # Force TERM to match xterm.js's emulation - a missing/minimal
            # ambient TERM breaks color and alt-screen restore in ncurses
            # apps (nano, vim).
            if sys.platform != "win32":
                env["TERM"] = "xterm-256color"
            proc = PtyProcess.spawn(
                _spawn_argv(), cwd=spawn_cwd, env=env,
                dimensions=(pending_rows[0], pending_cols[0]),
            )
            new_session = {
                "proc": proc,
                "ws": ws,
                "buffer": [],
                "buffer_bytes": 0,
                # Guards buffer/buffer_bytes across the reader thread and
                # the event loop, see _ingest_into_buffer.
                "buffer_lock": threading.Lock(),
                "idle_timer": None,
                "idle_kill_ms": idle_kill_ms_param if idle_kill_ms_param is not None else self.defaults["idle_kill_ms"],
                "buffer_max_bytes": buffer_max_bytes_param if buffer_max_bytes_param is not None else self.defaults["buffer_max_bytes"],
                "stop_reader": threading.Event(),
                "session_key": session_key,
                # Handed to /host by whoever's dialog spawned this session
                # (the client mints it, never this server), required again on
                # /host so naming a session id there isn't enough on its own.
                "host_key": host_key,
                # Serializes sends, see attach()'s comment.
                "send_lock": asyncio.Lock(),
                # Bounded live-output queue, see reader_loop/drain_send_queue.
                # Thread-safe handoff between the reader thread and the
                # event loop, separate from send_lock (which only
                # serializes the actual ws.send calls).
                "send_queue": deque(maxlen=OUTPUT_QUEUE_MAX_CHUNKS),
                "send_queue_lock": threading.Lock(),
                "send_pending": False,
                # See reader_loop's comment above _is_pure_negotiation calls.
                "handshake_done": False,
                # Groups this session for MAX_CONCURRENT_SESSIONS.
                "doc_id": doc_id,
            }
            self.sessions[key] = new_session
            session = new_session
            threading.Thread(target=reader_loop, args=(proc, new_session), daemon=True).start()
            log(log_prefix, "shell started, cwd:", spawn_cwd or os.getcwd(),
                  "at", "%dx%d" % (pending_cols[0], pending_rows[0]))
            return new_session

        try:
            async for raw in ws:
                try:
                    m = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                mtype = m.get("type")
                if mtype == "resize":
                    try:
                        cols = max(1, int(m.get("cols", 80)))
                        rows = max(1, int(m.get("rows", 24)))
                    except (TypeError, ValueError):
                        # /terminal is reachable by anything holding the
                        # token, not just the stock client - a malformed
                        # value is caught here rather than raising out of
                        # the loop and killing the connection silently.
                        continue
                    pending_cols[0] = cols
                    pending_rows[0] = rows
                    if session is None:
                        effective_cap = self._effective_cap_for_doc(doc_id, declared_cap)
                        if self._sessions_for_doc(doc_id) >= effective_cap:
                            # Mirrors client-side limit
                            # per document (latched from this doc's first
                            # connection, see _effective_cap_for_doc).
                            # 1013 rather than 1008 so the client doesn't
                            # show the token-specific rejection hint.
                            await ws.close(code=1013, reason="session limit reached (%d) for this document" % effective_cap)
                            return
                        new_session = spawn_process()
                        await ws.send(json.dumps({"type": "spawned"}))
                        if cwd_warning:
                            await ws.send(json.dumps({"type": "warning", "message": cwd_warning}))
                        # Synthesize an initial title, ConPTY doesn't
                        # forward one on its own.
                        title_data = "\x1b]0;%s\x07" % _shell_display_path()
                        _ingest_into_buffer(new_session, title_data)
                        await send_output(new_session, title_data)
                        session = new_session
                    else:
                        session["proc"].setwinsize(pending_rows[0], pending_cols[0])
                        await ws.send(json.dumps({
                            "type": "resized", "cols": pending_cols[0], "rows": pending_rows[0]
                        }))
                elif mtype == "input":
                    if session is not None:
                        data = m.get("data", "")
                        # xterm.js auto-answers the shell's DA1 startup
                        # query via onData. Under load this can arrive
                        # late enough for PSReadLine to insert it literally.
                        # Matches _ingest_into_buffer's reattach-side filter.
                        if not session["handshake_done"] and _is_pure_negotiation(data):
                            continue
                        session["proc"].write(data)
                elif mtype == "terminate":
                    if session is not None:
                        if session["idle_timer"] is not None:
                            session["idle_timer"].cancel()
                        try:
                            session["proc"].terminate(force=True)
                        except Exception as exc:
                            log(log_prefix, "terminate() failed:", exc)
                        if self.sessions.get(key) is session:
                            del self.sessions[key]
                        if session_id and self.host_bridge is not None:
                            self.host_bridge.fail_pending(session_id, "Shell was closed.")
                        log(log_prefix, "session terminated on request")
                        # The client waits for this before treating the kill as confirmed, 
                        # rather than assuming success once the message was merely sent.
                        try:
                            await ws.send(json.dumps({"type": "terminated"}))
                        except Exception:
                            pass
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            if session is not None and session["ws"] is ws and self.sessions.get(key) is session:
                session["ws"] = None

                def idle_kill():
                    if session["ws"] is None:
                        try:
                            session["proc"].terminate(force=True)
                        except Exception:
                            pass
                        if self.sessions.get(key) is session:
                            del self.sessions[key]
                        log(log_prefix, "session idle-killed")

                session["idle_timer"] = loop.call_later(session["idle_kill_ms"] / 1000, idle_kill)
            log(log_prefix, "connection closed")


class HostBridge:
    """Ports /host: one connection per open shell, keyed by ?session=.

    A call for a session with no client connected is held in `pending`
    (`sent=False`), flushed to that session's next connection (see
    _flush_pending). Resolves via its own timer if no reconnect happens,
    or fails immediately via fail_pending if the shell closes first. A
    call already sent (`sent=True`) whose client disconnects mid-response
    is never retried - failed immediately instead via _fail_in_flight.
    """

    def __init__(self):
        self.clients = {}  # sessionId -> ws
        self.pending = {}  # relayId -> {resolve, timer, session_id, message, sent, ws}
        self.next_relay_id = 1
        # Set by create_app() - lets handle_mcp() look up the per-spawn
        # session_key for a session id, so an explicit ?session=...
        # connection must prove it owns that session, not just name it.
        self.terminal_hub = None

    def resolve_client(self, session_id):
        if session_id:
            ws = self.clients.get(session_id)
            return ws if ws is not None else None
        if len(self.clients) == 1:
            return next(iter(self.clients.values()))
        return None

    async def _flush_pending(self, session_id, ws):
        for relay_id, entry in list(self.pending.items()):
            if entry["session_id"] == session_id and not entry["sent"]:
                entry["sent"] = True
                entry["ws"] = ws
                try:
                    await ws.send(json.dumps(entry["message"]))
                except Exception:
                    pass

    def fail_pending(self, session_id, reason):
        for relay_id, entry in list(self.pending.items()):
            if entry["session_id"] == session_id and not entry["sent"]:
                entry["timer"].cancel()
                del self.pending[relay_id]
                asyncio.ensure_future(entry["resolve"]({"error": reason}))

    def _fail_in_flight(self, session_id, ws, too_large=False):
        # Fails immediately rather than waiting for a timeout, scoped to
        # this exact ws so a newer reconnect isn't touched. too_large is
        # the exception: nothing was delivered, so "outcome unknown"
        # doesn't apply.
        if too_large:
            reason = ("The response was too large for this connection's message-size limit "
                      "and never arrived - nothing was delivered, safe to retry with a "
                      "smaller range, or raise --max-message-mb on the server.")
        else:
            reason = "Bridge disconnected while awaiting a response - outcome unknown, verify before retrying."
        for relay_id, entry in list(self.pending.items()):
            if entry["session_id"] == session_id and entry["sent"] and entry.get("ws") is ws:
                entry["timer"].cancel()
                del self.pending[relay_id]
                asyncio.ensure_future(entry["resolve"]({"error": reason}))

    async def handle(self, ws, session_id, host_key=""):
        # Naming a session id here isn't proof of owning it either, same
        # reasoning as handle_mcp's session_key check below.
        session = self.terminal_hub.sessions.get(session_id) if self.terminal_hub else None
        if session is None:
            # Not a rejection, the sidebar opens /host before its dialog
            # has spawned the PTY, so this resolves itself once it does.
            # 1013, not 1008, so the sidebar keeps retrying.
            log("[host] no active session yet for %s - closing so the sidebar retries" % session_id)
            await ws.close(code=1013, reason="session not ready yet")
            return
        expected_key = session.get("host_key")
        if not expected_key or not hmac.compare_digest(
            (host_key or "").encode("utf-8"), expected_key.encode("utf-8")
        ):
            log("[host] rejected connection for session %s: invalid or missing host key" % session_id)
            await ws.close(code=1008, reason="invalid or missing host key")
            return
        old_ws = self.clients.get(session_id)
        if old_ws is not None and old_ws is not ws:
            # A silently overwritten dict entry left the old socket open
            # with nothing routing to it. Closing it explicitly lets its
            # own client see the disconnect and reconnect instead of
            # looking "connected" while every call to it times out.
            try:
                # Not 1008 - that means "don't bother retrying" to the
                # sidebar. This is a routine handover, not a rejection: the
                # old tab should keep retrying to reclaim the bridge if
                # the newer connection later drops.
                await old_ws.close(code=4000, reason="superseded by a new /host connection")
            except Exception:
                pass
        self.clients[session_id] = ws
        await self._flush_pending(session_id, ws)
        log("[host] client connected" + (" (session %s)" % session_id if session_id else ""))
        too_large = False
        try:
            async for raw in ws:
                try:
                    m = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                pending = self.pending.get(m.get("id"))
                if pending:
                    pending["timer"].cancel()
                    del self.pending[m["id"]]
                    await pending["resolve"](m)
        except websockets.exceptions.ConnectionClosed as exc:
            # An over-max_size frame fails with 1009, checked on the
            # exception, not ws.close_code (which ends up 1006 here).
            sent = getattr(exc, "sent", None)
            too_large = sent is not None and sent.code == 1009
        finally:
            log("[host] client disconnected" + (" (session %s)" % session_id if session_id else ""))
            if self.clients.get(session_id) is ws:
                del self.clients[session_id]
            self._fail_in_flight(session_id, ws, too_large=too_large)

    async def handle_mcp(self, ws, session_id, session_key):
        loop = asyncio.get_running_loop()
        if session_id:
            # Naming a session id isn't proof of owning it - require the
            # matching per-spawn secret (spawn_process's GRIDSHELL_SESSION_KEY).
            provided_key = session_key or ""
            session = self.terminal_hub.sessions.get(session_id) if self.terminal_hub else None
            expected_key = session.get("session_key") if session else None
            if not expected_key or not hmac.compare_digest(
                provided_key.encode("utf-8"), expected_key.encode("utf-8")
            ):
                log("[mcp] rejected connection for session %s: invalid or missing session key" % session_id)
                await ws.close(code=1008, reason="invalid or missing session key")
                return
        log("[mcp] MCP server connected" + (" (session %s)" % session_id if session_id else ""))
        try:
            async for raw in ws:
                try:
                    m = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue

                target = self.resolve_client(session_id)
                # No session_id is ambiguous, not a transient gap - fail
                # immediately. An explicit session_id with no client
                # queues instead (see class docstring).
                if target is None and not session_id:
                    if len(self.clients) > 1:
                        error = ("Multiple clients connected and no session was specified - run "
                                 "this MCP server from a document's embedded terminal, or set "
                                 "GRIDSHELL_SESSION.")
                    else:
                        error = "Client not connected"
                    await ws.send(json.dumps({"id": m.get("id"), "error": error}))
                    continue

                relay_id = "r%d" % self.next_relay_id
                self.next_relay_id += 1
                caller_id = m.get("id")
                relay_timeout = m.get("timeoutMs") or 15000
                if relay_timeout <= 0:
                    relay_timeout = 15000

                def make_timeout(rid, cid, sock):
                    async def send_timeout():
                        try:
                            await sock.send(json.dumps({"id": cid, "error": "Timeout waiting for client"}))
                        except Exception:
                            pass
                    def on_timeout():
                        if rid in self.pending:
                            del self.pending[rid]
                            asyncio.ensure_future(send_timeout())
                    return on_timeout

                timer = loop.call_later(
                    relay_timeout / 1000,
                    make_timeout(relay_id, caller_id, ws),
                )

                async def resolve(result, cid=caller_id, sock=ws):
                    result["id"] = cid
                    try:
                        await sock.send(json.dumps(result))
                    except Exception:
                        pass

                m["id"] = relay_id
                self.pending[relay_id] = {
                    "resolve": resolve, "timer": timer,
                    "session_id": session_id, "message": m,
                    "sent": target is not None, "ws": target,
                    "mcp_ws": ws,  # this /mcp connection, see handle_mcp's finally
                }
                if target is not None:
                    await target.send(json.dumps(m))
                # else: left pending, delivered by _flush_pending() on
                # reconnect, or timed out/failed above.
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            log("[mcp] MCP server disconnected" + (" (session %s)" % session_id if session_id else ""))
            # Otherwise these survive until their own timer fires and tries
            # sending to this now-closed socket - nothing else resolves them.
            for relay_id, entry in list(self.pending.items()):
                if entry.get("mcp_ws") is ws:
                    entry["timer"].cancel()
                    del self.pending[relay_id]


def create_app(idle_hours, buffer_mb, required_token=None, allowed_origin_suffixes=(), max_message_bytes=None):
    # Separated from main() so tests can build a router without the
    # CLI/asyncio.run bootstrap.
    terminal_hub = TerminalHub({
        "idle_kill_ms": idle_hours * 60 * 60 * 1000,
        "buffer_max_bytes": buffer_mb * 1024 * 1024,
    })
    # Lets attach() chunk a buffer replay below the actual wire limit
    # instead of risking one oversized send that's silently dropped, see
    # its own comment. Defaults to the same ceiling serve() itself would.
    terminal_hub.max_message_bytes = max_message_bytes or DEFAULT_MAX_MESSAGE_BYTES
    host_bridge = HostBridge()
    terminal_hub.host_bridge = host_bridge
    terminal_hub.required_token = required_token
    host_bridge.terminal_hub = terminal_hub

    async def router(ws):
        # See _is_allowed_origin and the module docstring - secondary
        # layer only, not the primary trust boundary.
        origin = ws.request.headers.get("Origin")
        if not _is_allowed_origin(origin, allowed_origin_suffixes):
            log("[auth] rejected connection from disallowed origin:", origin)
            await ws.close(code=1008, reason="origin not allowed")
            return
        # Credentials travel as the first message, not the URL, see the
        # module docstring. Non-secret params stay in the URL.
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
            auth = json.loads(raw)
            if not isinstance(auth, dict):
                raise ValueError("auth frame must be a JSON object")
        except (asyncio.TimeoutError, json.JSONDecodeError, TypeError, ValueError,
                websockets.exceptions.ConnectionClosed):
            log("[auth] rejected connection from", ws.remote_address,
                "path", urlparse(ws.request.path).path, "- missing or malformed auth frame")
            try:
                await ws.close(code=1008, reason="missing or malformed auth frame")
            except Exception:
                pass
            return
        token = auth.get("token") or ""
        session_id = auth.get("session") or ""
        session_key = auth.get("sessionKey") or ""
        host_key = auth.get("hostKey") or ""
        doc_id = auth.get("docId") or ""
        # /terminal only - the per-document session cap this doc's first
        # connection declares. See TerminalHub._effective_cap_for_doc.
        declared_cap = auth.get("declaredCap")
        # session_id alone isn't a credential, required_token is what
        # actually gates access. See the module docstring.
        if required_token is not None:
            # Compare bytes, not str - hmac.compare_digest(str, str)
            # raises on non-ASCII input instead of returning False.
            if not hmac.compare_digest(token.encode("utf-8"), required_token.encode("utf-8")):
                # Logged with the peer address, never the attempted token.
                log("[auth] rejected connection from", ws.remote_address, "path", urlparse(ws.request.path).path)
                await ws.close(code=1008, reason="invalid or missing token")
                return
        path = urlparse(ws.request.path).path
        if path == "/terminal":
            await terminal_hub.handle(ws, session_id, host_key, doc_id, declared_cap)
        elif path == "/host":
            await host_bridge.handle(ws, session_id, host_key)
        elif path == "/mcp":
            await host_bridge.handle_mcp(ws, session_id, session_key)
        else:
            await ws.close(code=1008, reason="unknown path: %s" % path)

    return router, terminal_hub, host_bridge


def _kill_all_sessions(terminal_hub):
    for session in list(terminal_hub.sessions.values()):
        try:
            session["proc"].terminate(force=True)
        except Exception:
            pass


def _install_shutdown_hook(terminal_hub):
    # Ties spawned shells' lifetime to the server process, without this a
    # restart orphans every running PTY. atexit covers Ctrl+C; SIGTERM
    # needs an explicit handler (POSIX only). A hard crash or forceful
    # kill still isn't covered.
    atexit.register(_kill_all_sessions, terminal_hub)

    def _handle_signal(signum, frame):
        _kill_all_sessions(terminal_hub)
        sys.exit(0)

    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, _handle_signal)
        except (ValueError, OSError):
            # signal.signal() requires the main thread, skip rather than crash.
            pass


_LOOPBACK_HOSTS = ("localhost", "127.0.0.1", "::1", "[::1]")

# Apps Script serves the Sidebar/Dialog from a googleusercontent.com subdomain.
_ALLOWED_ORIGIN_SUFFIXES = (".googleusercontent.com",)


def _is_allowed_origin(origin, extra_suffixes):
    # No Origin header means a non-browser client (curl, gridshell-mcp). 
    if origin is None:
        return True
    host = urlparse(origin).hostname or ""
    all_suffixes = _ALLOWED_ORIGIN_SUFFIXES + extra_suffixes
    # Exact match too (suffix minus its leading dot). A bare "example.com"
    # suffix should also allow the host "example.com" itself, not just
    # subdomains of it.
    return any(host == suffix.lstrip(".") or host.endswith(suffix) for suffix in all_suffixes)


def _running_as_root_or_admin():
    if sys.platform == "win32":
        try:
            import ctypes
            return bool(ctypes.windll.shell32.IsUserAnAdmin())
        except Exception:
            return False
    return hasattr(os, "geteuid") and os.geteuid() == 0


def _default_token_path():
    override = os.environ.get("GRIDSHELL_TOKEN_PATH")
    if override:
        return override
    return os.path.expanduser(os.path.join("~", ".gridshell", "token"))


def _persisted_token_exists():
    try:
        with open(_default_token_path(), "r", encoding="utf-8") as f:
            return bool(f.read().strip())
    except OSError:
        return False


def _try_clipboard_copy(text):
    # Best-effort, never raises. GRIDSHELL_NO_CLIPBOARD is an internal test
    # seam, not a documented flag.
    if os.environ.get("GRIDSHELL_NO_CLIPBOARD"):
        return False
    try:
        if sys.platform == "win32":
            subprocess.run(["clip"], input=text.encode("utf-16-le"), check=True, timeout=5)
            return True
        if sys.platform == "darwin":
            subprocess.run(["pbcopy"], input=text.encode("utf-8"), check=True, timeout=5)
            return True
        for cmd in (["wl-copy"], ["xclip", "-selection", "clipboard"], ["xsel", "--clipboard", "--input"]):
            if shutil.which(cmd[0]):
                subprocess.run(cmd, input=text.encode("utf-8"), check=True, timeout=5)
                return True
        return False
    except Exception:
        return False


def _load_or_create_default_token(explicit_token, no_auth_token, regenerate):
    # Persisted across restarts, the client's settings are configured once.
    if no_auth_token:
        return None
    if explicit_token:
        return explicit_token
    path = _default_token_path()
    if not regenerate:
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = f.read().strip()
            if existing:
                return existing
        except OSError:
            pass
    token = secrets.token_urlsafe(32)
    try:
        dirname = os.path.dirname(path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(token)
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
    except OSError as exc:
        log("[auth] could not persist a generated token to %s (%s) - using an "
            "in-memory token for this run only; it will change on restart." % (path, exc))
    return token


async def main(port, idle_hours, buffer_mb, cert_path, key_path, use_wss, auth_token, host,
               allow_origins, max_message_mb, allow_root):
    if _running_as_root_or_admin() and not allow_root:
        sys.exit(
            "Refusing to start as root/Administrator - /terminal spawns a real "
            "interactive shell, so running this process elevated hands that shell "
            "the same elevated privileges. Pass --allow-root if you really mean it."
        )
    if use_wss and not auth_token:
        sys.exit(
            "--wss with --no-auth-token has no other safeguard at all - refusing to "
            "start /terminal (a real interactive shell), /host, and /mcp reachable "
            "beyond localhost with no authentication at all."
        )
    if use_wss and not (cert_path and key_path):
        sys.exit("--wss requires --cert-path and --key-path pointing at a real certificate.")
    # Same exposure as the --wss guard above, minus TLS - refused for
    # consistency. Only triggers with an explicit --no-auth-token.
    if host not in _LOOPBACK_HOSTS and not auth_token:
        sys.exit(
            "--host %s is not loopback-only and --no-auth-token was passed - "
            "refusing to start /terminal (a real interactive shell), /host, "
            "and /mcp reachable beyond localhost with no authentication at all." % host
        )
    max_message_bytes = (
        max_message_mb * 1024 * 1024 if max_message_mb is not None else DEFAULT_MAX_MESSAGE_BYTES
    )
    # required_token flows in independent of --wss, see the module docstring.
    router, terminal_hub, _host_bridge = create_app(
        idle_hours, buffer_mb, required_token=auth_token,
        allowed_origin_suffixes=tuple(allow_origins), max_message_bytes=max_message_bytes,
    )
    _install_shutdown_hook(terminal_hub)

    ssl_context = None
    if use_wss:
        # Purpose.CLIENT_AUTH pins the same guarantees (TLS 1.2 floor, no
        # compression) regardless of the OpenSSL build the operator happens
        # to have, which matters for a bare SSLContext(PROTOCOL_TLS_SERVER) 
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        try:
            ssl_context.load_cert_chain(cert_path, key_path)
        except (OSError, ssl.SSLError) as exc:
            sys.exit("Could not load the TLS certificate/key pair (--cert-path %s, --key-path %s): %s"
                      % (cert_path, key_path, exc))

    async with serve(router, host, port, ssl=ssl_context, max_size=max_message_bytes) as server:
        scheme = "wss" if use_wss else "ws"
        print("Terminal server (Python) running at %s://%s:%d" % (scheme, host, port))
        if auth_token:
            # Token never printed, see resolve_flags()'s clipboard-or-refuse handling above.
            print("  Sheets sidebar Server address: %s:%d  (paste the auth token into the Auth token field - see above for how to retrieve it, or run `gridshell-server --copy-token`)" % (host, port))
        print("  /terminal  xterm.js I/O")
        print("  /host      Host command bridge")
        print("  /mcp       MCP server relay")
        await asyncio.Future()


USAGE = """gridshell-server [--port N] [--host HOST] [--idle-hours H] [--buffer-mb M]
              [--wss --cert-path FILE --key-path FILE] [--auth-token TOKEN]
              [--no-auth-token] [--regenerate-token] [--copy-token] [--allow-root]
              [--allow-origin HOST_SUFFIX[,HOST_SUFFIX...]] [--max-message-mb M]

  --port N            Port to listen on (default 3000, or $PORT)
  --host HOST         Bind address (default localhost, or $HOST) - a
                      non-loopback value requires a token (auto-generated
                      by default; refused only with --no-auth-token)
  --idle-hours H      Idle time before a disconnected shell is killed
                      (default 12, or $IDLE_HOURS)
  --buffer-mb M       Max size of the per-session output replay buffer
                      (default 8, or $BUFFER_MB)
  --wss               Serve wss:// directly (requires --cert-path,
                      --key-path, and a token)
  --cert-path FILE    TLS certificate (or $CERT_PATH), required with --wss
  --key-path FILE     TLS private key (or $KEY_PATH), required with --wss
  --auth-token TOKEN  Shared secret required on every connection
                      (or $AUTH_TOKEN). If not set, one is auto-generated
                      and persisted to ~/.gridshell/token (or
                      $GRIDSHELL_TOKEN_PATH), reused on future launches.
  --no-auth-token     Disable the auth token entirely. Not recommended
                      unless access is restricted at a different layer -
                      refused together with --wss or a non-loopback --host.
  --regenerate-token  Force a fresh auto-generated token this launch,
                      replacing the persisted one. Not valid together
                      with --auth-token or --no-auth-token.
  --copy-token        Resolve the configured token (persisted/auto-generated,
                      or an explicit --auth-token/AUTH_TOKEN) and copy it to
                      the clipboard, then exit without starting the server.
                      The token is never printed to the console by this or
                      any other flag - only ever copied to the clipboard, or
                      readable directly from the persisted token file.
  --allow-root        Allow starting as root/Administrator (refused by
                      default - /terminal spawns a shell at this
                      process's own privilege level).
  --allow-origin H    Extra allowed Origin header suffix(es), comma-separated
                      (or $ALLOW_ORIGIN)
  --max-message-mb M  Max size of a single WebSocket message (default 64,
                      or $MAX_MESSAGE_MB)
  --help, -h          Show this message and exit
"""


def _parse_number(raw, flag, kind, minimum=None):
    try:
        value = kind(raw)
    except ValueError:
        sys.exit("%s must be a number, got %r" % (flag, raw))
    if minimum is not None and value < minimum:
        sys.exit("%s must be >= %s, got %s" % (flag, minimum, value))
    return value


def resolve_flags():
    argv = sys.argv[1:]
    use_wss = False
    no_auth_token = False
    regenerate_token = False
    copy_token = False
    allow_root = False
    # Hand-rolled: a handful of flags in a small copy-and-edit script,
    # argparse would be overkill for something meant to stay this simple.
    value_flags = {
        "--host": "host", "--port": "port", "--idle-hours": "idle_hours",
        "--buffer-mb": "buffer_mb", "--cert-path": "cert_path",
        "--key-path": "key_path", "--auth-token": "auth_token",
        "--allow-origin": "allow_origin", "--max-message-mb": "max_message_mb",
    }
    values = {}
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in ("--help", "-h"):
            print(USAGE)
            sys.exit(0)
        elif arg == "--wss":
            use_wss = True
        elif arg == "--no-auth-token":
            no_auth_token = True
        elif arg == "--regenerate-token":
            regenerate_token = True
        elif arg == "--copy-token":
            copy_token = True
        elif arg == "--allow-root":
            allow_root = True
        elif arg in value_flags:
            if i + 1 >= len(argv):
                sys.exit("%s requires a value" % arg)
            values[value_flags[arg]] = argv[i + 1]
            i += 1
        else:
            sys.exit("Unknown option: %s" % arg)
        i += 1
    host = values.get("host")
    port = _parse_number(values["port"], "--port", int, minimum=1) if "port" in values else None
    # 0 is a documented, meaningful value (explicit zero idle grace),
    # only negative values are nonsensical.
    idle_hours = _parse_number(values["idle_hours"], "--idle-hours", float, minimum=0) if "idle_hours" in values else None
    buffer_mb = _parse_number(values["buffer_mb"], "--buffer-mb", float, minimum=0.01) if "buffer_mb" in values else None
    cert_path = values.get("cert_path")
    key_path = values.get("key_path")
    auth_token = values.get("auth_token")
    allow_origin_raw = values.get("allow_origin")
    max_message_mb = (
        _parse_number(values["max_message_mb"], "--max-message-mb", float, minimum=0.01)
        if "max_message_mb" in values else None
    )
    if port is None:
        port = int(os.environ.get("PORT", 3000))
    if idle_hours is None:
        idle_hours = float(os.environ.get("IDLE_HOURS", 12))
    if buffer_mb is None:
        buffer_mb = float(os.environ.get("BUFFER_MB", 8))
    if cert_path is None:
        cert_path = os.environ.get("CERT_PATH")
    if key_path is None:
        key_path = os.environ.get("KEY_PATH")
    if auth_token is None:
        auth_token = os.environ.get("AUTH_TOKEN")
    if host is None:
        host = os.environ.get("HOST", "localhost")
    if allow_origin_raw is None:
        allow_origin_raw = os.environ.get("ALLOW_ORIGIN")
    # A dot boundary so "example.com" matches "sub.example.com" but not
    # "evilexample.com"
    allow_origins = [
        s if s.startswith(".") else "." + s
        for s in (p.strip() for p in allow_origin_raw.split(",")) if s
    ] if allow_origin_raw else []
    if max_message_mb is None and os.environ.get("MAX_MESSAGE_MB"):
        max_message_mb = float(os.environ["MAX_MESSAGE_MB"])
    if regenerate_token and (auth_token or no_auth_token):
        sys.exit(
            "--regenerate-token has no effect together with --auth-token or "
            "--no-auth-token - pass one or the other."
        )
    if copy_token:
        # Resolves the same token a real launch would use, but only ever via
        # clipboard, never stdout.
        token = _load_or_create_default_token(auth_token, no_auth_token, regenerate_token)
        if not token:
            sys.exit("No auth token is configured (--no-auth-token) - nothing to copy.")
        if _try_clipboard_copy(token):
            print("Auth token copied to clipboard.")
            sys.exit(0)
        sys.exit(
            "No clipboard is available on this system - gridshell-server never "
            "prints the token itself. Read it directly from %s (or from wherever "
            "--auth-token/AUTH_TOKEN sources it, if set explicitly)." % _default_token_path()
        )
    # Delivered only through the clipboard, never stdout (see module
    # docstring). Starts normally even with no clipboard, since the token
    # is already persisted to the file by then - only an actual persist
    # failure refuses to start.
    token_is_explicit = bool(auth_token)
    auth_token = _load_or_create_default_token(auth_token, no_auth_token, regenerate_token)
    if token_is_explicit:
        print("Using the auth token provided via --auth-token/AUTH_TOKEN.")
    elif no_auth_token:
        pass
    elif auth_token:
        if _try_clipboard_copy(auth_token):
            print("Auth token copied to clipboard.")
        elif _persisted_token_exists():
            print(
                "Auth token available at %s (clipboard unavailable - read it "
                "there, or run --copy-token once a clipboard is available)."
                % _default_token_path()
            )
        else:
            sys.exit(
                "Could not persist an auth token, and no clipboard is available "
                "to deliver one either way - refusing to start with no safe way "
                "to retrieve it. Retry, or pass --auth-token (or set AUTH_TOKEN) "
                "with a value you provide, or --no-auth-token to run without one."
            )
    return (port, idle_hours, buffer_mb, cert_path, key_path, use_wss, auth_token, host,
            allow_origins, max_message_mb, allow_root)


def run():
    """Entry point for the `gridshell-server` console command."""
    flags = resolve_flags()
    try:
        asyncio.run(main(*flags))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()
