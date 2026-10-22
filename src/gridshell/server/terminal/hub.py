"""/terminal: a TerminalHub owns the session table and serves each socket
through a TerminalConnection."""

import asyncio
import json
import os
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

import websockets
from websockets.asyncio.server import ServerConnection

from ..bridge import HostBridge
from ..common import DEFAULT_MAX_MESSAGE_BYTES, hello_message, keys_match, log, wire
from .session import Session, SessionTable, is_pure_negotiation
from .shell import SpawnFn, shell_display_path, spawn_shell

# Wait for the client's shell list; sessions younger than the grace aren't reaped.
RECONCILE_TIMEOUT_S = 5


RECONCILE_GRACE_S = 15


def parse_hours_param(qs: dict[str, list[str]], name: str, ceiling_ms: float) -> float:
    # 0 means zero grace (like --idle-hours 0); negative, unparsable or
    # missing means "no override". Clamped to ceiling_ms so an untrusted
    # page can't opt a session out of idle-kill.
    raw = qs.get(name, [None])[0]
    if raw is None:
        return ceiling_ms
    try:
        hours = float(raw)
    except ValueError:
        return ceiling_ms
    if hours < 0:
        return ceiling_ms
    return min(hours * 60 * 60 * 1000, ceiling_ms)


def parse_mb_param(qs: dict[str, list[str]], name: str, ceiling_bytes: float) -> float:
    # Clamped for the same reason as parse_hours_param - bounds per-session
    # memory. Unlike there, 0 means "no override" here, not an explicit
    # zero, a zero-size buffer isn't meaningful.
    raw = qs.get(name, [None])[0]
    if raw is None:
        return ceiling_bytes
    try:
        mb = float(raw)
    except ValueError:
        return ceiling_bytes
    return min(mb * 1024 * 1024, ceiling_bytes) if mb > 0 else ceiling_bytes


@dataclass(frozen=True)
class TerminalDefaults:
    """Per-session defaults a connection's query string may lower, see parse_*_param."""

    idle_kill_ms: float
    buffer_max_bytes: float


@dataclass(frozen=True)
class AuthFrame:
    """The first message of every connection, see create_app's router."""

    token: str = ""
    session: str = ""
    session_key: str = ""  # /mcp only: proves ownership of `session`
    host_key: str = ""
    doc_id: str = ""  # /terminal only: groups sessions for the per-document cap
    declared_cap: Any = None  # /terminal only: see SessionTable.effective_cap
    # /terminal only: optional protocol extensions the client understands
    # ("timeline", "reconcile", "hello", "control"). Anything unknown or malformed is
    # ignored, and a client that sends none gets the original behavior.
    caps: frozenset[str] = frozenset()

    @classmethod
    def parse(cls, raw: str | bytes) -> "AuthFrame":
        frame = json.loads(raw)
        if not isinstance(frame, dict):
            raise ValueError("auth frame must be a JSON object")
        raw_caps = frame.get("caps")
        caps = (
            frozenset(c for c in raw_caps if isinstance(c, str))
            if isinstance(raw_caps, list)
            else frozenset()
        )
        return cls(
            token=frame.get("token") or "",
            session=frame.get("session") or "",
            session_key=frame.get("sessionKey") or "",
            host_key=frame.get("hostKey") or "",
            doc_id=frame.get("docId") or "",
            declared_cap=frame.get("declaredCap"),
            caps=caps,
        )


@dataclass(frozen=True)
class SpawnParams:
    """What a connection's query string asks of the session it spawns."""

    cwd: str | None
    cwd_warning: str | None  # told to the client once, after spawn
    idle_kill_ms: float
    buffer_max_bytes: float

    @classmethod
    def from_query(cls, path: str, defaults: TerminalDefaults) -> "SpawnParams":
        qs = parse_qs(urlparse(path).query)
        cwd = None
        warning = None
        requested = (qs.get("cwd") or [None])[0]
        if requested:
            if os.path.isabs(requested) and os.path.isdir(requested):
                cwd = os.path.normpath(requested)
            else:
                # An invalid cwd (relative, missing, wrong OS, a file) falls
                # back to the default.
                warning = (
                    f"Working directory {requested!r} isn't usable on this server - "
                    "started in the default location instead."
                )
        return cls(
            cwd=cwd,
            cwd_warning=warning,
            idle_kill_ms=parse_hours_param(qs, "idleHours", defaults.idle_kill_ms),
            buffer_max_bytes=parse_mb_param(qs, "bufferMb", defaults.buffer_max_bytes),
        )


class TerminalHub:
    """Serves /terminal: one socket per client, attached to a session in
    `table` that outlives any socket. Keyed by ?session= (or an anon key if
    omitted)."""

    def __init__(
        self,
        defaults: TerminalDefaults,
        spawn_fn: SpawnFn = spawn_shell,
        required_token: str | None = None,
        max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES,
    ) -> None:
        self.table = SessionTable()
        self.anon_counter = 0
        self.defaults = defaults
        self.spawn_fn = spawn_fn
        # Handed to each shell as GRIDSHELL_AUTH_TOKEN.
        self.required_token = required_token
        # So a replay is split below the real wire limit, not dropped whole.
        self.max_message_bytes = max_message_bytes
        # Set by create_app(). Told when a shell is terminated, so its
        # pending /host calls fail fast.
        self.host_bridge: HostBridge | None = None
        self.reconcile_timeout_s = RECONCILE_TIMEOUT_S
        self.reconcile_grace_s = RECONCILE_GRACE_S

    def end_session(self, session: Session, *, reuse_id: bool) -> None:
        """The one way a session leaves the table. A session killed on request
        (reuse_id=False) blocks its id from respawning and fails its pending
        /host calls, an idle-killed or exited one stays reusable."""
        self.table.remove(session)
        session.kill()
        if not reuse_id and session.session_id:
            self.table.block(session.session_id)
            if self.host_bridge is not None:
                self.host_bridge.fail_pending(session.session_id, "Shell was closed.")

    def _on_shell_exit(self, session: Session, loop: asyncio.AbstractEventLoop) -> None:
        # On the reader thread. Skipped if a terminate already ended the session.
        if self.table.get(session.key) is not session:
            return
        ws = session.ws
        if ws is not None:
            try:
                asyncio.run_coroutine_threadsafe(ws.close(), loop)
            except RuntimeError:
                pass
        # An error exit can leave the process alive, kill() so it isn't orphaned.
        self.end_session(session, reuse_id=True)
        log(session.log_prefix, "shell exited")

    def _idle_kill(self, session: Session) -> None:
        if session.ws is None and self.table.get(session.key) is session:
            self.end_session(session, reuse_id=True)
            log(session.log_prefix, "session idle-killed")

    async def _ask_live_sessions(self, ws: ServerConnection) -> set[str] | None:
        # The client's list of its live shells, None if it doesn't answer in
        # time. Reads the socket directly: the message loop is waiting on us.
        await ws.send(wire("reconcile"))
        deadline = asyncio.get_running_loop().time() + self.reconcile_timeout_s
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return None
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                m = json.loads(raw)
            except TimeoutError:
                return None
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(m, dict) and m.get("type") == "liveSessions":
                ids = m.get("ids")
                if isinstance(ids, list) and all(isinstance(i, str) for i in ids):
                    return set(ids)
                return None

    def _reap_unlisted(self, doc_id: str, live_ids: set[str]) -> None:
        # The client lost these shells' entries without the server hearing.
        reaped = []
        for session in self.table.unlisted(doc_id, live_ids, self.reconcile_grace_s):
            old_ws = session.ws
            self.end_session(session, reuse_id=False)
            if old_ws is not None:
                asyncio.ensure_future(old_ws.close(code=4004, reason="session was closed"))
            reaped.append(session.key)
        if reaped:
            log(
                "[reconcile]",
                "doc",
                doc_id,
                "closed sessions missing from the client's list:",
                ", ".join(reaped),
            )

    def new_anon_key(self, loop: asyncio.AbstractEventLoop) -> str:
        self.anon_counter += 1
        return f"anon-{self.anon_counter}-{int(loop.time() * 1000)}"

    async def admit(self, ws: ServerConnection, auth: AuthFrame) -> str | None:
        """None if a new session fits under the document's cap, else the
        refusal reason. Over the cap, a client that lists its own shells is
        asked first: a session it no longer knows is a leftover, and freeing
        it may make room."""
        cap = self.table.effective_cap(auth.doc_id, auth.declared_cap)
        if (
            self.table.count_for_doc(auth.doc_id) >= cap
            and auth.doc_id
            and "reconcile" in auth.caps
        ):
            live_ids = await self._ask_live_sessions(ws)
            if live_ids is not None:
                self._reap_unlisted(auth.doc_id, live_ids)
        if self.table.count_for_doc(auth.doc_id) >= cap:
            return f"session limit reached ({cap}) for this document"
        return None

    def start_session(self, session: Session, loop: asyncio.AbstractEventLoop) -> None:
        self.table.add(session)
        threading.Thread(
            target=session.reader_loop,
            args=(loop, lambda s: self._on_shell_exit(s, loop)),
            daemon=True,
        ).start()

    def session_detached(
        self, session: Session, ws: ServerConnection, loop: asyncio.AbstractEventLoop
    ) -> None:
        """`ws` ended: start the idle-kill countdown, unless a newer socket
        already took the session over."""
        if session.ws is ws and self.table.get(session.key) is session:
            session.ws = None
            if session.idle_timer is not None:
                session.idle_timer.cancel()
            session.idle_timer = loop.call_later(
                session.idle_kill_ms / 1000, self._idle_kill, session
            )

    async def handle(self, ws: ServerConnection, auth: AuthFrame) -> None:
        await TerminalConnection(self, ws, auth).run()


# A control connection ("control" in the auth frame's caps) acts on a session
# without viewing it: no attach, no replay, nobody evicted by connecting. It
# can send only these, so it can never spawn one.
CONTROL_MESSAGES = frozenset({"terminate"})


class TerminalConnection:
    """One /terminal socket: reads its messages and routes each to a handler.
    The session it drives is found on reattach, or spawned by its first resize."""

    def __init__(self, hub: TerminalHub, ws: ServerConnection, auth: AuthFrame) -> None:
        self.hub = hub
        self.ws = ws
        self.auth = auth
        self.control = "control" in auth.caps
        self.loop = asyncio.get_running_loop()
        self.params = SpawnParams.from_query(ws.request.path, hub.defaults)
        self.key = auth.session or hub.new_anon_key(self.loop)
        self.log_prefix = f"[terminal:{self.key}]"
        self.session: Session | None = hub.table.get(self.key)
        # Why this connection ended, when this side knows. Logged on close.
        self.end_reason: str | None = None
        # Message type -> handler. A handler returns False to end the connection.
        self.handlers: dict[str, Callable[[dict[str, Any]], Awaitable[bool]]] = {
            "resize": self._on_resize,
            "input": self._on_input,
            "terminate": self._on_terminate,
        }

    async def run(self) -> None:
        if self.session is not None:
            if not await self._check_host_key():
                return
            if not self.control:
                await self.session.attach(self.ws, self.auth.caps, self.hub.max_message_bytes)
        try:
            async for raw in self.ws:
                try:
                    message = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                if isinstance(message, dict) and not await self._dispatch(message):
                    return
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            if self.session is not None:
                self.hub.session_detached(self.session, self.ws, self.loop)
            reason = self._why_closed()
            log(self.log_prefix, "connection closed" + (f" ({reason})" if reason else ""))

    async def _close(self, code: int, reason: str) -> None:
        self.end_reason = reason
        await self.ws.close(code=code, reason=reason)

    def _why_closed(self) -> str | None:
        if self.end_reason is not None:
            return self.end_reason
        session = self.session
        if session is None:
            return self._close_code()
        if session.superseded_ws is self.ws:
            return "evicted by a newer connection"
        if self.hub.table.get(session.key) is not session:
            return "session ended"
        return self._close_code()

    def _close_code(self) -> str | None:
        # The code the connection ended with, e.g. 1011 "keepalive ping timeout".
        code = self.ws.close_code
        if code is None:
            return None
        return f"code {code}" + (f" {self.ws.close_reason}" if self.ws.close_reason else "")

    async def _check_host_key(self) -> bool:
        # A session id alone isn't proof of ownership: reattach (and a control
        # connection) needs the same host key /host does. A fresh spawn isn't checked.
        expected_key = self.session.host_key
        if not expected_key or not keys_match(self.auth.host_key, expected_key):
            log(self.log_prefix, "rejected: invalid or missing host key")
            await self._close(1008, "invalid or missing host key")
            return False
        return True

    async def _dispatch(self, message: dict[str, Any]) -> bool:
        """Routes one message. False ends the connection (the socket was closed).
        Unknown types, and any a control connection may not send, are ignored."""
        kind = message.get("type")
        handler = self.handlers.get(kind) if isinstance(kind, str) else None
        if handler is None or (self.control and kind not in CONTROL_MESSAGES):
            return True
        return await handler(message)

    async def _on_resize(self, message: dict[str, Any]) -> bool:
        try:
            cols = max(1, int(message.get("cols", 80)))
            rows = max(1, int(message.get("rows", 24)))
        except (TypeError, ValueError):
            # Not only the stock client reaches /terminal: ignore a malformed
            # size rather than end the loop.
            return True
        if self.session is None:
            return await self._spawn(cols, rows)
        self.session.resize(cols, rows)
        await self.ws.send(wire("resized", cols=cols, rows=rows))
        return True

    async def _spawn(self, cols: int, rows: int) -> bool:
        hub, auth, params = self.hub, self.auth, self.params
        if auth.session and hub.table.is_blocked(auth.session):
            await self._close(4004, "session was closed")
            return False
        refusal = await hub.admit(self.ws, auth)
        if refusal is not None:
            # 1013, not 1008, so the client skips the token-rejection hint.
            await self._close(1013, refusal)
            return False
        session = Session(
            self.key,
            auth.session,
            self.ws,
            params.idle_kill_ms,
            params.buffer_max_bytes,
            auth.host_key,
            auth.doc_id,
            cols,
            rows,
        )
        session.spawn(hub.spawn_fn, hub.required_token, params.cwd)
        self.session = session
        hub.start_session(session, self.loop)
        log(self.log_prefix, "shell started, cwd:", params.cwd or os.getcwd(), f"at {cols}x{rows}")
        if "hello" in auth.caps:
            await self.ws.send(hello_message())
        await self.ws.send(wire("spawned"))
        if params.cwd_warning:
            await self.ws.send(wire("warning", message=params.cwd_warning))
        # Synthesize an initial title, ConPTY doesn't forward one on its own.
        title_data = f"\x1b]0;{shell_display_path()}\x07"
        session.replay.ingest(title_data)
        await session.send_output(title_data)
        return True

    async def _on_input(self, message: dict[str, Any]) -> bool:
        session = self.session
        if session is None:
            return True
        data = message.get("data", "")
        # xterm.js answers the shell's startup DA1 query as input, and under
        # load PSReadLine can type it literally. Same filter as ReplayBuffer.
        if not session.replay.handshake_done and is_pure_negotiation(data):
            return True
        session.proc.write(data)
        return True

    async def _on_terminate(self, message: dict[str, Any]) -> bool:
        if self.session is not None:
            # Whoever is viewing the session is told it is gone. A connection
            # that attached has already evicted the previous viewer: no-op.
            await self.session.evict(keep=self.ws)
            self.hub.end_session(self.session, reuse_id=False)
            self.end_reason = "terminated on request"
            log(self.log_prefix, "session terminated on request")
        elif self.auth.session:
            # Still opening: remember the id so its spawn is refused.
            self.hub.table.block(self.auth.session)
            self.end_reason = "terminated before spawn"
            log(self.log_prefix, "terminate before spawn, session id blocked")
        else:
            return True
        try:
            await self.ws.send(wire("terminated"))
        except Exception:
            pass
        return True
