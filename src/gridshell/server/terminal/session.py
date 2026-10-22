"""One shell with its scrollback (replay buffer) and live-output delivery, and
the table of live sessions."""

import asyncio
import json
import os
import re
import secrets
import sys
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from websockets.asyncio.server import ServerConnection

from ..common import hello_message, log, wire
from .shell import PtyLike, SpawnFn

# Caps live-output chunks queued for a slow client (4096 bytes each, so
# ~1MB per session). Independent of the replay buffer's own cap.
OUTPUT_QUEUE_MAX_CHUNKS = 256


# v1 free-tier cap, mirrors the client's limit; deliberately not a CLI flag.
# Per doc_id; the fallback when a doc declared no cap of its own.
MAX_CONCURRENT_SESSIONS = 2


# Matches CSI/OSC escape sequences, used to detect a chunk that's pure
# terminal negotiation (device-attribute queries, title-setting), no real
# content. See is_pure_negotiation below.
_ANSI_SEQ_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b\][^\x07]*\x07|\x1b[()][A-Za-z0-9]")


def is_pure_negotiation(chunk: str) -> bool:
    return _ANSI_SEQ_RE.sub("", chunk) == ""


# The specific DA1/DA2/cursor-position query sequences xterm.js
# auto-answers - narrower than _ANSI_SEQ_RE above.
_AUTO_ANSWERED_QUERY_RE = re.compile(r"\x1b\[>?0?c|\x1b\[6n")


def _split_for_max_message(text: str, max_bytes: int) -> list[str]:
    # Split a replay bigger than --max-message-mb into several sends, an
    # oversized one is silently dropped. Budget assumes JSON escaping can
    # expand 1 character up to 12 bytes (a surrogate pair as \uXXXX\uXXXX).
    envelope_overhead = 32  # {"type":"output","data":"..."}
    budget = max(1, (max_bytes - envelope_overhead) // 12)
    return [text[i : i + budget] for i in range(0, len(text), budget)]


class ReplayBuffer:
    """Scrollback kept for reattach: output chunks interleaved with (cols, rows)
    markers where the terminal size changed, trimmed to a byte budget. The
    reader thread writes and the event loop reads, `_lock` guards both."""

    def __init__(self, max_bytes: float, cols: int, rows: int) -> None:
        self.max_bytes = max_bytes
        self.items: list[str | tuple[int, int]] = []
        self._bytes = 0
        self._lock = threading.Lock()
        # Set once the first non-negotiation output is seen, see ingest().
        self.handshake_done = False
        # Terminal size now, and the size the oldest kept data was written at.
        self.cur_size = (cols, rows)
        self.base_size = (cols, rows)

    def ingest(self, data: str) -> None:
        # Strip startup negotiation from the buffer: replayed, xterm.js
        # re-answers it and PSReadLine can misparse that as a keystroke.
        # Pre-handshake only, or legitimate TUI redraw chunks would go too.
        if not self.handshake_done:
            if is_pure_negotiation(data):
                return
            self.handshake_done = True
        else:
            # A TUI's own later DA1 probe would otherwise sit in the buffer and
            # get re-answered on every reattach. Strips just the query bytes.
            data = _AUTO_ANSWERED_QUERY_RE.sub("", data)
            if not data:
                return
        with self._lock:
            self.items.append(data)
            self._bytes += len(data.encode("utf-8"))
            while self._bytes > self.max_bytes and len(self.items) > 1:
                dropped = self.items.pop(0)
                if isinstance(dropped, tuple):
                    # A size marker: the kept data now starts at this size.
                    self.base_size = dropped
                else:
                    self._bytes -= len(dropped.encode("utf-8"))

    def record_size(self, cols: int, rows: int) -> None:
        # A marker between the output chunks lets a reattach replay each
        # stretch at the width it was written at.
        with self._lock:
            if self.cur_size == (cols, rows):
                return
            self.cur_size = (cols, rows)
            self.items.append((cols, rows))

    def messages(self, with_sizes: bool, max_bytes: int) -> list[dict[str, Any]]:
        """Scrollback as wire messages: with `resized` markers for "timeline"
        clients, otherwise the joined output only."""
        with self._lock:
            items = list(self.items)
            base = self.base_size
        if not any(isinstance(i, str) for i in items):
            return []
        messages: list[dict[str, Any]] = []
        if with_sizes:
            messages.append({"type": "resized", "cols": base[0], "rows": base[1]})
        run: list[str] = []

        def flush() -> None:
            if run:
                for piece in _split_for_max_message("".join(run), max_bytes):
                    messages.append({"type": "output", "data": piece})
                del run[:]

        for item in items:
            if isinstance(item, tuple):
                if with_sizes:
                    flush()
                    messages.append({"type": "resized", "cols": item[0], "rows": item[1]})
            else:
                run.append(item)
        flush()
        return messages


class Session:
    """One shell: its PTY, replay buffer and live-output delivery. Outlives
    any single socket, a reconnecting client reattaches to it."""

    def __init__(
        self,
        key: str,
        session_id: str,
        ws: ServerConnection | None,
        idle_kill_ms: float,
        buffer_max_bytes: float,
        host_key: str,
        doc_id: str,
        cols: int,
        rows: int,
    ) -> None:
        # Table key: the session id, or a generated one for an anonymous session.
        self.key = key
        self.session_id = session_id
        self.proc: PtyLike | None = None
        self.ws = ws
        self.replay = ReplayBuffer(buffer_max_bytes, cols, rows)
        self.idle_timer: asyncio.TimerHandle | None = None
        self.idle_kill_ms = idle_kill_ms
        # The socket the latest attach() replaced, so that connection can say
        # it was evicted (it may finish closing before self.ws is reassigned).
        self.superseded_ws: ServerConnection | None = None
        self.session_key: str | None = None
        # Minted by the client, never this server. /host requires it too, so
        # naming a session id isn't enough.
        self.host_key = host_key
        # Serializes ws sends, which keeps a replay atomic with live output.
        self.send_lock = asyncio.Lock()
        # Live output handed from the reader thread to the event loop.
        self.send_queue: deque[str] = deque(maxlen=OUTPUT_QUEUE_MAX_CHUNKS)
        self.send_queue_lock = threading.Lock()
        self.send_pending = False
        # Groups this session for MAX_CONCURRENT_SESSIONS.
        self.doc_id = doc_id
        self.cols = cols
        self.rows = rows
        # Exempts a fresh session from reconcile, see _reap_unlisted.
        self.created_at = time.monotonic()

    @property
    def log_prefix(self) -> str:
        return f"[terminal:{self.key}]"

    def kill(self) -> None:
        """Cancel the idle timer and force the shell down. Safe to repeat."""
        if self.idle_timer is not None:
            self.idle_timer.cancel()
            self.idle_timer = None
        try:
            self.proc.terminate(force=True)
        except Exception as exc:
            log(self.log_prefix, "terminate() failed:", exc)

    def spawn(self, spawn_fn: SpawnFn, required_token: str | None, spawn_cwd: str | None) -> None:
        env = dict(os.environ)
        if self.session_id:
            self.session_key = secrets.token_urlsafe(32)
            env["GRIDSHELL_SESSION"] = self.session_id
            # Per-spawn secret: a session id alone isn't a credential (another
            # shell's environ can read it), /mcp requires this too.
            env["GRIDSHELL_SESSION_KEY"] = self.session_key
        if required_token:
            # Lets a client launched inside this shell pick the token up.
            env["GRIDSHELL_AUTH_TOKEN"] = required_token
        # Match xterm.js: a missing or minimal ambient TERM breaks color and
        # alt-screen restore in nano and vim.
        if sys.platform != "win32":
            env["TERM"] = "xterm-256color"
        self.proc = spawn_fn(cwd=spawn_cwd, env=env, dimensions=(self.rows, self.cols))

    async def send_output(self, data: str) -> None:
        async with self.send_lock:
            target_ws = self.ws
            if target_ws is not None:
                try:
                    await target_ws.send(wire("output", data=data))
                except Exception:
                    pass

    async def drain_send_queue(self) -> None:
        # One at a time per session (send_pending), in order, so a slow
        # client doesn't pile up a coroutine per chunk.
        while True:
            with self.send_queue_lock:
                if not self.send_queue:
                    self.send_pending = False
                    return
                data = self.send_queue.popleft()
            await self.send_output(data)

    def reader_loop(
        self, loop: asyncio.AbstractEventLoop, on_exit: Callable[["Session"], None]
    ) -> None:
        """Runs on its own thread: pumps shell output into the replay buffer
        and the live send queue until the shell ends, then calls on_exit."""
        while True:
            try:
                data = self.proc.read(4096)
            except EOFError:
                break
            except Exception as exc:
                log(self.log_prefix, "reader error:", exc)
                break
            if not data:
                continue
            self.replay.ingest(data)
            with self.send_queue_lock:
                # Bounded: past capacity the oldest unsent live output drops.
                self.send_queue.append(data)
                already_pending = self.send_pending
                self.send_pending = True
            if already_pending:
                continue
            try:
                asyncio.run_coroutine_threadsafe(self.drain_send_queue(), loop)
            except RuntimeError:
                break
        on_exit(self)

    async def evict(self, keep: ServerConnection | None = None) -> None:
        """Tell the connected viewer, unless it is `keep`, that it was replaced,
        and close its socket."""
        old_ws = self.ws
        if old_ws is None or old_ws is keep:
            return
        self.superseded_ws = old_ws
        try:
            await old_ws.send(wire("evicted"))
            await old_ws.close()
        except Exception:
            pass

    async def attach(
        self, ws: ServerConnection, caps: frozenset[str], max_message_bytes: int
    ) -> None:
        await self.evict(keep=ws)
        # After the eviction: the old viewer's socket ending starts a countdown.
        if self.idle_timer is not None:
            self.idle_timer.cancel()
            self.idle_timer = None
        # Under send_lock the replay can't interleave with live output.
        replay = self.replay.messages("timeline" in caps, max_message_bytes)
        async with self.send_lock:
            self.ws = ws
            try:
                if "hello" in caps:
                    await ws.send(hello_message())
                await ws.send(wire("attached"))
                for message in replay:
                    await ws.send(json.dumps(message))
            except Exception as exc:
                # The only signal: the client never learns the replay was cut short.
                log(self.log_prefix, "failed to send attach/replay:", exc)
        log(self.log_prefix, "attached to session:", self.key)

    def resize(self, cols: int, rows: int) -> None:
        self.cols = cols
        self.rows = rows
        self.replay.record_size(cols, rows)
        self.proc.setwinsize(rows, cols)


class SessionTable:
    """The live sessions by key, plus the bookkeeping that outlives them:
    ids killed on request and each document's session cap."""

    def __init__(self) -> None:
        self.sessions: dict[str, Session] = {}
        # Ids killed on request. Never reused, so a later spawn under one is a
        # stale dialog racing the kill.
        self.terminated: set[str] = set()
        # doc_id -> the cap latched from that doc's first /terminal connection.
        # Resets on server restart, same as `sessions`.
        self._doc_caps: dict[str, int] = {}

    def get(self, key: str) -> Session | None:
        return self.sessions.get(key)

    def add(self, session: Session) -> None:
        self.sessions[session.key] = session

    def remove(self, session: Session) -> None:
        # Only if it's still the current one under its key (not already
        # replaced by a respawn or removed by "terminate").
        if self.sessions.get(session.key) is session:
            del self.sessions[session.key]

    def block(self, session_id: str) -> None:
        self.terminated.add(session_id)

    def is_blocked(self, session_id: str) -> bool:
        return session_id in self.terminated

    def count_for_doc(self, doc_id: str) -> int:
        return sum(1 for s in self.sessions.values() if s.doc_id == doc_id)

    def effective_cap(self, doc_id: str, declared_cap: Any) -> int:
        # Latched from the first /terminal connection for this doc_id, for
        # the server run. A missing or malformed value falls back to the default.
        if doc_id not in self._doc_caps:
            if (
                isinstance(declared_cap, int)
                and not isinstance(declared_cap, bool)
                and declared_cap > 0
            ):
                self._doc_caps[doc_id] = declared_cap
            else:
                self._doc_caps[doc_id] = MAX_CONCURRENT_SESSIONS
        return self._doc_caps[doc_id]

    def unlisted(self, doc_id: str, live_ids: set[str], grace_s: float) -> list[Session]:
        """This document's sessions the client no longer lists. Younger than
        the grace period are kept, their entry may postdate the list."""
        now = time.monotonic()
        return [
            s
            for key, s in self.sessions.items()
            if s.doc_id == doc_id and key not in live_ids and now - s.created_at >= grace_s
        ]
