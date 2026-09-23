"""Host-agnostic transport speaking GridShell terminal-server's /mcp envelope
protocol: {id, tool, params, timeoutMs} requests, {id, result} / {id, error}
responses over a single WebSocket, see schemas/request-envelope.schema.json
and response-envelope.schema.json in this repo for the wire contract
this mirrors.

This is the piece every host-specific client is built on. It knows
nothing about what any tool name or its params mean - that's the
host-specific layer's job. Synchronous: one call in flight at a time,
blocking.
"""

import json
import os
import random
import string
from typing import Any, Optional

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect as ws_connect

from .exceptions import GridShellConnectionError, GridShellError, GridShellTimeoutError

# Bootstrap fallback, until/unless a host-specific client narrows it down
# further (see sheets.py's batch-time-budget fetch).
DEFAULT_TIMEOUT = 300

# Extra time on top of the server's own timeoutMs, so recv() doesn't race
# the server's own timeout message.
CLIENT_MARGIN = 20

# Matches server.py's DEFAULT_MAX_MESSAGE_BYTES, but a server started with
# a raised --max-message-mb needs this raised too, or an oversized response
# is rejected client-side regardless. Same $MAX_MESSAGE_MB env var as the
# server, so one setting covers both sides.
MAX_WS_MESSAGE_BYTES = int(float(os.environ.get("MAX_MESSAGE_MB", 64)) * 1024 * 1024)


def _generate_id() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=16))


def _describe_close(exc: ConnectionClosed) -> str:
    # A rejected/missing token closes with code 1008 and a specific reason
    # (server.py's router()) - surface it instead of a generic message that
    # sends the user looking at their socket layer instead of their token.
    rcvd = getattr(exc, "rcvd", None)
    if rcvd is not None and rcvd.reason:
        return f"Rejected by the server: {rcvd.reason}"
    return "Connection to terminal server was closed"


class Connection:
    """Owns one WebSocket connection to a terminal-server's /mcp endpoint."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3000,
        wss: bool = False,
        session: Optional[str] = None,
        session_key: Optional[str] = None,
        token: Optional[str] = None,
        default_timeout: float = DEFAULT_TIMEOUT,
    ):
        self.host = host
        self.port = port
        self.wss = wss
        # Auto-targets the document a script was launched from. An explicit
        # session (including "") always wins over the env var.
        self.session = session if session is not None else os.environ.get("GRIDSHELL_SESSION", "")
        # Proves this process owns `session`, not just names it. See
        # server.py's spawn_process()/handle_mcp().
        self.session_key = session_key if session_key is not None else os.environ.get("GRIDSHELL_SESSION_KEY", "")
        # Auto-pickup for a client launched inside a GridShell shell.
        self.token = token if token is not None else os.environ.get("GRIDSHELL_AUTH_TOKEN", "")
        self.default_timeout = default_timeout
        self._ws = None

    @property
    def url(self) -> str:
        # No query string - credentials travel as the first message instead
        # (see connect() below).
        scheme = "wss" if self.wss else "ws"
        return f"{scheme}://{self.host}:{self.port}/mcp"

    @property
    def _connect_auth(self) -> dict:
        return {"token": self.token, "session": self.session, "sessionKey": self.session_key}

    @property
    def connected(self) -> bool:
        return self._ws is not None

    def connect(self) -> None:
        if self._ws is not None:
            return
        try:
            # legacy=True: hold the ClientConnection directly and manage its
            # own lifetime, rather than connect() acting as a context
            # manager / auto-reconnecting iterator.
            self._ws = ws_connect(self.url, legacy=True, max_size=MAX_WS_MESSAGE_BYTES)
        except Exception as exc:
            raise GridShellConnectionError(f"Could not connect to {self.url}: {exc}") from exc
        # Credentials go first, before anything else, see server.py's router().
        self._ws.send(json.dumps(self._connect_auth))

    def close(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            finally:
                self._ws = None

    def __enter__(self) -> "Connection":
        self.connect()
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def call(self, tool: str, params: Optional[dict] = None, timeout: Optional[float] = None) -> Any:
        """Send one request envelope and block for its matching response.

        Raises GridShellConnectionError if not connected and connecting
        fails, GridShellTimeoutError if no matching response arrives in
        time, or GridShellError if the server's response was {id, error}.
        """
        if self._ws is None:
            self.connect()

        call_timeout = self.default_timeout if timeout is None else timeout
        request_id = _generate_id()
        envelope = {
            "id": request_id,
            "tool": tool,
            "params": params or {},
            "timeoutMs": int(call_timeout * 1000),
        }

        try:
            self._ws.send(json.dumps(envelope))
        except ConnectionClosed as exc:
            self._ws = None
            raise GridShellConnectionError(_describe_close(exc)) from exc

        recv_timeout = call_timeout + CLIENT_MARGIN
        while True:
            try:
                raw = self._ws.recv(timeout=recv_timeout)
            except TimeoutError as exc:
                raise GridShellTimeoutError(
                    f"Timed out waiting for a response to '{tool}' after {recv_timeout}s"
                ) from exc
            except ConnectionClosed as exc:
                self._ws = None
                raise GridShellConnectionError(_describe_close(exc)) from exc

            try:
                message = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue

            # Ignore anything not addressed to this request.
            if not isinstance(message, dict) or message.get("id") != request_id:
                continue

            if "error" in message:
                raise GridShellError(message["error"])
            return message.get("result")
