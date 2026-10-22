"""What every server module shares: the log line, the wire-message helper,
constant-time key comparison and the protocol constants."""

import hmac
import json
import sys
from datetime import datetime
from importlib.metadata import PackageNotFoundError, version
from typing import Any

# Raised well above the websockets default (1 MiB), a normal Sheets range
# easily exceeds that.
DEFAULT_MAX_MESSAGE_BYTES = 64 * 1024 * 1024


# Sent in `hello`; raise only for releases worth nudging users to update for.
PROTOCOL_LEVEL = 1


def log(*args: object) -> None:
    # Timestamped - local time + offset, not UTC, for whoever's running the server.
    print(datetime.now().astimezone().isoformat(timespec="seconds"), *args)


def session_suffix(session_id: str) -> str:
    return f" (session {session_id})" if session_id else ""


def wire(type_: str, **fields: Any) -> str:
    """One server -> client message as its JSON text, `type` first."""
    return json.dumps({"type": type_, **fields})


def needs_conpty_corrections() -> bool:
    """True when the ConPTY host bundled with pywinpty is older than the one in
    pywinpty 3.1, which re-syncs the cursor after a resize by itself."""
    try:
        major, minor = (int(p) for p in version("pywinpty").split(".")[:2])
    except (PackageNotFoundError, ValueError):
        return True
    return (major, minor) < (3, 1)


def hello_message() -> str:
    """The `hello` message: the protocol level and, for an older ConPTY host, the
    PTY kind and OS build, so the client can match xterm.js to its resize behavior."""
    fields: dict[str, Any] = {"protocol": PROTOCOL_LEVEL}
    if sys.platform == "win32" and needs_conpty_corrections():
        fields.update(pty="conpty", build=sys.getwindowsversion().build)
    return wire("hello", **fields)


def keys_match(provided: str, expected: str) -> bool:
    # Constant-time, over bytes: compare_digest(str, str) raises on non-ASCII
    # input instead of returning False.
    return hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))
