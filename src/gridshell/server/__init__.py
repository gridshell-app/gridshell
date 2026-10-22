"""GridShell terminal server - the /terminal, /host, and /mcp WebSocket
endpoints a self-hosted GridShell backend exposes. Installed automatically
with `pip install gridshell`, alongside the SheetsClient Python client and
mcp_grid.py. Run via the `gridshell-server` console command, or
`python -m gridshell.server`.

  /terminal  xterm.js <-> PTY (session reattach, idle-kill, output buffer)
  /host      Host command bridge (Sheets sidebar/dialog connects here)
  /mcp       MCP server relay (mcp_grid.py connects here)

PTY backend: pywinpty (ConPTY) on Windows, ptyprocess on Linux/Mac.

Dependencies are installed automatically with `pip install gridshell`
(websockets, plus pywinpty on Windows or ptyprocess on Linux/Mac).

Run:  gridshell-server --help  (the full flag list is USAGE in cli.py)
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

from .app import create_app
from .cli import run

__all__ = ["create_app", "run"]
