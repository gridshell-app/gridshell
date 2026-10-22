"""Builds the server: routes each connection to the hub or the bridge, and
starts it from Settings."""

import asyncio
import atexit
import json
import os
import signal
import ssl
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import websockets
from websockets.asyncio.server import ServerConnection, serve

from .bridge import HostBridge
from .common import DEFAULT_MAX_MESSAGE_BYTES, keys_match, log
from .terminal.hub import AuthFrame, TerminalDefaults, TerminalHub
from .terminal.shell import SpawnFn, spawn_shell

_LOOPBACK_HOSTS = ("localhost", "127.0.0.1", "::1", "[::1]")


# Apps Script serves the Sidebar/Dialog from a googleusercontent.com subdomain.
_ALLOWED_ORIGIN_SUFFIXES = (".googleusercontent.com",)


def _is_allowed_origin(origin: str | None, extra_suffixes: tuple[str, ...]) -> bool:
    # No Origin header means a non-browser client (curl, gridshell-mcp).
    if origin is None:
        return True
    host = urlparse(origin).hostname or ""
    all_suffixes = _ALLOWED_ORIGIN_SUFFIXES + extra_suffixes
    # Also the bare host: "example.com" allows "example.com" itself, not
    # only its subdomains.
    return any(host == suffix.lstrip(".") or host.endswith(suffix) for suffix in all_suffixes)


def create_app(
    idle_hours: float,
    buffer_mb: float,
    required_token: str | None = None,
    allowed_origin_suffixes: tuple[str, ...] = (),
    max_message_bytes: int | None = None,
    spawn: SpawnFn = spawn_shell,
) -> tuple[Callable[[ServerConnection], Awaitable[None]], TerminalHub, HostBridge]:
    # Apart from main() so tests can build a router without the CLI bootstrap.
    terminal_hub = TerminalHub(
        TerminalDefaults(
            idle_kill_ms=idle_hours * 60 * 60 * 1000,
            buffer_max_bytes=buffer_mb * 1024 * 1024,
        ),
        spawn_fn=spawn,
        required_token=required_token,
        max_message_bytes=max_message_bytes or DEFAULT_MAX_MESSAGE_BYTES,
    )
    host_bridge = HostBridge(terminal_hub.table)
    terminal_hub.host_bridge = host_bridge

    async def router(ws: ServerConnection) -> None:
        # Secondary layer only, see the gridshell.server docstring.
        origin = ws.request.headers.get("Origin")
        if not _is_allowed_origin(origin, allowed_origin_suffixes):
            log("[auth] rejected connection from disallowed origin:", origin)
            await ws.close(code=1008, reason="origin not allowed")
            return
        # Credentials are the first message, not the URL, see the gridshell.server docstring.
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
            auth = AuthFrame.parse(raw)
        except (
            TimeoutError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
            websockets.exceptions.ConnectionClosed,
        ):
            log(
                "[auth] rejected connection from",
                ws.remote_address,
                "path",
                urlparse(ws.request.path).path,
                "- missing or malformed auth frame",
            )
            try:
                await ws.close(code=1008, reason="missing or malformed auth frame")
            except Exception:
                pass
            return
        token = auth.token
        session_id = auth.session
        session_key = auth.session_key
        host_key = auth.host_key
        # The session id isn't a credential, the token is what gates access.
        if required_token is not None:
            if not keys_match(token, required_token):
                # Logged with the peer address, never the attempted token.
                log(
                    "[auth] rejected connection from",
                    ws.remote_address,
                    "path",
                    urlparse(ws.request.path).path,
                )
                await ws.close(code=1008, reason="invalid or missing token")
                return
        path = urlparse(ws.request.path).path
        if path == "/terminal":
            await terminal_hub.handle(ws, auth)
        elif path == "/host":
            await host_bridge.handle(ws, session_id, host_key)
        elif path == "/mcp":
            await host_bridge.handle_mcp(ws, session_id, session_key)
        else:
            await ws.close(code=1008, reason=f"unknown path: {path}")

    return router, terminal_hub, host_bridge


def _kill_all_sessions(terminal_hub: TerminalHub) -> None:
    # Through end_session, so the shell's exit finds the session gone and
    # doesn't kill it a second time.
    for session in list(terminal_hub.table.sessions.values()):
        terminal_hub.end_session(session, reuse_id=True)


def install_shutdown_hook(
    terminal_hub: TerminalHub,
    loop: asyncio.AbstractEventLoop | None = None,
    stop: asyncio.Future[None] | None = None,
) -> None:
    # Else a restart orphans every PTY. atexit covers Ctrl+C, SIGTERM needs
    # a handler. A hard kill isn't covered.
    atexit.register(_kill_all_sessions, terminal_hub)

    def _handle_signal(signum: int, frame: Any) -> None:
        _kill_all_sessions(terminal_hub)
        # Ending main() through the loop lets serve() close its connections.
        # A second signal exits at once.
        if loop is not None and stop is not None and not stop.done():
            loop.call_soon_threadsafe(lambda: stop.done() or stop.set_result(None))
            return
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


@dataclass(frozen=True)
class Settings:
    """What the CLI and environment resolve to, handed to main()."""

    port: int
    idle_hours: float
    buffer_mb: float
    cert_path: str | None
    key_path: str | None
    use_wss: bool
    auth_token: str | None
    host: str
    allow_origins: tuple[str, ...]
    max_message_mb: float | None
    allow_root: bool


def _running_as_root_or_admin() -> bool:
    if sys.platform == "win32":
        try:
            import ctypes

            return bool(ctypes.windll.shell32.IsUserAnAdmin())
        except Exception:
            return False
    return hasattr(os, "geteuid") and os.geteuid() == 0


async def main(settings: Settings) -> None:
    if _running_as_root_or_admin() and not settings.allow_root:
        sys.exit(
            "Refusing to start as root/Administrator - /terminal spawns a real "
            "interactive shell, so running this process elevated hands that shell "
            "the same elevated privileges. Pass --allow-root if you really mean it."
        )
    if settings.use_wss and not settings.auth_token:
        sys.exit(
            "--wss with --no-auth-token has no other safeguard at all - refusing to "
            "start /terminal (a real interactive shell), /host, and /mcp reachable "
            "beyond localhost with no authentication at all."
        )
    if settings.use_wss and not (settings.cert_path and settings.key_path):
        sys.exit("--wss requires --cert-path and --key-path pointing at a real certificate.")
    # Same exposure as the --wss guard above, minus TLS - refused for
    # consistency. Only triggers with an explicit --no-auth-token.
    if settings.host not in _LOOPBACK_HOSTS and not settings.auth_token:
        sys.exit(
            f"--host {settings.host} is not loopback-only and --no-auth-token was passed - "
            "refusing to start /terminal (a real interactive shell), /host, "
            "and /mcp reachable beyond localhost with no authentication at all."
        )
    max_message_bytes = (
        int(settings.max_message_mb * 1024 * 1024)
        if settings.max_message_mb is not None
        else DEFAULT_MAX_MESSAGE_BYTES
    )
    # required_token flows in independent of --wss, see the gridshell.server docstring.
    router, terminal_hub, _host_bridge = create_app(
        settings.idle_hours,
        settings.buffer_mb,
        required_token=settings.auth_token,
        allowed_origin_suffixes=tuple(settings.allow_origins),
        max_message_bytes=max_message_bytes,
    )
    loop = asyncio.get_running_loop()
    stop: asyncio.Future[None] = loop.create_future()
    install_shutdown_hook(terminal_hub, loop, stop)

    ssl_context = None
    if settings.use_wss:
        # Purpose.CLIENT_AUTH pins the same guarantees (TLS 1.2 floor, no
        # compression) regardless of the OpenSSL build the operator happens
        # to have, which matters for a bare SSLContext(PROTOCOL_TLS_SERVER)
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        try:
            ssl_context.load_cert_chain(settings.cert_path, settings.key_path)
        except (OSError, ssl.SSLError) as exc:
            sys.exit(
                "Could not load the TLS certificate/key pair "
                f"(--cert-path {settings.cert_path}, --key-path {settings.key_path}): {exc}"
            )

    async with serve(
        router, settings.host, settings.port, ssl=ssl_context, max_size=max_message_bytes
    ):
        scheme = "wss" if settings.use_wss else "ws"
        print(f"Terminal server (Python) running at {scheme}://{settings.host}:{settings.port}")
        if settings.auth_token:
            # Token never printed, see cli.resolve_flags()'s clipboard-or-refuse handling.
            print(
                f"  Sheets sidebar Server address: {settings.host}:{settings.port}  (paste the "
                "auth token into the Auth token field - see above for how to retrieve it, "
                "or run `gridshell-server --copy-token`)"
            )
        print("  /terminal  xterm.js I/O")
        print("  /host      Host command bridge")
        print("  /mcp       MCP server relay")
        await stop
