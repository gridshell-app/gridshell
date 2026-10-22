"""The command line: flags and environment resolved into Settings, and the
persisted auth token with its delivery through the clipboard."""

import asyncio
import os
import secrets
import shutil
import subprocess
import sys

from .app import Settings, main
from .common import log


def default_token_path() -> str:
    override = os.environ.get("GRIDSHELL_TOKEN_PATH")
    if override:
        return override
    return os.path.expanduser(os.path.join("~", ".gridshell", "token"))


def persisted_token_exists() -> bool:
    try:
        with open(default_token_path(), encoding="utf-8") as f:
            return bool(f.read().strip())
    except OSError:
        return False


def try_clipboard_copy(text: str) -> bool:
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
        for cmd in (
            ["wl-copy"],
            ["xclip", "-selection", "clipboard"],
            ["xsel", "--clipboard", "--input"],
        ):
            if shutil.which(cmd[0]):
                subprocess.run(cmd, input=text.encode("utf-8"), check=True, timeout=5)
                return True
        return False
    except Exception:
        return False


def load_or_create_default_token(
    explicit_token: str | None, no_auth_token: bool, regenerate: bool
) -> str | None:
    # Persisted across restarts, the client's settings are configured once.
    if no_auth_token:
        return None
    if explicit_token:
        return explicit_token
    path = default_token_path()
    if not regenerate:
        try:
            with open(path, encoding="utf-8") as f:
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
        log(
            f"[auth] could not persist a generated token to {path} ({exc}) - using an "
            "in-memory token for this run only; it will change on restart."
        )
    return token


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


def _parse_number(
    raw: str, flag: str, kind: type[int] | type[float], minimum: float | None = None
) -> int | float:
    try:
        value = kind(raw)
    except ValueError:
        sys.exit(f"{flag} must be a number, got {raw!r}")
    if minimum is not None and value < minimum:
        sys.exit(f"{flag} must be >= {minimum}, got {value}")
    return value


def resolve_flags() -> Settings:
    argv = sys.argv[1:]
    use_wss = False
    no_auth_token = False
    regenerate_token = False
    copy_token = False
    allow_root = False
    # Hand-rolled: argparse would be overkill for this handful of flags.
    value_flags = {
        "--host": "host",
        "--port": "port",
        "--idle-hours": "idle_hours",
        "--buffer-mb": "buffer_mb",
        "--cert-path": "cert_path",
        "--key-path": "key_path",
        "--auth-token": "auth_token",
        "--allow-origin": "allow_origin",
        "--max-message-mb": "max_message_mb",
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
                sys.exit(f"{arg} requires a value")
            values[value_flags[arg]] = argv[i + 1]
            i += 1
        else:
            sys.exit(f"Unknown option: {arg}")
        i += 1
    host = values.get("host")
    port = _parse_number(values["port"], "--port", int, minimum=1) if "port" in values else None
    # 0 is a documented, meaningful value (explicit zero idle grace),
    # only negative values are nonsensical.
    idle_hours = (
        _parse_number(values["idle_hours"], "--idle-hours", float, minimum=0)
        if "idle_hours" in values
        else None
    )
    buffer_mb = (
        _parse_number(values["buffer_mb"], "--buffer-mb", float, minimum=0.01)
        if "buffer_mb" in values
        else None
    )
    cert_path = values.get("cert_path")
    key_path = values.get("key_path")
    auth_token = values.get("auth_token")
    allow_origin_raw = values.get("allow_origin")
    max_message_mb = (
        _parse_number(values["max_message_mb"], "--max-message-mb", float, minimum=0.01)
        if "max_message_mb" in values
        else None
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
    allow_origins = (
        [
            s if s.startswith(".") else "." + s
            for s in (p.strip() for p in allow_origin_raw.split(","))
            if s
        ]
        if allow_origin_raw
        else []
    )
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
        token = load_or_create_default_token(auth_token, no_auth_token, regenerate_token)
        if not token:
            sys.exit("No auth token is configured (--no-auth-token) - nothing to copy.")
        if try_clipboard_copy(token):
            print("Auth token copied to clipboard.")
            sys.exit(0)
        sys.exit(
            "No clipboard is available on this system - gridshell-server never "
            f"prints the token itself. Read it directly from {default_token_path()} "
            "(or from wherever --auth-token/AUTH_TOKEN sources it, if set explicitly)."
        )
    # Delivered through the clipboard only, never stdout (see the gridshell.server docstring).
    # No clipboard is fine, the token is already in the file; a persist failure aborts.
    token_is_explicit = bool(auth_token)
    auth_token = load_or_create_default_token(auth_token, no_auth_token, regenerate_token)
    if token_is_explicit:
        print("Using the auth token provided via --auth-token/AUTH_TOKEN.")
    elif no_auth_token:
        pass
    elif auth_token:
        if try_clipboard_copy(auth_token):
            print("Auth token copied to clipboard.")
        elif persisted_token_exists():
            print(
                f"Auth token available at {default_token_path()} (clipboard unavailable - "
                "read it there, or run --copy-token once a clipboard is available)."
            )
        else:
            sys.exit(
                "Could not persist an auth token, and no clipboard is available "
                "to deliver one either way - refusing to start with no safe way "
                "to retrieve it. Retry, or pass --auth-token (or set AUTH_TOKEN) "
                "with a value you provide, or --no-auth-token to run without one."
            )
    return Settings(
        port=port,
        idle_hours=idle_hours,
        buffer_mb=buffer_mb,
        cert_path=cert_path,
        key_path=key_path,
        use_wss=use_wss,
        auth_token=auth_token,
        host=host,
        allow_origins=tuple(allow_origins),
        max_message_mb=max_message_mb,
        allow_root=allow_root,
    )


def run() -> None:
    """Entry point for the `gridshell-server` console command."""
    settings = resolve_flags()
    try:
        asyncio.run(main(settings))
    except KeyboardInterrupt:
        pass
