"""The shell process: which one to run on each platform, and how it is spawned."""

import codecs
import os
import shutil
import sys
from typing import Any, Protocol

if sys.platform == "win32":
    from winpty import PtyProcess
else:
    # PtyProcessUnicode gives str in/out on read()/write(), matching pywinpty.
    from ptyprocess import PtyProcessUnicode as PtyProcess


class PtyLike(Protocol):
    """The slice of a pywinpty / ptyprocess process this server relies on."""

    def read(self, size: int = ...) -> str: ...
    def write(self, data: str) -> Any: ...
    def setwinsize(self, rows: int, cols: int) -> None: ...
    def terminate(self, force: bool = ...) -> Any: ...


def shell_command() -> str:
    if sys.platform == "win32":
        return "powershell.exe"
    return os.environ.get("SHELL", "/bin/bash")


def _spawn_argv() -> str | list[str]:
    # pywinpty accepts a bare string; ptyprocess requires a real list/tuple.
    cmd = shell_command()
    return cmd if sys.platform == "win32" else [cmd]


def shell_display_path() -> str:
    # Full resolved path, not bare command name - the client
    # only collapses paths that have a separator.
    return shutil.which(shell_command()) or shell_command()


def _tolerate_invalid_utf8(proc: PtyLike) -> None:
    # ptyprocess decodes strictly, so one invalid byte (e.g. `cat` on a binary
    # file) would raise in the reader and end the session. pywinpty has no decoder.
    if getattr(proc, "decoder", None) is not None:
        proc.decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")


class SpawnFn(Protocol):
    """Starts a shell. create_app() takes one, so tests can swap in a fake."""

    def __call__(
        self, *, cwd: str | None, env: dict[str, str], dimensions: tuple[int, int]
    ) -> PtyLike: ...


def spawn_shell(*, cwd: str | None, env: dict[str, str], dimensions: tuple[int, int]) -> PtyLike:
    """The real SpawnFn. `dimensions` is (rows, cols)."""
    proc = PtyProcess.spawn(_spawn_argv(), cwd=cwd, env=env, dimensions=dimensions)
    _tolerate_invalid_utf8(proc)
    return proc
