"""GridShell Python client - talk to a self-hosted GridShell terminal-server
from a script or a REPL, over the same plain WebSocket /mcp envelope
protocol GridShell's own MCP relay (mcp_grid.py) uses.

Sheets is the only host implemented today:

    from gridshell import SheetsClient

    grid = SheetsClient(port=3012)
    grid.get_values("A1:C10")
    grid.set_values([[1, 2, 3]], range="A1:C1")
    grid.append_row(["Widget", 12, "2026-09-06"])
    grid.run_batch([{"chain": [...]}])   # generic structured-chain escape hatch
    grid.call("someNewTool", {...})      # raw escape hatch, by tool name"""

from .exceptions import (
    GridShellConnectionError,
    GridShellError,
    GridShellException,
    GridShellTimeoutError,
)
from .sheets import SheetsClient

__version__ = "1.0.0b2"

__all__ = [
    "SheetsClient",
    "GridShellException",
    "GridShellConnectionError",
    "GridShellError",
    "GridShellTimeoutError",
]
