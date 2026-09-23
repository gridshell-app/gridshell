"""SheetsClient - GridShell's Google Sheets host, on top of the shared
Connection transport."""

from typing import Any, List, Optional

from .exceptions import GridShellError
from .transport import DEFAULT_TIMEOUT, Connection

# Margin so this client's timeout clears runBatch's real budget.
BUDGET_MARGIN = 30


def _check_flat_row(values: Any, action: str) -> None:
    """Client-side validation: a nested list (e.g. [[3, 4]] instead of
    [3, 4]) would otherwise silently write as one cell containing Java's
    default toString() output, instead of erroring."""
    if isinstance(values, (str, bytes)):
        raise ValueError(
            f"{action} got a string ({values!r}), not a list of values. "
            f"A string is iterable character-by-character, so without this "
            f"check it would silently be treated as one value per character. "
            f"If you meant to write this string as a single cell, wrap it in "
            f"a list yourself, e.g. [{values!r}]."
        )
    if any(isinstance(v, (list, tuple)) for v in values):
        raise ValueError(
            f"{action} expects a flat list of one row's values (e.g. [1, 2, 3]), "
            f"not a nested list/tuple - got {values!r}. Did you mean to drop the "
            f"extra brackets, or did you want set_values([[...]]) instead?"
        )


def _normalize_2d(values: Any, action: str) -> list:
    """Accepts either a 2D array of rows, or a flat list/tuple treated as
    a single row (wrapped automatically) - same one-row convention as
    append_row(). Raises only on an ambiguous mix of rows and bare
    values in the same list."""
    if isinstance(values, (str, bytes)):
        raise ValueError(
            f"{action} got a string ({values!r}), not a list of values. "
            f"A string is iterable character-by-character, so without this "
            f"check it would silently write one character per cell instead "
            f"of the string itself. If you meant to write this string as a "
            f"single cell, wrap it in a list yourself, e.g. [{values!r}]."
        )
    if not values:
        raise ValueError(f"{action} got an empty values argument.")
    is_2d = isinstance(values[0], (list, tuple))
    if any(isinstance(v, (list, tuple)) != is_2d for v in values):
        raise ValueError(
            f"{action} got a mix of rows and plain values in the same list - {values!r}. "
            f"Use a flat list for one row, or a list of lists for multiple rows."
        )
    return list(values) if is_2d else [list(values)]


class SheetsClient:
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
        self._conn = Connection(
            host=host, port=port, wss=wss, session=session, session_key=session_key,
            token=token, default_timeout=default_timeout,
        )
        self._budget_fetched = False

    def connect(self) -> None:
        self._conn.connect()
        self._fetch_batch_time_budget()

    def close(self) -> None:
        self._conn.close()
        # Reset so a future reconnect re-fetches, in case the server's own
        # budget setting changed in the meantime.
        self._budget_fetched = False

    def __enter__(self) -> "SheetsClient":
        self.connect()
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def _fetch_batch_time_budget(self) -> None:
        # Asks the server for its real time budget once per connection and
        # derives this client's own default timeout with margin.
        # Best-effort: a server without this accessor leaves
        # default_timeout unchanged.
        if self._budget_fetched:
            return
        self._budget_fetched = True
        try:
            budget_ms = self._conn.call("getBatchTimeBudgetMs", {}, timeout=self._conn.default_timeout)
        except Exception:
            return
        if isinstance(budget_ms, (int, float)) and budget_ms > 0:
            self._conn.default_timeout = (budget_ms / 1000) + BUDGET_MARGIN

    def call(self, tool: str, params: Optional[dict] = None, timeout: Optional[float] = None) -> Any:
        """Raw escape hatch - call any tool by name with a params dict. 
        Connects lazily on first use."""
        if not self._budget_fetched:
            self.connect()
        return self._conn.call(tool, params, timeout)

    def run_batch(self, ops: List[dict], timeout: Optional[float] = None) -> Any:
        """The generic structured-chain escape hatch. `ops` is a list of
        {"chain": [...]} entries, each a sequence of {"method", "args"} steps
        (plus {"index"}/{"chartId"} lookups and {"__chain": [...]} builder
        arguments) resolved starting from SpreadsheetApp, see
        mcp_grid.py's runBatch tool description for the full grammar and
        worked examples."""
        return self.call("runBatch", {"ops": ops}, timeout=timeout)

    def _run_single_op(self, chain: List[dict], action: str, timeout: Optional[float]) -> Any:
        """Runs a one-op run_batch chain and unwraps the batch envelope
        into a plain value, or raises GridShellError with the envelope's
        detail. Used only by get_values()/set_values()'s selection path."""
        batch = self.run_batch([{"chain": chain}], timeout=timeout)
        status = batch.get("status")
        if status == "complete":
            # Use .get(), not indexing. An unserializable result (e.g. a
            # fluent setValues() call) means "value" may be absent, not null.
            return batch["results"][0].get("value")
        if status == "rejected":
            raise GridShellError(batch.get("message") or f"{action} was rejected ({batch.get('reason')})")
        error = batch.get("error") or {}
        raise GridShellError(error.get("message") or f"{action} did not complete (reason: {batch.get('reason')})")

    def get_values(
        self,
        range: Optional[str] = None,
        sheet: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> List[list]:
        """Read a range's values.

        Omit `range` to read the current selection instead
        (SpreadsheetApp.getActiveRange()). `sheet` isn't valid together
        with a selection read - the active selection is always on
        whichever sheet is currently active, so there's no such thing as
        "the selection on a different sheet". Pass an explicit `range` if
        you want to target one.
        """
        if range is None:
            if sheet is not None:
                raise ValueError(
                    "sheet is not supported together with range=None (reading the "
                    "current selection) - the active selection is always on "
                    "whichever sheet is currently active. Pass an explicit range "
                    "to target a different sheet."
                )
            chain = [{"method": "getActiveRange", "args": []}, {"method": "getValues", "args": []}]
            return self._run_single_op(chain, "get_values() on the current selection", timeout)
        params = {"range": range}
        if sheet is not None:
            params["sheet"] = sheet
        return self.call("getValues", params, timeout=timeout)

    def set_values(
        self,
        values: list,
        range: Optional[str] = None,
        sheet: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Write values into a range.

        `values` is normally a 2D array of rows, but a flat list is also
        accepted as a single row (e.g. [1, 2, 3] same as [[1, 2, 3]]) -
        the same convention append_row() uses.

        Omit `range` to write into the current selection instead, with
        the same sheet=None-only restriction as get_values() above.
        """
        values = _normalize_2d(values, "set_values()")
        if range is None:
            if sheet is not None:
                raise ValueError(
                    "sheet is not supported together with range=None (writing to "
                    "the current selection) - the active selection is always on "
                    "whichever sheet is currently active. Pass an explicit range "
                    "to target a different sheet."
                )
            chain = [{"method": "getActiveRange", "args": []}, {"method": "setValues", "args": [values]}]
            self._run_single_op(chain, "set_values() on the current selection", timeout)
            return
        params = {"range": range, "values": values}
        if sheet is not None:
            params["sheet"] = sheet
        self.call("setValues", params, timeout=timeout)

    def append_row(self, values: list, sheet: Optional[str] = None, timeout: Optional[float] = None) -> None:
        """Append one row after the end of the sheet's existing data."""
        _check_flat_row(values, "append_row()")
        params = {"values": values}
        if sheet is not None:
            params["sheet"] = sheet
        self.call("appendRow", params, timeout=timeout)
