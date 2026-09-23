"""Exceptions raised by the gridshell client."""


class GridShellException(Exception):
    """Base class for every exception this library raises."""


class GridShellConnectionError(GridShellException):
    """The connection to the terminal server could not be made, or was lost mid-call."""


class GridShellTimeoutError(GridShellException):
    """A call did not get a matching response within its timeout."""


class GridShellError(GridShellException):
    """The server (or the host client it relayed to) returned an error for a call.

    The message is whatever the server sent, e.g. a deny-list
    message, a runBatch step failure, or "No client connected for this
    session." passed through unchanged rather than reworded.
    """
