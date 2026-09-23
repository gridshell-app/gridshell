"""Smoke tests for mcp_grid.py: build_runtime_config() and the tool
request-validation in on_call_tool.

No pytest-asyncio - driven with asyncio.run() directly, same as the rest
of this project's test dependencies (pytest + jsonschema only).
"""
import asyncio
import sys

import pytest

from gridshell import mcp_grid


class FakeParams:
    def __init__(self, name, arguments):
        self.name = name
        self.arguments = arguments


class FakeConnection:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.calls = []

    async def call(self, tool_name, params):
        self.calls.append((tool_name, params))
        if self.error is not None:
            raise self.error
        return self.result


@pytest.fixture(autouse=True)
def restore_connection():
    original = mcp_grid.connection
    yield
    mcp_grid.connection = original


def test_import_succeeds_regardless_of_argv(monkeypatch):
    # The regression this whole file exists to catch: importing the module
    # must never look at sys.argv, so a bogus argv (e.g. this process was
    # actually launched as `pytest --some-pytest-flag`) can't break it.
    monkeypatch.setattr(sys, "argv", ["gridshell-mcp", "--totally-bogus-flag"])
    import importlib

    importlib.reload(mcp_grid)
    assert mcp_grid.connection is None  # only set once main() actually runs


def test_build_runtime_config_defaults(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["gridshell-mcp"])
    monkeypatch.delenv("PORT", raising=False)
    monkeypatch.delenv("AUTH_TOKEN", raising=False)
    monkeypatch.delenv("GRIDSHELL_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("GRIDSHELL_SESSION", raising=False)
    monkeypatch.delenv("GRIDSHELL_SESSION_KEY", raising=False)
    url, auth = mcp_grid.build_runtime_config()
    assert url == "ws://localhost:3000/mcp"
    assert auth == {"token": None, "session": "", "sessionKey": ""}


def test_build_runtime_config_reads_flags_and_env(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["gridshell-mcp", "--port", "3012", "--wss"])
    monkeypatch.setenv("GRIDSHELL_SESSION", "sheet-abc")
    monkeypatch.setenv("GRIDSHELL_SESSION_KEY", "the-key")
    monkeypatch.setenv("GRIDSHELL_AUTH_TOKEN", "auto-picked-token")
    monkeypatch.delenv("AUTH_TOKEN", raising=False)
    url, auth = mcp_grid.build_runtime_config()
    assert url == "wss://localhost:3012/mcp"
    assert auth == {"token": "auto-picked-token", "session": "sheet-abc", "sessionKey": "the-key"}


def test_build_runtime_config_rejects_unknown_flag(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["gridshell-mcp", "--nope"])
    with pytest.raises(SystemExit):
        mcp_grid.build_runtime_config()


def test_on_list_tools_returns_the_four_documented_tools():
    result = asyncio.run(mcp_grid.on_list_tools(None, None))
    names = {tool.name for tool in result.tools}
    assert names == {"runBatch", "getValues", "setValues", "appendRow"}


@pytest.mark.parametrize(
    "name,args",
    [
        ("runBatch", {"ops": "not-an-array"}),
        ("runBatch", {}),
        ("setValues", {"values": [[1]]}),  # range missing
        ("getValues", {"range": ["not", "a", "string"]}),
        ("appendRow", {"values": {"not": "a list"}}),
    ],
)
def test_on_call_tool_rejects_malformed_arguments_without_reaching_the_connection(name, args):
    fake = FakeConnection()
    mcp_grid.connection = fake
    result = asyncio.run(mcp_grid.on_call_tool(None, FakeParams(name, args)))
    assert result.is_error is True
    assert fake.calls == []  # never reached the connection


def test_on_call_tool_forwards_well_formed_arguments():
    fake = FakeConnection(result=[["A1"]])
    mcp_grid.connection = fake
    result = asyncio.run(mcp_grid.on_call_tool(None, FakeParams("getValues", {"range": "A1:B2"})))
    assert fake.calls == [("getValues", {"range": "A1:B2"})]
    assert not result.is_error
