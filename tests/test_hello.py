"""The `hello` message: pty/build only for a pywinpty older than 3.1."""
import json
import sys
from types import SimpleNamespace

import pytest

from gridshell.server import common


@pytest.fixture
def windows(monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(
        sys, "getwindowsversion", lambda: SimpleNamespace(build=26200), raising=False
    )


def hello_with(monkeypatch, pywinpty_version):
    monkeypatch.setattr(common, "version", lambda name: pywinpty_version)
    return json.loads(common.hello_message())


@pytest.mark.parametrize("ver", ["2.0.0", "3.0.5", "3.0.99"])
def test_old_pywinpty_sends_pty_and_build(windows, monkeypatch, ver):
    assert hello_with(monkeypatch, ver) == {
        "type": "hello", "protocol": common.PROTOCOL_LEVEL, "pty": "conpty", "build": 26200,
    }


@pytest.mark.parametrize("ver", ["3.1.0", "3.1.0rc1", "3.2.0", "4.0.0"])
def test_new_pywinpty_sends_neither(windows, monkeypatch, ver):
    assert hello_with(monkeypatch, ver) == {"type": "hello", "protocol": common.PROTOCOL_LEVEL}


def test_unreadable_version_keeps_the_corrections(windows, monkeypatch):
    assert hello_with(monkeypatch, "dev")["pty"] == "conpty"


def test_not_windows_never_sends_them(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    assert hello_with(monkeypatch, "3.0.5") == {
        "type": "hello", "protocol": common.PROTOCOL_LEVEL,
    }
