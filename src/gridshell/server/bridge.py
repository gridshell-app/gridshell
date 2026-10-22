"""The /host and /mcp relay: calls from an MCP server, forwarded to the
sidebar's /host connection and answered back."""

import asyncio
import json
from dataclasses import dataclass
from typing import Any

import websockets
from websockets.asyncio.server import ServerConnection

from .common import keys_match, log, session_suffix
from .terminal.session import SessionTable

DEFAULT_RELAY_TIMEOUT_S = 15.0


def _relay_timeout_s(message: dict[str, Any]) -> float:
    ms = message.get("timeoutMs") or 0
    return ms / 1000 if ms > 0 else DEFAULT_RELAY_TIMEOUT_S


@dataclass
class PendingCall:
    """One /mcp call relayed to a /host client, awaiting its reply."""

    relay_id: str
    caller_id: Any  # the id the /mcp caller used; the relay id replaces it on the wire
    session_id: str
    message: dict[str, Any]  # as relayed, id already rewritten to relay_id
    mcp_ws: ServerConnection  # where the reply goes
    ws: ServerConnection | None  # /host socket it was sent on; None until delivered
    timer: asyncio.TimerHandle | None = None

    @property
    def sent(self) -> bool:
        return self.ws is not None

    async def reply(self, result: dict[str, Any]) -> None:
        try:
            await self.mcp_ws.send(json.dumps({**result, "id": self.caller_id}))
        except Exception:
            pass


class HostBridge:
    """The /host and /mcp relay: one /host connection per open shell, keyed by session.

    Each call in flight is a PendingCall in `pending`. One for a session
    with no client connected (not yet `sent`) is flushed to that session's
    next connection (see _flush_pending). It times out via _expire if no
    reconnect happens, or fails at once via fail_pending if the shell
    closes first. A call already sent whose client disconnects
    mid-response is never retried - failed at once via _fail_in_flight.
    """

    def __init__(self, table: SessionTable) -> None:
        # Lets /host and /mcp check a connection's key against the session it names.
        self.table = table
        self.clients: dict[str, ServerConnection] = {}  # session id -> /host socket
        self.pending: dict[str, PendingCall] = {}  # relay id -> call
        self.next_relay_id = 1

    def resolve_client(self, session_id: str) -> ServerConnection | None:
        if session_id:
            return self.clients.get(session_id)
        if len(self.clients) == 1:
            return next(iter(self.clients.values()))
        return None

    async def _flush_pending(self, session_id: str, ws: ServerConnection) -> None:
        for call in list(self.pending.values()):
            if call.session_id == session_id and not call.sent:
                call.ws = ws
                try:
                    await ws.send(json.dumps(call.message))
                except Exception:
                    pass

    def _take(self, relay_id: str) -> PendingCall | None:
        """Remove a call and stop its timer; whoever takes it owes it a reply."""
        call = self.pending.pop(relay_id, None)
        if call is not None and call.timer is not None:
            call.timer.cancel()
        return call

    def _fail(self, relay_id: str, reason: str) -> None:
        call = self._take(relay_id)
        if call is not None:
            asyncio.ensure_future(call.reply({"error": reason}))

    def _expire(self, relay_id: str) -> None:
        self._fail(relay_id, "Timeout waiting for client")

    def fail_pending(self, session_id: str, reason: str) -> None:
        for relay_id, call in list(self.pending.items()):
            if call.session_id == session_id and not call.sent:
                self._fail(relay_id, reason)

    def _fail_in_flight(
        self, session_id: str, ws: ServerConnection, too_large: bool = False
    ) -> None:
        # Fails immediately, scoped to this exact ws so a newer reconnect
        # isn't touched. Not too_large: nothing was delivered.
        if too_large:
            reason = (
                "The response was too large for this connection's message-size limit "
                "and never arrived - nothing was delivered, safe to retry with a "
                "smaller range, or raise --max-message-mb on the server."
            )
        else:
            reason = (
                "Bridge disconnected while awaiting a response - outcome unknown, "
                "verify before retrying."
            )
        for relay_id, call in list(self.pending.items()):
            if call.session_id == session_id and call.ws is ws:
                self._fail(relay_id, reason)

    async def handle(self, ws: ServerConnection, session_id: str, host_key: str = "") -> None:
        # Naming a session id isn't proof of owning it, same as on /mcp.
        session = self.table.get(session_id)
        if session is None:
            # Not a rejection: the sidebar opens /host before its dialog has
            # spawned the shell. 1013, not 1008, so the sidebar keeps retrying.
            await ws.close(code=1013, reason="session not ready yet")
            return
        expected_key = session.host_key
        if not expected_key or not keys_match(host_key, expected_key):
            log(f"[host] rejected connection for session {session_id}: invalid or missing host key")
            await ws.close(code=1008, reason="invalid or missing host key")
            return
        old_ws = self.clients.get(session_id)
        if old_ws is not None and old_ws is not ws:
            # Close it explicitly, or it looks connected to its client while
            # every call to it times out. Not 1008, which stops the sidebar
            # retrying: this is a handover, the old tab may reclaim the bridge.
            try:
                await old_ws.close(code=4000, reason="superseded by a new /host connection")
            except Exception:
                pass
        self.clients[session_id] = ws
        await self._flush_pending(session_id, ws)
        log("[host] client connected" + session_suffix(session_id))
        too_large = False
        try:
            async for raw in ws:
                try:
                    m = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                call = self._take(m.get("id"))
                if call is not None:
                    await call.reply(m)
        except websockets.exceptions.ConnectionClosed as exc:
            # An over-max_size frame closes with 1009, only visible on the exception.
            sent = getattr(exc, "sent", None)
            too_large = sent is not None and sent.code == 1009
        finally:
            log("[host] client disconnected" + session_suffix(session_id))
            if self.clients.get(session_id) is ws:
                del self.clients[session_id]
            self._fail_in_flight(session_id, ws, too_large=too_large)

    async def handle_mcp(self, ws: ServerConnection, session_id: str, session_key: str) -> None:
        loop = asyncio.get_running_loop()
        if session_id:
            # Naming a session id isn't proof of owning it: require the
            # per-spawn GRIDSHELL_SESSION_KEY.
            session = self.table.get(session_id)
            expected_key = session.session_key if session else None
            if not expected_key or not keys_match(session_key, expected_key):
                log(
                    f"[mcp] rejected connection for session {session_id}: "
                    "invalid or missing session key"
                )
                await ws.close(code=1008, reason="invalid or missing session key")
                return
        log("[mcp] MCP server connected" + session_suffix(session_id))
        try:
            async for raw in ws:
                try:
                    m = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue

                target = self.resolve_client(session_id)
                # No session id is ambiguous, not a transient gap: fail now. An
                # explicit session id with no client queues instead.
                if target is None and not session_id:
                    if len(self.clients) > 1:
                        error = (
                            "Multiple clients connected and no session was specified - run "
                            "this MCP server from a document's embedded terminal, or set "
                            "GRIDSHELL_SESSION."
                        )
                    else:
                        error = "Client not connected"
                    await ws.send(json.dumps({"id": m.get("id"), "error": error}))
                    continue

                relay_id = f"r{self.next_relay_id}"
                self.next_relay_id += 1
                call = PendingCall(
                    relay_id=relay_id,
                    caller_id=m.get("id"),
                    session_id=session_id,
                    message={**m, "id": relay_id},
                    mcp_ws=ws,
                    ws=target,
                )
                call.timer = loop.call_later(_relay_timeout_s(m), self._expire, relay_id)
                self.pending[relay_id] = call
                if target is not None:
                    await target.send(json.dumps(call.message))
                # else: left pending, delivered by _flush_pending() on
                # reconnect, or timed out/failed by _expire/fail_pending.
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            log("[mcp] MCP server disconnected" + session_suffix(session_id))
            # Else they sit until their timer fires and sends to a closed socket.
            for relay_id, call in list(self.pending.items()):
                if call.mcp_ws is ws:
                    self._take(relay_id)
