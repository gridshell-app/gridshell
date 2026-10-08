# GridShell terminal-server wire protocol - JSON Schemas

Message shapes for the three WebSocket endpoints `server.py` exposes: `/terminal`, `/host`, `/mcp`. Written for anyone reimplementing a compatible server (or client) without access to `server.py`'s own source.

## Files

| File | Covers |
|---|---|
| `terminal-client-message.schema.json` | Messages a client sends on `/terminal` (`resize`, `input`, `liveSessions`, `terminate`) |
| `terminal-server-message.schema.json` | Messages the server sends on `/terminal` (`hello`, `attached`, `output`, `spawned`, `resized`, `reconcile`, `evicted`) |
| `request-envelope.schema.json` | The `{id, tool, params, timeoutMs}` envelope sent on `/mcp`, forwarded verbatim to `/host` |
| `response-envelope.schema.json` | The `{id, result}` / `{id, error}` envelope sent back on `/host`, forwarded verbatim to `/mcp` |

`/host` and `/mcp` share one envelope pair - `server.py` relays between them without ever inspecting `tool`/`params`, so one request schema and one response schema cover both endpoints.

## What this does NOT cover

These schemas validate message *shape* only. They say nothing about the *behavioral* guarantees that actually matter for interoperability - a server that gets every message shape byte-for-byte correct can still be behaviorally wrong. In particular:

- **Reattach replays the buffer.** Reattaching to a session with buffered scrollback gets an `output` message (the joined buffer) immediately after `attached`.
- **Optional capabilities.** The first (auth) frame may carry `"caps": [...]`. A server ignores unknown values and a client that sends none gets the behavior described above. `timeline`: the replay is sent as the stretches of output between size changes, each preceded by a `resized` with the size it was written at, so the client can replay at that size and reflow as the size changes. `hello`: the server sends `hello` (its protocol level) right before `spawned`/`attached`, so a client that reaches either without one knows the server predates it. `reconcile`: when a spawn would exceed the document's shell cap, the server sends `reconcile` instead of refusing, the client answers with `liveSessions`, and the server closes that document's sessions missing from the list before deciding.
- **At most one attached socket per session.** A new connection attaching to a session that already has a live socket evicts it - the evicted socket gets `evicted` immediately before the server closes it.
- **Idle-kill only starts on disconnect, not on evict.** Losing the attached socket (WebSocket close) starts a grace-period timer; if nothing reattaches before it elapses, the process is killed and the session is deleted. An explicit `terminate` bypasses this timer entirely.
- **`resize` behaves differently before vs. after the first spawn.** The very first `resize` on a fresh session triggers the process spawn and gets `spawned` back; every subsequent `resize` resizes the running process in place and gets `resized` back.
- **No client connected / timeout on `/mcp`.** If no `/host` client is reachable for the request's session, or the reachable client doesn't reply within `timeoutMs`, `server.py` synthesizes an error envelope itself - this is the one case where a response doesn't originate from a `/host` client at all.

These are documented in prose here (and in `server.py`'s own comments) rather than as schema, since JSON Schema has no way to express them.

