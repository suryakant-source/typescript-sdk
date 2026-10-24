---
'@modelcontextprotocol/sdk': patch
---

fix(stdio): stop EventEmitter listener accumulation in the stdio transport

Backports the listener-cleanup approach of #3073 to v1.x, fixing #3070 on the v1 line:

- `send()` used to add a dedicated `drain` listener to `stdin` for every write that hit a full buffer, so sustained back-pressure accumulated listeners and triggered `MaxListenersExceededWarning`. Sends now share a single `drain` wait, and any still-pending sends are settled when
  the process exits or is closed so their promises cannot hang.
- `close()` now detaches the listeners installed in `start()` (the `data` listener on `stdout` and the `error` listeners) and explicitly unpipes and ends the `stderr` pass-through, so the `drain` listener `pipe()` attaches cannot linger on a dead stream.
