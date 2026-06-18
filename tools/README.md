# tools

Vendor-neutral relay tooling (Node 22+, no dependencies).

| Tool | Purpose | Run |
|---|---|---|
| `relay-bridge.mjs` | Mirror live local relay sessions to a remote relay so glasses surfaces (meta / google / snapchat) can see them | `node tools/relay-bridge.mjs` |
| `ws-check.mjs` | Connect to a relay WS like a glasses client and print `hello.threads` | `WS=wss://host/ambient-link/ws node tools/ws-check.mjs` |

Env:
- `relay-bridge.mjs`: `LOCAL_HTTP`, `LOCAL_WS`, `REMOTE`, `POLL_MS`
- `ws-check.mjs`: `WS`

Protocol conformance test lives in [`../protocol/ws-protocol.test.mjs`](../protocol/ws-protocol.test.mjs):

```
node --test protocol/ws-protocol.test.mjs
# AMBIENT_HOST / AMBIENT_WS override the target (default local host :5181)
```
