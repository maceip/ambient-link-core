# ambient-link-core

> The hardware-agnostic core of **Ambient Link** — a framework for surfacing
> live coding-agent activity on face-worn displays.

Ambient Link separates **what's happening with your agents** from **where you
look to see it**. This repository is the "what's happening" half: a daemon
that watches your `claude` / `codex` / future coding agents wherever they
run, normalizes their lifecycle events into a single wire protocol, and
fans them out to hardware-specific "relay" clients (mobile apps that drive
the actual face-worn display).

The current target relay is at **[ambient-link-meta](https://github.com/maceip/ambient-link-meta)**
(Meta Ray-Ban Display glasses). Future targets (Apple, Android XR) get
their own relay repos but consume the same protocol and connect to the
same host.

## What's in here

| Path | Purpose |
|---|---|
| [`host/`](host) | The production host daemon (Go). Multi-source signal aggregator: hooks ingest, JSONL tailer, process watcher → state machine → WS broadcast. |
| [`protocol/PROTOCOL.md`](protocol/PROTOCOL.md) | Wire-stable protocol between host and any relay. Single source of truth — both Go and any relay implementation must conform. |
| [`agents/`](agents) | Helper scripts to launch `claude` / `codex` / etc. in local tmux sessions (optional; the Go host normally uses hooks + JSONL). |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Full coverage of which signal sources see which sessions, why we run them in parallel, and what the option space looks like. |

## Quick start

Build + run the host daemon:

```bash
cd host
go build -o /usr/local/bin/ambient-link-host ./cmd/host
ambient-link-host install       # writes Claude/Codex hook configs + service unit
ambient-link-host serve         # foreground; or use the service unit
curl localhost:5181/face-chat/status
```

Then point your relay app (e.g. ambient-link-meta) at
`ws://<host-ip>:5181/face-chat/ws`.

## Design principles

- **Defense in depth.** No single signal source is authoritative. Hooks +
  JSONL file tailing + process watching all run in parallel; the mux
  dedupes on `(session_id, event_type)` within a short window.
- **Never mutates user OS settings.** Notification preferences, app
  permissions, AccessibilityService grants — all user preference, treated
  as immutable. The daemon is read-mostly.
- **One canonical wire protocol** ([protocol/PROTOCOL.md](protocol/PROTOCOL.md)).
  Hardware-specific behavior lives in the relay repo for that hardware,
  not here.
- **Single static binary** for the host daemon. No node/npm/python runtime
  required on a user's laptop. `curl | sh` installable.

## Roadmap

- Browser-extension producer for `claude.ai/code` / `chatgpt.com/codex`
  coverage of web-only sessions
- Live-audio path — bidirectional voice between agent and user via the
  relay's hardware mic + speakers
- Attested-enclave routing for end-to-end auth of cross-host events
