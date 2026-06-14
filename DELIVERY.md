# Delivery — HUD replies back into agent sessions

Symmetric with observation (hooks + JSONL + proc → mux), delivery routes
glasses/phone/web chip taps back into live agents.

## Architecture

```
chip tap / dictate_commit
        │
        ▼
  inject.SendInput(thread, text)
        │
        ▼
  mux.SessionForThread → session_id + agent
        │
        ▼
  delivery.Deliver
        ├─ claude/codex: try correlated tmux send-keys (PID from proc watcher)
        └─ always: outbox pending file (~/.ambient-link/outbox/<session>.pending.json)
                │
                ▼
        hooks drain on Stop / PermissionRequest / UserPromptSubmit
        (bidirectional HTTP hook response JSON)
```

## Durable path ranking

| Priority | Mechanism | Claude/Codex | Cursor |
|---|---|---|---|
| 1 | **Outbox + bidirectional hooks** | Primary — `Stop` block+reason, `PermissionRequest` allow/deny | Primary — ingest-side consumer TBD |
| 2 | **Correlated tmux** | Immediate when agent runs inside a tmux pane (PID match) | N/A |
| 3 | **PTY `/dev/ttys*`** | Ruled out on macOS (writes go to terminal emulator as output, not shell stdin) | N/A |
| 4 | **Accessibility / AppleScript** | Rejected | Rejected |

### Post-idle timing

`thread_idle` fires after `Stop`. User taps chip **after** that. The next
`Stop` won't fire until another turn completes. So:

- **tmux** delivers immediately when available.
- **outbox** holds the reply until the next hook event (`Stop`, `PermissionRequest`, or `UserPromptSubmit`) drains it.

Non-tmux Claude sessions rely on outbox + hooks until the user starts another
interaction or Claude fires another lifecycle hook.

## Cursor SDK verdict (2026-06-14)

**`Agent.list` / `Agent.resume` do NOT reach live IDE Composer or ad-hoc CLI
sessions you started manually.**

Evidence:

- SDK local agents persist in a **separate checkpoint store** (SQLite/JSONL
  under SDK state root), not the IDE's in-memory Composer session.
- `Agent.create()` always mints a **new** `agentId`; `Agent.resume(agentId)`
  only works for agents previously created via SDK (or another process using
  the same store + `cwd`).
- Docs: *"The SDK doesn't auto-discover credentials from a local Cursor app
  installation."*
- `~/.cursor/chats/` holds IDE chat blobs; SDK `Agent.list({ runtime: "local", cwd })` lists SDK-store agents only.
- `cursor-agent --resume <session_id>` is a **CLI session id**, not the same
  surface as `@cursor/sdk` agent IDs unless explicitly bridged.

**Implication for Ambient Link:** Cursor observation via `POST /ambient-link/ingest`
(from a Cursor extension or hook) is correct. Delivery for live IDE sessions
is **outbox + ingest-side consumer** (extension reads outbox and injects into
Composer), **not** `@cursor/sdk` against the open tab.

SDK is appropriate only when **we** spawn the agent via `Agent.create` and
store that `agentId` at creation time — a different product mode than
"HUD for whatever agent I'm already running in Cursor."

## Claude / Codex

Same as Cursor for native inject APIs: no `claude inject` yet (GitHub feature
requests). Hooks are the official bidirectional surface. Installer wires
`PermissionRequest` in addition to observation hooks.

## Files

| Path | Role |
|---|---|
| `host/internal/delivery/outbox.go` | Durable pending queue per session |
| `host/internal/delivery/tmux.go` | PID → tmux pane → send-keys |
| `host/internal/delivery/hooks.go` | Hook response JSON from outbox |
| `host/internal/delivery/deliver.go` | Adapter router |
| `host/internal/delivery/registry.go` | proc watcher PID/session/tty map |
| `host/internal/inject/inject.go` | WS/debug entry point |

## Sync layers (host as source of truth)

| Layer | Authority | Sync mechanism |
|---|---|---|
| Mac agents | Ground truth | hooks + JSONL + proc → mux |
| Go host mux | Canonical session state | `/ambient-link/status`, WS `hello` + `thread_*` |
| Web companion | Cache | WS events + poll `/ambient-link/status` every 15s |
| Phone APK | Cache | WS `hello` + `thread_idle`/`hud_yank`; chip → `input` |
| Glasses DAT | Display only | Phone `HudPresenter`; no direct host link |

Phone and web should treat host WS + status as authoritative; local HUD
state is ephemeral (peek/engage/snooze).
