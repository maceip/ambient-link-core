# phone-shared — WS protocol

The contract both `phone-android` and `phone-ios` implement against
[`relay/server.mjs`](../relay/server.mjs). Keep this file in sync with
both codebases; bump `PROTOCOL_VERSION` below when you break wire compat.

```
PROTOCOL_VERSION = 1
```

## Transport

- WebSocket, `wss://<relay-host>/face-chat/ws`
- Text frames, one JSON object per frame, no batching
- Phone daemon reconnects with exponential backoff (500ms → 10s cap) on
  `close` and `error`; resumes per-thread cursor on reconnect

## Server → phone messages

The phone daemon only cares about a subset of what the web app subscribes
to. Specifically it needs `hello` (to learn thread labels for peek cards)
and `thread_idle` (the yank trigger). Other server messages can be
ignored.

### `hello`

Sent once on connect.

```json
{
  "type": "hello",
  "threads": [
    { "id": "claude", "label": "claude", "agent": "claude" },
    { "id": "codex",  "label": "codex",  "agent": "codex"  }
  ],
  "cursor": { "claude": 1234, "codex": 5678 }
}
```

### `thread_idle` — THE YANK TRIGGER

Emitted by the relay when a thread's output stops for `FC_IDLE_MS` (default
2000ms). This is what causes the phone to wake DAT and render a peek card.

```json
{
  "type": "thread_idle",
  "thread": "claude",
  "lastAssistant": "Done — pushed to main. Want me to open a PR?",
  "at": 1717000000000
}
```

The phone daemon SHOULD:
1. Classify `lastAssistant` to pick a chip set (see "Chip classification" below).
2. Open / reuse a DAT session targeted at the user's glasses.
3. Push a **compact peek card** with thread label, truncated message, and
   `open` / `snooze` / `dismiss` buttons.
4. Auto-time-out the peek to ambient after `PEEK_TIMEOUT_MS` (default 12000)
   if no interaction.

### `thread_busy`

Counterpart to `thread_idle`. The phone daemon SHOULD dismiss any open
peek/expand card for the same thread (the agent started typing again).

```json
{ "type": "thread_busy", "thread": "claude", "at": 1717000000000 }
```

### `append`, `snapshot`

Streaming text deltas / full snapshots. The phone daemon does NOT need
these — they exist for the web app. Skip on receipt.

## Phone → server messages

### `subscribe`

Sent on connect / reconnect. The phone daemon doesn't need historical
content, so `since` can be empty or omit individual threads.

```json
{ "type": "subscribe", "since": {} }
```

### `input` — REPLY FROM HUD

When the user taps a chip in the expanded card, the daemon emits the
corresponding text input back to the agent's tmux session.

```json
{ "type": "input", "thread": "claude", "text": "yes", "enter": true }
```

`enter: true` (the default) sends a trailing newline. For chips that send
multi-step or special-key input (e.g. tool-approval prompts that want a
specific `y`/`N` keystroke without echo), use `special`:

### `special`

```json
{ "type": "special", "thread": "claude", "key": "y" }
```

## State machine

Both daemons implement the same six-state machine:

```
                                  on `thread_busy`
                          ┌───────────────────────────────┐
                          ▼                               │
   AMBIENT ── thread_idle ──▶ PEEKING ── tap `open` ──▶ ENGAGED
      ▲                          │                        │
      │                          │ tap `dismiss`/timeout  │ chip tap
      │                          ▼                        │ → emit `input`
      │                       AMBIENT ◀──────────────────┘
      │                                  (close session)
      │                          │
      │                          │ tap `snooze`
      │                          ▼
      │                       SNOOZED ── time elapses ──▶ PEEKING (same content)
      │                                                       (re-emits the yank)
      └───────────────────────────────────────────────────────┘
```

States:

- **AMBIENT** — no DAT session, glasses are doing whatever the user wants
- **PEEKING** — DAT session open, compact peek card on HUD, timer running
- **ENGAGED** — DAT session open, expanded card on HUD, user is reading / picking a chip
- **SNOOZED** — DAT session closed, timer holds the yank for re-fire (default 60s, configurable)

Only one state at a time across all threads (last yank wins). If a new
`thread_idle` arrives while ENGAGED for a different thread, queue it for
re-fire after the user dismisses the current one.

## Chip classification

The daemon picks chip labels by pattern-matching `lastAssistant`:

| Pattern | Chips |
|---|---|
| Ends with `?` or contains "should I" / "would you like" | `yes` · `no` · `tell me more` · `dismiss` |
| Matches `\b[yY]/[nN]\b` or "approve this" / "allow" | `approve` · `deny` · `dismiss` |
| Otherwise (just finished work) | `continue` · `looks good` · `ask follow-up` · `dismiss` |

Each chip maps to an `input` payload:

| Chip | `text` | `enter` |
|---|---|---|
| `yes` | `yes` | true |
| `no` | `no` | true |
| `approve` | `y` | true |
| `deny` | `n` | true |
| `continue` | `continue` | true |
| `looks good` | `looks good, thanks` | true |
| `tell me more` | `tell me more` | true |
| `ask follow-up` | — | (opens chip picker, no immediate send) |
| `dismiss` | — | (close session, no send) |
| `snooze` | — | (re-fire after SNOOZE_MS) |

## DAT-side rendering hints

Both platforms should render the peek card using the DAT display DSL
primitives that exist on each SDK (functionally equivalent — the Android
SDK is the reference; iOS mirrors it). Compact card layout:

```
┌────────────────────────────────┐
│ ▓ claude          paused · now │   ← META text, 12dp, SECONDARY color
│                                │
│ Done — pushed to main. Want    │   ← BODY text, 16dp, PRIMARY color
│ me to open a PR?               │
│                                │
│ [ open ] [ snooze ] [ dismiss ]│   ← buttons: PRIMARY · SECONDARY · OUTLINE
└────────────────────────────────┘
```

Expanded card just extends the body text region (scrollable if available)
and swaps the button row for the chip set chosen above. Same session — no
tear-down between peek and expand.
