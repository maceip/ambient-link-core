# protocol — WS protocol

The contract both `relay-android` and `relay-ios` implement against the
Go host daemon ([`host/`](../host)). Keep this file in sync with both
codebases; bump `PROTOCOL_VERSION` below when you break wire compat.

```
PROTOCOL_VERSION = 1
```

_Additive in v1: `hud_yank` (web → server → phone daemons), `dictate_*` (phone/web STT sessions), `session_focus` / `session_blur` (mic pre-warm)._

## Transport

- WebSocket, `wss://<relay-host>/ambient-link/ws`
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

Emitted by the relay when a thread's output stops for `AMBIENT_IDLE_MS` (default
2000ms). This is what causes the phone to wake DAT and render a peek card.

```json
{
  "type": "thread_idle",
  "thread": "claude",
  "label": "claude",
  "agent": "claude",
  "lastAssistant": "Done — pushed to main. Want me to open a PR?",
  "awaiting": "reply",
  "at": 1717000000000
}
```

When the host mux detects a tool permission prompt, the same message
includes `awaiting: "permission"` and `permissionPrompt` (the prompt text).
Phone daemons render approve/deny chips directly on the peek card.

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

### `hud_yank` — MANUAL PEEK FROM WEB

Forwarded by the relay when the glasses web app asks the phone daemon to
push a peek card for a thread (e.g. user taps **push to HUD** in chat).
Same payload shape as `thread_idle`; phone daemons SHOULD treat it
identically (`HudPresenter.yank()`).

```json
{
  "type": "hud_yank",
  "thread": "claude",
  "label": "claude",
  "agent": "claude",
  "lastAssistant": "Done — pushed to main. Want me to open a PR?",
  "awaiting": "reply",
  "at": 1717000000000
}
```

Not sent back to the requesting web client (relay fans out to other
connected clients only).

### `dictate_active`, `dictate_partial`, `dictate_end`

The host fans out dictation UI state while a client captures speech.
Capture runs on the phone (`SpeechRecognizer`) or web (`SpeechRecognition`);
on-device SODA (see `~/neural/.../lib/soda`) is a future v2 path.

```json
{ "type": "dictate_active", "thread": "claude", "source": "phone", "at": 1717000000000 }
{ "type": "dictate_partial", "thread": "claude", "text": "please fix the", "source": "web", "at": 1717000000001 }
{ "type": "dictate_end", "thread": "claude", "text": "please fix the tests", "source": "phone", "at": 1717000000002 }
```

`dictate_end` with empty `text` means the session was aborted. When `text`
is set on commit, the host has already injected the transcript via the same
path as `input` and updated mux `lastUserInput`.

## Web / phone → server messages

### `subscribe`

Sent on connect / reconnect. The phone daemon doesn't need historical
content, so `since` can be empty or omit individual threads.

```json
{ "type": "subscribe", "since": {} }
```

### `input` — REPLY FROM HUD

When the user taps a chip in the expanded card, the daemon emits the
corresponding text input back to the agent session (via the host).

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

### `hud_yank` — REQUEST PEEK ON PHONE

Sent by the glasses web app when the user wants the native daemon to
render a HUD peek for the current thread. The relay enriches `thread`
with `label` and `lastAssistant` from live pane state, then forwards
`hud_yank` to connected phone daemons.

```json
{ "type": "hud_yank", "thread": "claude" }
```

### `dictate_begin`, `dictate_partial`, `dictate_commit`, `dictate_abort`

Phone or web companion starts dictation for a thread. Partials are UI-only;
`dictate_commit` injects `text` with `enter: true` and records
`lastUserInput` on the mux (same as tapping a chip).

```json
{ "type": "dictate_begin", "thread": "claude", "source": "phone" }
{ "type": "dictate_partial", "thread": "claude", "text": "please fix", "source": "phone" }
{ "type": "dictate_commit", "thread": "claude", "text": "please fix the tests", "source": "phone" }
{ "type": "dictate_abort", "thread": "claude", "source": "web" }
```

### `session_focus`, `session_blur`

Sent when the user opens or leaves a session in the glasses web app (or mirrored from HUD activity). Phone daemons may start the mic before `dictate_begin` to cut latency.

```json
{ "type": "session_focus", "thread": "claude", "source": "web" }
{ "type": "session_blur", "thread": "claude", "source": "web" }
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

The host mux sets `awaiting` on `thread_idle` / `hud_yank`:

| `awaiting` | Peek chips (2-up) | Action |
|---|---|---|
| `permission` | `approve` · `deny` | `input` with `y` / `n` |
| `question` | `yes` · `dictate` | `input` yes, or `dictate_*` for custom text |
| `done` | `continue` · `verify` | `input` with preset phrases |

Each chip maps to an `input` payload (except `dictate`, which opens
phone/web STT and ends in `dictate_commit`):

| Chip | `text` | `enter` |
|---|---|---|
| `yes` | `yes` | true |
| `approve` | `y` | true |
| `deny` | `n` | true |
| `continue` | `continue` | true |
| `verify` | `please verify the tasks are completed` | true |
| `dictate` | — | (`dictate_begin` → `dictate_commit`) |

Follow-up picker (after `verify` / expanded card): `change it`, `explain more`,
`what's next?`, plus agent-tuned extras (`fix errors` for codex,
`continue task` for claude).

Hardware back / timeout dismisses the peek without sending.

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
│ [ yes ] [ dictate ]            │   ← question: PRIMARY · SECONDARY
│ [ continue ] [ verify ]        │   ← done: PRIMARY · SECONDARY
│ [ approve ] [ deny ]           │   ← permission
└────────────────────────────────┘
```

Expanded card just extends the body text region (scrollable if available)
and swaps the button row for the chip set chosen above. Same session — no
tear-down between peek and expand.
