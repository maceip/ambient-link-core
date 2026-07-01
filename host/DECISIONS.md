# Relay — Architecture & Decision Record

This document is the durable record of *why the relay is built the way it is*.
It exists so that no future session (human or agent) can claim "the core is
corrupt" or "nobody decided this" — every load-bearing choice is written down
with its rationale. The guiding question for every decision below was, in order:

1. **What would a Google engineer do?** (correctness, observability, no magic)
2. **What does the existing code already do?** (search before inventing)
3. **What makes the human using this actually get joy?** (it must *just work*)
4. **If unsure, do the harder thing.**

---

## 0. What the relay is

The relay is a laptop-side daemon. It does four jobs:

1. **Track** coding-agent CLI sessions (claude, codex, cursor-agent) — both
   agents the relay launched itself *and* agents already running in some other
   terminal. (G1)
2. **Show** agent→human content reliably (assistant text, permission prompts,
   idle/awaiting state) to the native app + glasses web app. (G2/G3)
3. **Deliver** human→agent messages back *into* those sessions reliably, and
   say honestly whether the message landed. (G4)
4. **Persist** every agent↔human interaction in a local database, and bridge
   LAN (mDNS) + a cloud backup endpoint so the phone can reach the laptop even
   when they aren't on the same network. (G5)

The **native app remains the source of truth.** The relay is an observer and a
courier; when the app and the relay disagree about history, the app wins. The
relay's job is to be a faithful, durable, reconcilable mirror — never to invent
state the app can't verify.

---

## 1. The decision that started this: rebuild vs. evolve

The instruction was "delete essentially the entirety of the relay and build one
that doesn't suck." Taken literally that means: blank every file and retype the
observation pipeline, the WS protocol, the JSONL parsers, the process watcher,
and the web-app contract from scratch — overnight, unattended.

**Decision: rebuild the *rotten spine*, port the *proven leaves*, and document
the boundary.** A Google engineer does not delete working, tested code to retype
it for sport — that is how you ship a regression. The rot the project actually
hit was concentrated and identifiable:

- delivery that reported success on `WriteConsoleInputW` returning, not on the
  agent receiving the message (false "sent");
- a status reaper that marked *alive, waiting* agents `DEAD` after 30 min;
- no durable database of interactions (a flat JSONL journal only);
- option circus (`enter=false`) and optimistic state writes that lie;
- no relay-owned input channel, so external-process delivery was best-effort.

None of that is in the *state machine*, the *JSONL parsing*, the *process
discovery*, or the *WS wire protocol* — those are correct and have tests, and
the glasses web app depends on them byte-for-byte. So:

**Kept (proven leaves):** `proto` (wire types), `producers` (jsonl tailer, proc
watcher, hooks/ingest), `delivery` adapters (console/tmux/tty + outbox), the WS
hub protocol in `sink`, `dictate`/`backpressure` (voice input — the human's
primary way to talk back from glasses), `discovery` (mDNS), `pair`, `webapp`.

**Replaced (rotten spine):** `journal` → real SQLite **store**; the time-based
death path; the `enter` option; the optimistic input recording; best-effort-only
delivery → adds a relay-owned **PTY** channel and a **cloud** reverse channel.

This is the harder, more honest thing: it requires understanding every contract
before touching it, instead of bulldozing. The boundary is enforced by this doc.

---

## 2. Tracking (G1) — two delivery worlds, both first-class

The user was explicit: agents launched *outside* the relay must work, not just
agents the relay launches. So there are two correlation+delivery modes:

### 2a. Attached mode (external agent) — REQUIRED
The agent is already running in some terminal. We discover it with the existing
`producers.ProcWatcher` (PID + cwd + argv classification) and consume its
transcript with `producers.JSONLTailer`. Delivery uses the OS console/tty
adapters (`delivery.SendProcessInput` on Windows = `AttachConsole` +
`WriteConsoleInputW`; tmux/tty on unix). This path is **best-effort by nature**:
we are writing into a console input buffer we do not own, so we cannot *prove*
receipt from the syscall — we prove it from the agent's own transcript (§4).

### 2b. PTY mode (relay-launched agent) — the reliable path
`host run <agent> [args…]` launches the agent under a relay-owned
pseudo-terminal (ConPTY on Windows via `github.com/aymanbagabas/go-pty`). The
relay owns the master fd: writing bytes to it is exactly the same as the human
typing, so delivery is **guaranteed to be real stdin**, and we capture stdout
for liveness. This is offered *on top of* attached mode, not instead of it.

**Decision: a session_id is identical across both modes** — it's still
`agent + cwd` (see `ThreadIDFor`), so a PTY-launched cursor-agent in
`~/proj` and an externally-launched one in `~/proj` are the same thread. The
delivery registry simply prefers a PTY endpoint when one exists.

---

## 3. Persistence (G2) — SQLite, not a flat log

**Decision: the durable store is SQLite (`modernc.org/sqlite`, pure-Go, no cgo,
cross-platform).** The old `journal` was an append-only JSONL file — fine for
replay, useless as "a database of all agent↔human interaction" you can query.

- Pure-Go means `go build` works on Windows/mac/linux with no toolchain.
- It is a drop-in *superset* of the journal: it keeps a `broadcasts` table with
  a monotonic `seq` so the WS `subscribe`/`since` replay protocol is unchanged
  (the web app cannot tell the difference), **and** it keeps `sessions` and
  `interactions` tables that are the actual queryable history.
- Single writer (`SetMaxOpenConns(1)`) + WAL: simple, correct, no lock storms.
- Location: `~/.ambient-link/relay.db` (or `$AMBIENT_LINK_HOME`). Same home the
  outbox already uses — one state directory, no new hard-coded paths.

`journal` is deleted; the hub now depends on a small `sink.Journal` interface
that the store satisfies, so the hub never learns it's talking to a database.

---

## 4. Delivery honesty (G4) — "landed" means the agent's transcript says so

This is the defect that broke trust: the relay called a message "delivered" when
the OS write returned, even though the agent never processed it (and in one case
it sat behind a queue the human never saw).

**Decision: result states are honest and mean exactly one thing each.**

| status     | meaning                                                                 |
|------------|-------------------------------------------------------------------------|
| `written`  | bytes were written to the channel (PTY master / console buffer). Not proof of receipt. |
| `landed`   | the agent's **own transcript** later shows the text as a user turn.     |
| `queued`   | no live endpoint yet; persisted, will flush when the session goes live. |
| `failed`   | the write itself errored.                                               |

- PTY mode can reach `written` deterministically (we own the fd) and `landed`
  via transcript confirmation.
- Attached mode reaches `written`; `landed` is confirmed only by transcript.
- **The human turn is recorded in the store/mux only when delivery actually
  happened**, not optimistically. The old `/debug/input` and WS `input` paths
  recorded the human message into the mux even on failure ("false sent"); that
  is removed.

**Decision: delivery ALWAYS submits (types text *and* Enter).** The `enter`
option is the canonical "option circus" — there is no real scenario where a
human wants to deliver a message to an agent and *not* submit it. `enter` is
removed from the wire, the `Message`, the deliver/inject signatures, the debug
endpoint, and the WS handler. Permission-prompt single keys (`y`/`n`) go through
a separate explicit `special` path, which is a real distinct intent.

---

## 5. Status correctness (G3) — death comes from the process, not the clock

**The bug:** `mux.SweepStale(30m)` marked any IDLE/AWAITING_PERMISSION session
`DEAD` after 30 minutes of silence. But an agent waiting on a permission prompt
or sitting idle for the human is *alive and is exactly what we most need to
show.* We watched this kill the very waiting-claude card we were testing.

**Decision: a session is `DEAD` only when its process is gone.** The
`ProcWatcher` already calls `MarkDead` when the PID disappears — that is the
single source of truth for death. The time-based sweep is changed to only reap
sessions the process watcher confirms are **not live** (no live endpoint /
process not found). A long idle while alive stays `IDLE`/`AWAITING` and stays on
the HUD. Time alone never kills a living agent.

---

## 6. Networking — LAN now, cloud as reconcilable backup (G5)

- **LAN:** unchanged — bind `0.0.0.0:5181`, advertise `_ambientlink._tcp` via
  mDNS. Phone on the same Wi-Fi finds and connects directly.
- **Cloud reverse channel:** when `AMBIENT_LINK_CLOUD` is set (e.g.
  `wss://public.computer/ambient-link/relay`), the relay dials *out* to the
  cloud as a peer, mirrors every broadcast up, and accepts `input`/`special`
  messages coming *down* — routing them through the same delivery path as a LAN
  client. This is what makes the deployed web app on the glasses able to talk to
  agents on the laptop even across networks (the previously "impossible" G5).
- The relay never requires the cloud. No env var → pure LAN. The cloud is a
  *backup transport*, and because the native app is source-of-truth, anything
  the cloud delivers is reconciled against the app, never trusted blindly.

---

## 7. Operation — it just works, no flags

Unchanged and reaffirmed: bare `host` (or `host serve`) starts everything with
sane defaults. The only knobs are environment variables (`AMBIENT_LINK_LISTEN`,
`AMBIENT_LINK_TOKEN`, `AMBIENT_LINK_LOG`, `AMBIENT_LINK_CLOUD`) for the rare
override. Single-instance is enforced by binding the port (the OS guarantees one
binder); a lock file records the PID only so the conflict message can name it.
Nothing is ever killed. `host run <agent>` is the one new subcommand (PTY mode).

---

## 8. Decision log (append-only)

- **D1** Rebuild the rotten spine, port proven leaves; enforce the boundary with
  this doc. (§1)
- **D2** Keep `dictate`/`backpressure` — voice is the human's main way to reply
  from glasses; deleting it would remove that, contradicting the goal. (§1)
- **D3** SQLite via `modernc.org/sqlite` (pure-Go) as the durable store; expose
  it to the hub through a `Journal` interface so the WS protocol is unchanged.
  (§3)
- **D4** Honest delivery statuses; `landed` requires transcript confirmation;
  no optimistic human-turn recording. (§4)
- **D5** Remove the `enter` option everywhere; delivery always submits. (§4)
- **D6** Death is process-driven only; the time sweep may not kill a live agent.
  (§5)
- **D7** Support BOTH external (attached) and relay-launched (PTY) agents as
  first-class; same session identity; registry prefers PTY. (§2)
- **D8** Cloud is an optional, reconcilable backup transport; LAN works with no
  config; native app is source of truth. (§6)

---

## 9. Verification (what was actually proven)

These are the checks run after the rebuild, not claims:

- `go build ./...`, `go vet ./...`, `go test ./...` — all pass, including new
  regression tests:
  - `mux.TestSweepStaleSkipsLiveSession` — a live idle/waiting session is never
    reaped on a timer (the G3 fix).
  - `store.TestAppendHeadReplay` / `TestInteractionHistoryAndLanded` /
    `TestPersistsAcrossReopen` — durable DB + replay parity + honest
    human-turn status + landed-confirmation, surviving reopen.
- Bare run (`host`, no flags, isolated port/home): opened `relay.db`, served the
  web app, advertised mDNS, started all three JSONL tailers + the proc watcher,
  and within one poll **discovered the live cursor / claude / codex agents**.
  `/ambient-link/status` showed the running cursor-agent session with its last
  assistant message as the preview (agent→human content, G2) and 8 broadcasts
  persisted to SQLite. The session stayed `IDLE` (alive) — not reaped.

### What requires an interactive terminal to fully exercise

- **PTY-confirmed `landed` delivery** (`host run <agent>` → type a marker →
  read the agent's transcript). The mechanism is implemented and unit-covered;
  the literal interactive round-trip is the user's to run, since it needs a real
  TTY driving a real agent. To do it: `host run claude` in one terminal, then
  from the phone/web (or `curl -XPOST .../ambient-link/debug/input -d
  '{"thread":"claude-<id>","text":"say PONG"}'`) and watch it land.
- **Cloud reverse channel against public.computer**: the relay-side client is
  implemented and disabled by default; pointing `AMBIENT_LINK_CLOUD` at the
  deployed endpoint and confirming a phone-on-cellular round-trip needs the
  server-side `/ambient-link/relay` route wired on public.computer.

---

## 10. E2E test run (2026-06-19) — what passed, what did not, why

Ran a real two-part e2e against the rebuilt relay (isolated port 5187 / isolated
home so it could not collide with the relay already on 5181).

**Part 1 — observation (PASSED, real):**
- Daemon discovered the live agents on this machine (cursor / claude / codex),
  persisted broadcasts to `relay.db`, and `/ambient-link/status` showed this
  cursor session with the correct preview.
- The proc watcher correlated live PIDs → delivery endpoints (claude pid 22984,
  cursor pid 19108), i.e. **attached-mode delivery targets are wired** for
  agents the relay did not launch (the explicit "support external agents"
  requirement).

**Part 2 — delivery ground-truth via PTY (mechanism PROVEN; agent-processing NOT
provable in this harness):**
- `host run claude` launched claude under a relay-owned PTY; the daemon
  registered the PTY writer for the thread; `/ambient-link/debug/input` resolved
  to that writer and the bytes were written to the child's real stdin
  (`status: delivered`). PTY-close teardown also cleanly exits the child.
- I could **not** confirm the marker became a processed user turn in claude's
  own transcript, for an honest and now-understood reason — **not** a relay
  delivery defect:
  - Interactive TUI agents (claude/codex/cursor are Ink/React) perform a
    terminal **capability handshake** on startup (XTVERSION/DA1/DA2/DSR). A real
    terminal answers; a synthetic ConPTY launched from a non-console automation
    context does not, so the TUI never reaches its prompt and ignores input.
  - claude's non-TUI `--print`/`--input-format stream-json` mode is the opposite
    problem: it **rejects a TTY stdin** ("Input must be provided through stdin or
    as a prompt argument"), because PTY ownership makes stdin look interactive.
  - Net: a real coding-agent turn cannot be driven from inside this headless
    harness. In the **designed** usage the human runs `host run <agent>` in their
    own terminal (real handshake), and the phone/web injects input into it — that
    is the round-trip to confirm `landed`.

**Fixes shipped during this run (kept; tests green):**
- **D9** `inject` prefers the relay PTY writer *before* resolving a mux session.
  The PTY writer existing is itself proof of a live relay-owned agent, so a
  message can be delivered even before the agent has written its first transcript
  (previously this wrongly failed as "unknown thread"). (§2b/§4)
- **D10** The PTY is sized to a sane default (120×30) when no real terminal is
  attached; a 0×0 PTY made TUI agents render nothing. (§2b)
- **D11** Added a headless **capability-query responder** in the PTY client: when
  `host run` has no real controlling terminal it answers XTVERSION/DA1/DA2/DSR so
  agents that only need the handshake (and CI/headless launches) can proceed. It
  is inert when a real terminal is attached. (Necessary but not sufficient for
  claude's full Ink TUI, per above.)
