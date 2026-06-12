# Architecture — agent signal surfaces

This document maps the **full option space** for getting a live signal of
"my coding agent has something to say" into the phone-side daemon, across
every place the user might run a coding agent. Read this before adding any
new signal source.

## The fundamental fragmentation

A user's coding agent sessions are scattered across surfaces that don't
talk to each other:

```
                    [ user account at Anthropic / OpenAI / etc. ]
                                       │
   ┌──────────────┬──────────────┬─────┴──────┬──────────────┬───────────────┐
   ▼              ▼              ▼            ▼              ▼               ▼
 local CLI    cloud sandbox  web chats     web CC dash    mobile app    direct API
 on laptop    (CCR / CMA /   claude.ai/    claude.ai/     (Claude /     (your own
 ($ claude)   Workspaces)    chats         code           ChatGPT)      key)
                             chatgpt.com   chatgpt.com/
                                           codex
```

**There is no single API that lists "all my active sessions."** Anthropic's
public API exposes per-call messaging but no `GET /v1/conversations` for
direct-API users. OpenAI's Conversations API has the same gap (community
feedback ticket #1359530).

**The web side has two separate surfaces on each vendor**, and they don't
overlap:

| Vendor | Conversational chat UI | Coding-agent dashboard | Cross-visible? |
|---|---|---|---|
| Anthropic | `claude.ai/chats` | `claude.ai/code` (Claude Code Remote) | No — a CCR session shows in `/code` only; a `/chats` conversation shows in `/chats` only |
| OpenAI | `chatgpt.com` | `chatgpt.com/codex` (Workspace Agents / Codex) | Same pattern |

**Cross-surface visibility is poor across the whole matrix.** Specifically:

- A `claude` CLI session started on your laptop is **invisible in both
  `claude.ai/chats` and `claude.ai/code`**. Verified empirically
  2026-06-12: a fresh interactive `claude` session running on a laptop
  (authenticated to the user's account) did not appear in either web
  surface during the run. Local CLI sessions register no web-side
  conversation object.
- This kills the "single web observation point" hope: `claude.ai/code`
  only lists sessions that were *initiated through web/mobile-paired
  surfaces* (CCR, claude.ai/code-launched runs). Local terminal usage is
  on a separate signal path.
- A **Claude Code Remote (CCR) session** — one started from the mobile
  app or `claude.ai/code` itself — does show in `claude.ai/code` and in
  the Claude mobile app. It does **not** show in `claude.ai/chats`.
- A **claude.ai chat conversation** shows in `claude.ai/chats` and the
  chat tab of the Claude mobile app. It does **not** show in
  `claude.ai/code`.
- ChatGPT mobile sessions are visible in `chatgpt.com`; ChatGPT API calls
  are not.
- **Cloud-sandbox** sessions (Anthropic Managed Agents, OpenAI Workspace
  Agents) live in their own respective dashboards.
- **Direct API** calls are visible nowhere except your billing/usage
  dashboard.

This means any single signal source covers only a *slice* of the user's
agent activity. To cover the full picture we need multiple sources fanning
into the relay.

## Signal sources — full matrix

| Source | Sessions it can see | Auth | Latency | Permissions | Status | Verdict |
|---|---|---|---|---|---|---|
| **Claude Code CLI hooks** (`~/.claude/settings.json`, `http` handler) | All local `claude` sessions on machines where the user installs the config | none | <1s | filesystem write | documented, supported | **PRIMARY** |
| **Codex CLI hooks** (`hooks.json`, `command:` handler with `curl`) | All local `codex` sessions on configured machines | none | <1s | filesystem write | `command:` handler stable; `http:` handler parsed-and-skipped today | **PRIMARY** |
| **tmux pipe-pane wrapper** (current relay default) | Any CLI run inside the wrapped tmux session | none | <2s + idle-detect debounce | none | DIY, what we already have | **FALLBACK** for users who don't want to edit hook configs |
| **Direct ingest HTTP endpoint** on the relay (`POST /face-chat/ingest`) | Anything that can `curl` | shared bearer | <1s | none | our own — to be built | **GLUE** for any of the above |
| Anthropic Managed Agents webhooks | Only sessions launched through CMA harness | API key + signing secret | seconds | none | documented | parked — too specific to one Anthropic product |
| OpenAI webhooks | API-account events (batch, fine-tune); **not** ChatGPT/Codex session state | API key | n/a | none | documented but doesn't fire the events we care about | not useful |
| Claude Code Remote Control mobile push | Anthropic-internal channel from CLI → user's Claude mobile app | n/a | n/a | n/a | not exposed to third parties | unusable |
| Claude Android `NotificationListenerService` | Whatever notifications Claude Android happens to post, IF the user has Claude notifications enabled | system grant | <1s | special access | research probe at `phone-android/.../probe/` | OPT-IN bonus only — most users keep AI app notifications off |
| Accessibility service scraping (Claude / ChatGPT) | Any foreground UI text | accessibility grant | seconds | very heavy | Play Store policy risk | avoid |
| Browser extension (user-installed) | `claude.ai/chats` + `claude.ai/code` + `chatgpt.com` + `chatgpt.com/codex` — separate observers per surface since they don't overlap | user installs extension | <1s | Chrome perms | undocumented internal SSE; legitimate if user-installed | RESEARCH — covers web-only sessions, but four distinct surfaces to maintain |
| Reverse-engineered cookie replay of claude.ai/chatgpt.com | All web sessions on captured account | captured cookies | streaming | full-account risk, ToS violating | unofficial | NEVER |

## What we're actually building

Three concrete paths plus a glue point:

1. **`POST /face-chat/ingest`** on the relay — single HTTP endpoint that
   takes a JSON event and re-broadcasts it as if it came from a tmux
   thread idle. This is the entry point everything else hits.

2. **Claude Code hooks installer** — a small script that drops a hook
   config into `~/.claude/settings.json` that POSTs `Stop` and
   `Notification(matcher: permission_prompt)` events to
   `<relay>/face-chat/ingest`.

3. **Codex CLI hooks installer** — same, but using `command:` handlers
   that `curl` because Codex's `http:` handler isn't executed today.

4. **(Parked, available)** Browser extension for claude.ai / chatgpt.com
   when we want web-session coverage. Not in v1.

5. **(Parked, available)** Local `NotificationListenerService` opt-in for
   users who happen to have Claude Android notifications on. Already
   scaffolded in `phone-android/.../probe/`, not in default path.

## Coverage we get

With paths 1–3 installed, the user's coverage is:

- ✅ Every local `claude` session on every machine where they ran the
  installer
- ✅ Every local `codex` session on every machine where they ran the
  installer
- ✅ Sessions on cloud VMs / dev sandboxes — IF the user ran the
  installer there (relay endpoint is reachable from anywhere with
  internet)
- ❌ claude.ai web sessions (would need browser extension — path 4)
- ❌ chatgpt.com web sessions (same — path 4)
- ❌ Mobile-app initiated sessions where the user doesn't also have the
  CLI installed
- ❌ Anthropic Managed Agents cloud sandboxes (parked path)

This is acceptable v1 coverage: it handles the dominant "I run agents in
my terminal" case for both Anthropic and OpenAI ecosystems. Web and mobile
coverage layered in later as the option space matures.

## When in doubt

Add a new signal source by sending an `ingest` POST. Don't invent a
parallel transport. If the relay's protocol is wrong for some new source,
extend the relay, don't fork the daemon.
