# agents

The relay mirrors **tmux sessions** by name. So wiring up an agent = starting
that agent inside a tmux session whose name matches one of the entries in
`relay/etc/threads.json`.

These scripts are minimal helpers. Run them on whatever host runs the relay
(laptop/cloud/phone). They're idempotent — re-running attaches to the
existing session instead of creating a duplicate.

| Script | Session | Default agent |
|---|---|---|
| [`start-claude.sh`](start-claude.sh) | `fc-claude` | `claude --dangerously-skip-permissions` |
| [`start-codex.sh`](start-codex.sh) | `fc-codex` | `codex` |
| [`start-bash.sh`](start-bash.sh) | `fc-bash` | `bash` (raw shell, useful for debugging) |

Add more agents by:
1. Creating a tmux session with a unique name.
2. Appending `{ id, label, tmux, agent }` to `relay/etc/threads.json`.
3. `kill -HUP $(pgrep -f relay/server.mjs)` to hot-reload.
