# agents

Helper scripts to start `claude`, `codex`, or a raw shell inside named tmux
sessions. Run them on whatever machine runs the Go host (`ambient-link-host`).

The production host watches agents via **hooks + JSONL tailers + process
watcher** (see [`ARCHITECTURE.md`](../ARCHITECTURE.md)). These tmux helpers are
for manual runs and local debugging — not required for normal hook-based setup.

| Script | Session | Default agent |
|---|---|---|
| [`start-claude.sh`](start-claude.sh) | `ambient-claude` | `claude --dangerously-skip-permissions` |
| [`start-codex.sh`](start-codex.sh) | `ambient-codex` | `codex` |
| [`start-bash.sh`](start-bash.sh) | `ambient-bash` | `bash` (raw shell, useful for debugging) |

Scripts are idempotent — re-running attaches to the existing session instead
of creating a duplicate.
