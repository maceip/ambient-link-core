#!/usr/bin/env bash
# Start (or attach to) the tmux session the relay mirrors as the "claude" thread.
set -euo pipefail
SESSION="${FC_CLAUDE_SESSION:-fc-claude}"
CMD="${FC_CLAUDE_CMD:-claude --dangerously-skip-permissions}"
if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "[$SESSION] already running — attach with: tmux attach -t $SESSION"
else
  tmux new-session -d -s "$SESSION" -x 80 -y 24 "$CMD"
  tmux set-option -t "$SESSION" -g window-size manual
  echo "[$SESSION] started: $CMD"
fi
