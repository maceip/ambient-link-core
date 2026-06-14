#!/usr/bin/env bash
set -euo pipefail
SESSION="${AMBIENT_CODEX_SESSION:-ambient-codex}"
CMD="${AMBIENT_CODEX_CMD:-codex}"
if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "[$SESSION] already running — attach with: tmux attach -t $SESSION"
else
  tmux new-session -d -s "$SESSION" -x 80 -y 24 "$CMD"
  tmux set-option -t "$SESSION" -g window-size manual
  echo "[$SESSION] started: $CMD"
fi
