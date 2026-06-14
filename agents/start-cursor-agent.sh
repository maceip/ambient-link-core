#!/usr/bin/env bash
# Start (or attach to) Cursor Agent CLI in tmux — reliable HUD delivery via send-keys.
set -euo pipefail
SESSION="${AMBIENT_CURSOR_SESSION:-ambient-cursor}"
CMD="${AMBIENT_CURSOR_CMD:-agent --yolo}"
if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "[$SESSION] already running — attach with: tmux attach -t $SESSION"
else
  tmux new-session -d -s "$SESSION" -x 80 -y 24 "$CMD"
  tmux set-option -t "$SESSION" -g window-size manual
  echo "[$SESSION] started: $CMD"
fi
