#!/usr/bin/env bash
set -euo pipefail
SESSION="${FC_BASH_SESSION:-fc-bash}"
if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "[$SESSION] already running — attach with: tmux attach -t $SESSION"
else
  tmux new-session -d -s "$SESSION" -x 80 -y 24 bash
  tmux set-option -t "$SESSION" -g window-size manual
  echo "[$SESSION] started: bash"
fi
