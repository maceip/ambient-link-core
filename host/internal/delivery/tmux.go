//go:build !windows

package delivery

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// SendTmuxPID types text into the tmux pane whose shell PID matches pid.
func SendTmuxPID(pid int, text string, enter bool) error {
	if pid <= 0 {
		return fmt.Errorf("delivery: invalid pid %d", pid)
	}
	target, err := tmuxTargetForPID(pid)
	if err != nil {
		return err
	}
	args := []string{"send-keys", "-t", target, text}
	if enter {
		args = append(args, "Enter")
	}
	out, err := exec.Command("tmux", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("delivery: tmux %v: %s", args, strings.TrimSpace(string(out)))
	}
	return nil
}

func tmuxTargetForPID(pid int) (string, error) {
	// Separator must be a plain printable char: when tmux runs without a
	// locale (launchd daemons have no LANG), it sanitizes control characters
	// in format output, turning a \t separator into "_" and breaking the
	// parse. Target by pane id (%N) — unambiguous and free of name parsing.
	out, err := exec.Command("tmux", "list-panes", "-a", "-F", "#{pane_pid} #{pane_id}").Output()
	if err != nil {
		return "", fmt.Errorf("delivery: tmux not available: %w", err)
	}
	want := strconv.Itoa(pid)
	sc := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, line := range sc {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			continue
		}
		if parts[0] == want {
			return strings.TrimSpace(parts[1]), nil
		}
	}
	return "", fmt.Errorf("delivery: pid %d not in any tmux pane", pid)
}
