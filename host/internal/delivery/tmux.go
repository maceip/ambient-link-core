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
	out, err := exec.Command("tmux", "list-panes", "-a", "-F", "#{pane_pid}\t#{session_name}:#{pane_index}").Output()
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
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}
		if parts[0] == want {
			return parts[1], nil
		}
	}
	return "", fmt.Errorf("delivery: pid %d not in any tmux pane", pid)
}
