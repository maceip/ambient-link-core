// Package spawn starts new agent sessions in detached tmux panes — the
// simplest create-session that the relay can actually deliver into (tmux is
// delivery adapter #2, so a spawned session is immediately reachable).
package spawn

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/maceip/ambient-link-core/host/internal/delivery"
)

// yolo maps agent → the full command to run. The glasses flow has no way to
// answer permission prompts synchronously, so agents start in their
// skip-permissions ("YOLO") mode; replies still ride the normal delivery
// path. Overridable per agent via AMBIENT_LINK_SPAWN_<AGENT> (also how tests
// substitute a fake agent).
var yolo = map[string]string{
	"claude": "claude --dangerously-skip-permissions",
	"codex":  "codex --yolo",
	"cursor": "cursor-agent",
}

// Agent launches agent in a new detached tmux session rooted at cwd, with an
// optional initial prompt. Returns the tmux session name.
func Agent(agent, cwd, prompt string) (string, error) {
	agent = strings.ToLower(strings.TrimSpace(agent))
	cmd := strings.TrimSpace(os.Getenv("AMBIENT_LINK_SPAWN_" + strings.ToUpper(agent)))
	if cmd == "" {
		cmd = yolo[agent]
	}
	if cmd == "" {
		return "", fmt.Errorf("spawn: unsupported agent %q", agent)
	}
	dir, err := resolveCwd(cwd)
	if err != nil {
		return "", err
	}
	if prompt != "" {
		cmd += " " + shellQuote(prompt)
	}
	name := fmt.Sprintf("ambient-%s-%d", agent, time.Now().UnixMilli()%1_000_000)
	out, err := exec.Command(delivery.TmuxPath(), "new-session", "-d", "-s", name, "-c", dir, cmd).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("spawn: tmux: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return name, nil
}

func resolveCwd(cwd string) (string, error) {
	cwd = strings.TrimSpace(cwd)
	home, _ := os.UserHomeDir()
	switch {
	case cwd == "" || cwd == "~":
		cwd = home
	case strings.HasPrefix(cwd, "~/"):
		cwd = filepath.Join(home, cwd[2:])
	}
	st, err := os.Stat(cwd)
	if err != nil || !st.IsDir() {
		return "", fmt.Errorf("spawn: cwd %q is not a directory", cwd)
	}
	return cwd, nil
}

// shellQuote single-quotes s for the shell command tmux runs.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
