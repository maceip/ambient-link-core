// Process watcher producer.
//
// Periodically enumerates running `claude` / `codex` processes and correlates
// them to session IDs by looking at which session JSONL files each PID has
// open. When a PID disappears, every session_id we'd associated with it is
// immediately marked dead via mux.MarkDead — much faster than waiting for
// the time-based stale reaper.
//
// Implementation:
//   - Use `ps -A -o pid=,comm=` for portable process listing (Linux + macOS).
//   - Use `lsof -nP -p <pid> -Fn` for open-file enumeration (same).
//   - Match file paths against `~/.claude/projects/.../<session-uuid>.jsonl`
//     and extract the session UUID.
//   - Refresh PID → []session_id map every poll; PIDs gone from the live set
//     trigger MarkDead on their previously-observed sessions.
//
// We deliberately do NOT use gopsutil to keep the binary lean. `ps` + `lsof`
// are present on every realistic target OS.
package producers

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Reaper is the subset of *mux.Mux the watcher needs.
type Reaper interface {
	MarkDead(sessionID string)
}

// ProcConfig configures the watcher.
type ProcConfig struct {
	// PollInterval between scans. Default 5s.
	PollInterval time.Duration
	// AgentNames is the set of process names we consider. Default
	// {"claude", "codex"}.
	AgentNames []string
	// LsofPath / PsPath override the binaries the watcher exec's.
	LsofPath string
	PsPath   string
	Logger   *slog.Logger
}

// ProcWatcher watches process lifecycle and marks orphaned sessions dead.
type ProcWatcher struct {
	cfg  ProcConfig
	r    Reaper
	mu   sync.Mutex
	live map[int]map[string]struct{} // pid → set of session_ids previously observed
}

func NewProcWatcher(r Reaper, cfg ProcConfig) (*ProcWatcher, error) {
	if r == nil {
		return nil, errors.New("nil reaper")
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if len(cfg.AgentNames) == 0 {
		cfg.AgentNames = []string{"claude", "codex"}
	}
	if cfg.LsofPath == "" {
		cfg.LsofPath = "lsof"
	}
	if cfg.PsPath == "" {
		cfg.PsPath = "ps"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &ProcWatcher{cfg: cfg, r: r, live: make(map[int]map[string]struct{})}, nil
}

// Run polls until ctx is cancelled.
func (w *ProcWatcher) Run(ctx context.Context) error {
	t := time.NewTicker(w.cfg.PollInterval)
	defer t.Stop()
	// First sweep immediately to establish a baseline.
	w.sweep(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			w.sweep(ctx)
		}
	}
}

func (w *ProcWatcher) sweep(ctx context.Context) {
	pids, err := w.listAgentPIDs(ctx)
	if err != nil {
		w.cfg.Logger.Warn("proc: ps failed", "err", err)
		return
	}

	// Reap: any PID we had before that isn't in the current list — its
	// sessions are dead.
	w.mu.Lock()
	for pid, sessions := range w.live {
		if _, alive := pids[pid]; alive {
			continue
		}
		for sid := range sessions {
			w.r.MarkDead(sid)
		}
		delete(w.live, pid)
	}
	w.mu.Unlock()

	// Refresh PID → session set for live PIDs.
	for pid := range pids {
		sessions := w.sessionsFor(ctx, pid)
		if len(sessions) == 0 {
			continue
		}
		w.mu.Lock()
		w.live[pid] = sessions
		w.mu.Unlock()
	}
}

// listAgentPIDs returns pid → cmdline for every live process whose comm
// matches one of cfg.AgentNames.
func (w *ProcWatcher) listAgentPIDs(ctx context.Context) (map[int]string, error) {
	out, err := exec.CommandContext(ctx, w.cfg.PsPath, "-A", "-o", "pid=,comm=").Output()
	if err != nil {
		return nil, err
	}
	wanted := make(map[string]bool, len(w.cfg.AgentNames))
	for _, n := range w.cfg.AgentNames {
		wanted[n] = true
	}
	result := make(map[int]string)
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		// pid + whitespace + comm (may itself contain a path on Linux).
		i := strings.IndexAny(line, " \t")
		if i < 0 {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(line[:i]))
		if err != nil {
			continue
		}
		comm := strings.TrimSpace(line[i:])
		base := comm
		if slash := strings.LastIndex(comm, "/"); slash >= 0 {
			base = comm[slash+1:]
		}
		if wanted[base] {
			result[pid] = comm
		}
	}
	return result, sc.Err()
}

// sessionUUID matches the canonical "session id" segment in Claude Code's
// per-project JSONL paths: ~/.claude/projects/<sanitized-cwd>/<uuid>.jsonl
// or ~/.claude/projects/<sanitized-cwd>/<uuid>/...
var sessionUUID = regexp.MustCompile(`/\.claude/projects/[^/]+/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b`)

// sessionsFor returns the set of session UUIDs the given PID has open files
// against. Empty result is normal — agents only hold an open fd on the
// session JSONL they're actively writing to.
func (w *ProcWatcher) sessionsFor(ctx context.Context, pid int) map[string]struct{} {
	out, err := exec.CommandContext(ctx, w.cfg.LsofPath, "-nP", "-p", strconv.Itoa(pid), "-Fn").Output()
	if err != nil {
		// lsof returns non-zero on transient pid races; not a programming error.
		return nil
	}
	sessions := make(map[string]struct{})
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "n") {
			continue
		}
		path := line[1:]
		m := sessionUUID.FindStringSubmatch(path)
		if m == nil {
			continue
		}
		sessions[m[1]] = struct{}{}
	}
	return sessions
}
