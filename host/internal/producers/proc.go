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
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maceip/ambient-link-core/host/internal/delivery"
	"github.com/maceip/ambient-link-core/host/internal/proto"
)

// Reaper is the subset of *mux.Mux the watcher needs.
type Reaper interface {
	MarkDead(sessionID string)
}

// TargetRegistrar receives live PID/session correlation for delivery adapters.
type TargetRegistrar interface {
	Set(ep delivery.Endpoint)
	Remove(sessionID string)
}

// LiveSession is the watcher-facing subset of mux session state. It is used as
// a Windows fallback where lsof-style open-file correlation is unavailable.
type LiveSession struct {
	SessionID string
	Agent     string
	CWD       string
	State     proto.SessionState
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
	// Registry receives pid/tty correlation for delivery. Optional.
	Registry TargetRegistrar
	// LiveSessions supplies current mux sessions for platform fallbacks that
	// cannot inspect process open files. Optional.
	LiveSessions func() []LiveSession
	// OnSessionLive is called after a session_id is correlated to a live PID.
	OnSessionLive func(sessionID string)
	Logger        *slog.Logger
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
		cfg.AgentNames = []string{"claude", "codex", "agent", "cursor-agent"}
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
	agentProcessCounts := make(map[string]int)
	for _, comm := range pids {
		if isAgentHelperProcess(comm) {
			continue
		}
		agentProcessCounts[normalizeAgentName(agentFromCommand(comm))]++
	}
	if useWindowsProcSupport && w.cfg.Logger != nil {
		for pid, comm := range pids {
			w.cfg.Logger.Info("proc: discovered", "pid", pid,
				"agent", agentFromCommand(comm), "helper", isAgentHelperProcess(comm), "comm", comm)
		}
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
			if w.cfg.Registry != nil {
				w.cfg.Registry.Remove(sid)
			}
		}
		delete(w.live, pid)
	}
	w.mu.Unlock()

	// Refresh PID → session set for live PIDs.
	for pid, comm := range pids {
		agent := agentFromCommand(comm)
		sessions := w.sessionsFor(ctx, pid)
		if len(sessions) == 0 && useWindowsProcSupport {
			// Background helpers (codex app-server / sandbox-setup / node_repl)
			// are not the interactive TUI we inject into — never register them.
			if isAgentHelperProcess(comm) {
				continue
			}
			// Primary: match by (agent type + working directory), which
			// disambiguates agents that share a parent dir (claude/codex both
			// in C:\Users\mac). Fallback: a single live session of this agent.
			sessions = w.sessionsForWindowsCWD(pid, agent)
			if len(sessions) == 0 {
				sessions = w.sessionsForUniqueLiveAgent(agent, agentProcessCounts[normalizeAgentName(agent)])
			}
		}
		if len(sessions) == 0 {
			continue
		}
		tty := ttyForPID(ctx, w.cfg.PsPath, pid)
		w.mu.Lock()
		w.live[pid] = sessions
		w.mu.Unlock()
		if w.cfg.Registry == nil {
			continue
		}
		for sid := range sessions {
			w.cfg.Registry.Set(delivery.Endpoint{
				SessionID: sid,
				PID:       pid,
				TTY:       tty,
				Agent:     agent,
			})
			if w.cfg.OnSessionLive != nil {
				w.cfg.OnSessionLive(sid)
			}
		}
	}
}

// listAgentPIDs returns pid → command for processes that look like coding agents.
func (w *ProcWatcher) listAgentPIDs(ctx context.Context) (map[int]string, error) {
	if useWindowsProcSupport {
		return w.listAgentPIDsWindows(ctx)
	}
	out, err := exec.CommandContext(ctx, w.cfg.PsPath, "-A", "-o", "pid=,command=").Output()
	if err != nil {
		return nil, err
	}
	result := make(map[int]string)
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		i := strings.IndexAny(line, " \t")
		if i < 0 {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(line[:i]))
		if err != nil {
			continue
		}
		cmd := strings.TrimSpace(line[i:])
		if looksLikeAgentProcess(cmd) {
			result[pid] = cmd
		}
	}
	return result, sc.Err()
}

func looksLikeAgentProcess(cmd string) bool {
	cmd = strings.ToLower(cmd)
	// Normalize path separators so Windows command lines (which use backslashes,
	// e.g. C:\Users\mac\.local\share\cursor-agent\...\node.exe) match the same
	// markers as posix ones. Without this, node-launched cursor-agent is never
	// recognized on Windows and its session gets no delivery endpoint.
	cmd = strings.ReplaceAll(cmd, `\`, "/")
	// macOS comm is truncated; match full command instead.
	if strings.Contains(cmd, "/cursor-agent/") || strings.Contains(cmd, " cursor-agent") {
		return true
	}
	if strings.Contains(cmd, "/.local/bin/agent") || strings.HasSuffix(cmd, " agent") {
		return true
	}
	for _, name := range []string{"claude", "codex"} {
		if strings.Contains(cmd, name) {
			return true
		}
	}
	return false
}

// sessionUUID matches the canonical "session id" segment in Claude Code's
// per-project JSONL paths: ~/.claude/projects/<sanitized-cwd>/<uuid>.jsonl
// or ~/.claude/projects/<sanitized-cwd>/<uuid>/...
var sessionUUID = regexp.MustCompile(`/\.claude/projects/[^/]+/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b`)

var codexSessionUUID = regexp.MustCompile(`([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$`)

// Cursor Agent CLI keeps session state under ~/.cursor/chats/<hash>/<uuid>/store.db
var cursorChatsSessionUUID = regexp.MustCompile(`/\.cursor/chats/[^/]+/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/store\.db`)

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
		if m := sessionUUID.FindStringSubmatch(path); m != nil {
			sessions[m[1]] = struct{}{}
			continue
		}
		if strings.Contains(path, "/.codex/sessions/") {
			if m := codexSessionUUID.FindStringSubmatch(path); m != nil {
				sessions[m[1]] = struct{}{}
			}
			continue
		}
		if m := cursorTranscriptDir.FindStringSubmatch(path); m != nil {
			base := strings.TrimSuffix(filepath.Base(path), ".jsonl")
			if base == m[1] {
				sessions[m[1]] = struct{}{}
			}
			continue
		}
		if m := cursorChatsSessionUUID.FindStringSubmatch(path); m != nil {
			sessions[m[1]] = struct{}{}
		}
	}
	return sessions
}

func (w *ProcWatcher) sessionsForUniqueLiveAgent(agent string, agentProcessCount int) map[string]struct{} {
	if w.cfg.LiveSessions == nil || agentProcessCount != 1 {
		return nil
	}
	want := normalizeAgentName(agent)
	if want == "" {
		return nil
	}
	var matches []LiveSession
	for _, s := range w.cfg.LiveSessions() {
		if s.SessionID == "" || s.State == proto.StateDead {
			continue
		}
		if normalizeAgentName(s.Agent) == want {
			matches = append(matches, s)
		}
	}
	if len(matches) != 1 {
		return nil
	}
	return map[string]struct{}{matches[0].SessionID: {}}
}

func ttyForPID(ctx context.Context, psPath string, pid int) string {
	out, err := exec.CommandContext(ctx, psPath, "-p", strconv.Itoa(pid), "-o", "tty=").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func agentBase(comm string) string {
	base := comm
	if slash := strings.LastIndexAny(comm, `/\`); slash >= 0 {
		base = comm[slash+1:]
	}
	if sp := strings.IndexAny(base, " \t"); sp > 0 {
		base = base[:sp]
	}
	return strings.Trim(base, `"'`)
}

func agentFromCommand(cmd string) string {
	low := strings.ToLower(cmd)
	if strings.Contains(low, "cursor-agent") || strings.Contains(low, "/.local/bin/agent") {
		return "cursor"
	}
	// Node-launched agents present as `node.exe <path>/codex.js ...` etc, so the
	// first token (node) is not the agent — scan the whole command line.
	if strings.Contains(low, "codex") {
		return "codex"
	}
	if strings.Contains(low, "claude") {
		return "claude"
	}
	base := normalizeAgentName(agentBase(cmd))
	switch base {
	case "claude", "codex", "cursor-agent", "agent":
		if base == "agent" || base == "cursor-agent" {
			return "cursor"
		}
		return base
	default:
		return base
	}
}

// isAgentHelperProcess reports whether a command line belongs to a background
// helper spawned by an agent (codex app-server / sandbox-setup / node_repl,
// MCP/language servers) rather than the interactive terminal TUI. Such
// processes must never be registered as a delivery endpoint.
func isAgentHelperProcess(cmd string) bool {
	low := strings.ToLower(cmd)
	for _, marker := range []string{
		"app-server",
		"sandbox-setup",
		"node_repl",
		"--listen",
		"mcp-server",
		"language-server",
	} {
		if strings.Contains(low, marker) {
			return true
		}
	}
	return false
}

// normalizeCWD canonicalizes a path for cross-form comparison: lowercased,
// forward slashes, drive letter stripped, no trailing slash. This lets a
// process cwd ("C:\Users\mac\ambient-link-meta") match a session cwd stored in
// posix form ("/Users/mac/ambient-link-meta").
func normalizeCWD(p string) string {
	p = strings.TrimSpace(strings.ToLower(p))
	if p == "" {
		return ""
	}
	p = strings.ReplaceAll(p, `\`, "/")
	if len(p) >= 2 && p[1] == ':' {
		p = p[2:]
	}
	p = strings.TrimRight(p, "/")
	return p
}

func normalizeAgentName(agent string) string {
	agent = strings.TrimSpace(strings.ToLower(agent))
	agent = strings.Trim(agent, `"'`)
	agent = strings.TrimSuffix(agent, ".exe")
	if agent == "cursor-agent" || agent == "agent" {
		return "cursor"
	}
	return agent
}
