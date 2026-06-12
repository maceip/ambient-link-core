// Package mux holds the canonical per-session state machine.
//
// Multiple Producers (HTTP-ingested hooks, JSONL file tailer, process tree
// watcher) observe the same underlying coding-agent activity. They all emit
// proto.Event values into Mux.Ingest. The mux:
//
//   1. Dedupes (SessionID, EventType) pairs that arrive from different
//      producers within DedupWindow. Producers can fire freely; we collapse.
//   2. Maintains a per-session aggregate (proto.SessionState + last-snippet
//      buffers). State changes are pure functions of the deduped event stream.
//   3. Emits a [proto.Broadcast] envelope to a single Sink whenever the
//      observable state of a session changes — never on no-op events.
//   4. Bounds memory: dedup table is time-windowed-GC'd; sessions cap is
//      enforced by reaping the oldest DEAD session.
//
// Invariants:
//   - The mux never trusts any single producer as authoritative.
//   - The mux is concurrency-safe: any goroutine may call Ingest.
//   - The mux never panics on bad input; malformed events are logged and
//     dropped via the configured Logger.
//   - All timestamps are unix-milliseconds; Mux uses time.Now().UnixMilli()
//     when an event has ObservedAt == 0.
//
// Coordinate with protocol/PROTOCOL.md when changing emitted shapes.
package mux

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/maceip/ambient-link-core/host/internal/proto"
)

// Sink is the broadcaster (typically the WS hub) that receives every state
// transition. Implementations must be safe for concurrent invocation.
type Sink interface {
	Broadcast(proto.Broadcast)
}

// SinkFunc adapts a plain function to the Sink interface.
type SinkFunc func(proto.Broadcast)

func (f SinkFunc) Broadcast(b proto.Broadcast) { f(b) }

// Options control mux behavior. The zero value of Options is a working
// default for production via [WithDefaults].
type Options struct {
	// DedupWindow collapses identical (session, type) events arriving from
	// different producers within this duration. Default 2.5s.
	DedupWindow time.Duration
	// IdleDebounce flips a BUSY session to IDLE when no further events
	// arrive within this duration. Default 2s.
	IdleDebounce time.Duration
	// MaxAssistantSnippet bounds the buffered "last assistant text" used for
	// peek-card display. Default 4096.
	MaxAssistantSnippet int
	// MaxPermissionSnippet bounds the buffered permission-prompt text.
	// Default 1024.
	MaxPermissionSnippet int
	// MaxSessions caps retained sessions; the oldest DEAD session is reaped
	// when this is exceeded. Default 256.
	MaxSessions int
	// GCInterval controls how often the dedup table is swept. Default 60s.
	GCInterval time.Duration
	// Logger receives structured warnings about dropped events, etc.
	// Default slog.Default().
	Logger *slog.Logger
}

// WithDefaults fills any zero-valued field of opt with its production
// default. Returns opt for chaining.
func WithDefaults(opt Options) Options {
	if opt.DedupWindow == 0 {
		opt.DedupWindow = 2500 * time.Millisecond
	}
	if opt.IdleDebounce == 0 {
		opt.IdleDebounce = 2 * time.Second
	}
	if opt.MaxAssistantSnippet == 0 {
		opt.MaxAssistantSnippet = 4096
	}
	if opt.MaxPermissionSnippet == 0 {
		opt.MaxPermissionSnippet = 1024
	}
	if opt.MaxSessions == 0 {
		opt.MaxSessions = 256
	}
	if opt.GCInterval == 0 {
		opt.GCInterval = 60 * time.Second
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	return opt
}

// Mux is the canonical session state machine. Construct with [New], drive
// with Ingest, observe transitions via the Sink passed at construction time.
type Mux struct {
	sink Sink
	opt  Options

	mu       sync.Mutex
	sessions map[string]*session  // sessionID → session
	threads  map[string]threadSet // threadID  → set of sessionIDs
	recent   map[string]int64     // dedup key → unix-ms last seen

	stop   chan struct{}
	stopWg sync.WaitGroup
}

type threadSet = map[string]struct{}

// New constructs a Mux. Call [Mux.Close] to stop the background GC ticker.
func New(sink Sink, opt Options) *Mux {
	if sink == nil {
		panic("mux: nil sink")
	}
	m := &Mux{
		sink:     sink,
		opt:      WithDefaults(opt),
		sessions: make(map[string]*session),
		threads:  make(map[string]threadSet),
		recent:   make(map[string]int64),
		stop:     make(chan struct{}),
	}
	m.stopWg.Add(1)
	go m.gcLoop()
	return m
}

// Close stops the background goroutine and clears outstanding idle timers.
// Calling Ingest after Close is safe but state will not be advanced.
func (m *Mux) Close() {
	select {
	case <-m.stop:
	default:
		close(m.stop)
	}
	m.stopWg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.sessions {
		s.clearIdleTimer()
	}
}

// Ingest is the single entry point for producers. Returns sentinel errors
// for observability but is never panic-throwing.
func (m *Mux) Ingest(ev proto.Event) error {
	if err := validate(&ev); err != nil {
		m.opt.Logger.Warn("mux: dropped event", "err", err, "session_id", ev.SessionID, "type", ev.Type)
		return err
	}
	if ev.ObservedAt == 0 {
		ev.ObservedAt = time.Now().UnixMilli()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.duplicateLocked(&ev) {
		return ErrDuplicate
	}

	s, isNew := m.getOrCreateLocked(&ev)
	before := s.state
	s.absorb(&ev, &m.opt)
	after := s.state

	if isNew {
		m.broadcastLocked(proto.Broadcast{
			Type:   proto.BroadcastThreadStarted,
			Thread: s.threadID, Label: s.label, Agent: s.agent, CWD: s.cwd,
			At: ev.ObservedAt,
		})
	}
	if after != before {
		m.emitTransitionLocked(s, after, ev.ObservedAt)
	}
	// Arm the idle debounce whenever we end up BUSY (whether this event
	// caused the transition or the session was already BUSY). A quiet
	// period flips it to inferred-IDLE; subsequent events reset the timer.
	if after == proto.StateBusy {
		m.armIdleLocked(s)
	}
	return nil
}

// SweepStale scans for sessions in IDLE / AWAITING_PERMISSION states that
// have not seen any event for at least maxIdle, and marks them DEAD. This
// is the defense-in-depth backup to the process watcher: even if PID-based
// death detection misses a vanished agent, sessions still get cleaned up
// after maxIdle of silence. Returns count marked.
func (m *Mux) SweepStale(maxIdle time.Duration) int {
	cutoff := time.Now().Add(-maxIdle).UnixMilli()
	var victims []string
	m.mu.Lock()
	for id, s := range m.sessions {
		if s.state == proto.StateDead || s.state == proto.StateBusy {
			continue
		}
		if s.lastEventAt < cutoff {
			victims = append(victims, id)
		}
	}
	m.mu.Unlock()
	for _, id := range victims {
		m.MarkDead(id)
	}
	return len(victims)
}

// MarkDead is an external signal (typically from the process watcher) that
// a session is definitely gone. Idempotent.
func (m *Mux) MarkDead(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.sessions[sessionID]
	if !ok || s.state == proto.StateDead {
		return
	}
	s.clearIdleTimer()
	s.state = proto.StateDead
	m.broadcastLocked(proto.Broadcast{
		Type: proto.BroadcastThreadEnded, Thread: s.threadID,
		SessionID: s.id, At: time.Now().UnixMilli(),
	})
}

// Snapshot returns a read-only view of all live sessions, in insertion order.
// Safe to call concurrently; result is a copy.
func (m *Mux) Snapshot() []SessionView {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SessionView, 0, len(m.sessions))
	for _, s := range m.sessions {
		out = append(out, SessionView{
			SessionID:   s.id,
			ThreadID:    s.threadID,
			Agent:       s.agent,
			CWD:         s.cwd,
			State:       s.state,
			LastEventAt: s.lastEventAt,
			LastSource:  s.lastSource,
		})
	}
	return out
}

// SessionView is the diagnostic-friendly export shape; never includes large
// buffered snippets.
type SessionView struct {
	SessionID   string             `json:"session_id"`
	ThreadID    string             `json:"thread_id"`
	Agent       string             `json:"agent"`
	CWD         string             `json:"cwd"`
	State       proto.SessionState `json:"state"`
	LastEventAt int64              `json:"last_event_at"`
	LastSource  proto.ProducerName `json:"last_source"`
}

// ThreadIDFor is the pure function that maps (agent, cwd) → thread id.
// Same input always yields the same id; exposed so producers and tests can
// agree on identity without going through Ingest.
func ThreadIDFor(agent, cwd string) string {
	if agent == "" {
		agent = "unknown"
	}
	h := sha256.Sum256([]byte(agent + "::" + cwd))
	return agent + "-" + hex.EncodeToString(h[:])[:10]
}

// ErrDuplicate is returned by Ingest when an event was collapsed into a
// prior identical one within DedupWindow. Not a programming error.
var ErrDuplicate = errors.New("mux: duplicate event collapsed")

// ── internals (all called with m.mu held unless noted) ──────────────────

func (m *Mux) duplicateLocked(ev *proto.Event) bool {
	// Streamed assistant chunks and user prompts each carry distinct content;
	// collapsing them would lose the latest snippet or hide multi-prompt turns.
	if ev.Type == proto.EventAssistantMessage || ev.Type == proto.EventUserPrompt {
		return false
	}
	key := ev.SessionID + "|" + string(ev.Type)
	last, ok := m.recent[key]
	now := time.Now().UnixMilli()
	if ok && now-last < m.opt.DedupWindow.Milliseconds() {
		return true
	}
	m.recent[key] = now
	return false
}

func (m *Mux) getOrCreateLocked(ev *proto.Event) (*session, bool) {
	if s, ok := m.sessions[ev.SessionID]; ok {
		return s, false
	}
	if len(m.sessions) >= m.opt.MaxSessions {
		m.reapOldestDeadLocked()
	}
	s := newSession(ev.SessionID, ev.Agent, ev.CWD)
	m.sessions[s.id] = s
	tset, ok := m.threads[s.threadID]
	if !ok {
		tset = make(threadSet)
		m.threads[s.threadID] = tset
	}
	tset[s.id] = struct{}{}
	return s, true
}

func (m *Mux) emitTransitionLocked(s *session, state proto.SessionState, at int64) {
	switch state {
	case proto.StateBusy:
		// Don't clear idle timer here; the post-transition arm in Ingest will
		// reset it after broadcasting.
		m.broadcastLocked(proto.Broadcast{
			Type: proto.BroadcastThreadBusy, Thread: s.threadID, SessionID: s.id, At: at,
		})
	case proto.StateAwaitingPermission:
		s.clearIdleTimer()
		m.broadcastLocked(proto.Broadcast{
			Type:             proto.BroadcastThreadIdle,
			Thread:           s.threadID,
			SessionID:        s.id,
			Awaiting:         "permission",
			PermissionPrompt: s.lastPermissionPrompt,
			LastAssistant:    s.lastAssistant,
			At:               at,
		})
	case proto.StateIdle:
		s.clearIdleTimer()
		m.broadcastLocked(proto.Broadcast{
			Type:          proto.BroadcastThreadIdle,
			Thread:        s.threadID,
			SessionID:     s.id,
			Awaiting:      "reply",
			LastAssistant: s.lastAssistant,
			At:            at,
		})
	case proto.StateDead:
		s.clearIdleTimer()
		m.broadcastLocked(proto.Broadcast{
			Type: proto.BroadcastThreadEnded, Thread: s.threadID, SessionID: s.id, At: at,
		})
	}
}

func (m *Mux) armIdleLocked(s *session) {
	s.clearIdleTimer()
	d := m.opt.IdleDebounce
	s.idleTimer = time.AfterFunc(d, func() { m.inferIdle(s.id) })
}

func (m *Mux) inferIdle(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[sessionID]
	if !ok || s.state != proto.StateBusy {
		return
	}
	s.state = proto.StateIdle
	m.broadcastLocked(proto.Broadcast{
		Type:          proto.BroadcastThreadIdle,
		Thread:        s.threadID,
		SessionID:     s.id,
		Awaiting:      "reply",
		LastAssistant: s.lastAssistant,
		Inferred:      true,
		At:            time.Now().UnixMilli(),
	})
}

func (m *Mux) reapOldestDeadLocked() {
	var oldest *session
	for _, s := range m.sessions {
		if s.state != proto.StateDead {
			continue
		}
		if oldest == nil || s.lastEventAt < oldest.lastEventAt {
			oldest = s
		}
	}
	if oldest == nil {
		return
	}
	delete(m.sessions, oldest.id)
	if tset, ok := m.threads[oldest.threadID]; ok {
		delete(tset, oldest.id)
		if len(tset) == 0 {
			delete(m.threads, oldest.threadID)
		}
	}
}

func (m *Mux) broadcastLocked(b proto.Broadcast) {
	// Release the lock before invoking the sink to avoid holding it while a
	// downstream WS writer fans out. Pattern: copy + invoke unlocked.
	m.mu.Unlock()
	defer m.mu.Lock()
	m.sink.Broadcast(b)
}

func (m *Mux) gcLoop() {
	defer m.stopWg.Done()
	t := time.NewTicker(m.opt.GCInterval)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			m.gcRecent()
		}
	}
}

func (m *Mux) gcRecent() {
	cutoff := time.Now().UnixMilli() - 4*m.opt.DedupWindow.Milliseconds()
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, ts := range m.recent {
		if ts < cutoff {
			delete(m.recent, k)
		}
	}
}

// RunUntilContext blocks until ctx is cancelled, then calls Close. Useful
// for main-goroutine lifetime management.
func (m *Mux) RunUntilContext(ctx context.Context) {
	<-ctx.Done()
	m.Close()
}

// ── pure session aggregate ──────────────────────────────────────────────

type session struct {
	id        string
	agent     string
	cwd       string
	threadID  string
	label     string
	state     proto.SessionState
	lastEvent int64

	lastEventAt           int64
	lastSource            proto.ProducerName
	lastAssistant         string
	lastPermissionPrompt string

	idleTimer *time.Timer
}

func newSession(id, agent, cwd string) *session {
	if agent == "" {
		agent = "unknown"
	}
	return &session{
		id:          id,
		agent:       agent,
		cwd:         cwd,
		threadID:    ThreadIDFor(agent, cwd),
		label:       fmt.Sprintf("%s: %s", agent, shortCWD(cwd)),
		state:       proto.StateStarting,
		lastEventAt: time.Now().UnixMilli(),
	}
}

func (s *session) absorb(ev *proto.Event, opt *Options) {
	s.lastEventAt = ev.ObservedAt
	s.lastSource = ev.Source

	switch ev.Type {
	case proto.EventSessionStart:
		if s.state == proto.StateStarting || s.state == proto.StateDead {
			s.state = proto.StateIdle
		}
	case proto.EventAssistantMessage:
		if txt := extractText(ev.Payload); txt != "" {
			s.lastAssistant = clip(txt, opt.MaxAssistantSnippet)
		}
		s.state = proto.StateBusy
	case proto.EventUserPrompt, proto.EventToolUse:
		s.state = proto.StateBusy
	case proto.EventPermissionPrompt:
		if txt := extractText(ev.Payload); txt != "" {
			s.lastPermissionPrompt = clip(txt, opt.MaxPermissionSnippet)
		}
		s.state = proto.StateAwaitingPermission
	case proto.EventStop:
		s.state = proto.StateIdle
	case proto.EventSessionEnd:
		s.state = proto.StateDead
	}
}

func (s *session) clearIdleTimer() {
	if s.idleTimer != nil {
		s.idleTimer.Stop()
		s.idleTimer = nil
	}
}

// ── validation + payload coercion ───────────────────────────────────────

func validate(ev *proto.Event) error {
	if ev == nil {
		return errors.New("nil event")
	}
	if ev.SessionID == "" {
		return errors.New("empty session_id")
	}
	if !proto.ValidEventType(ev.Type) {
		return fmt.Errorf("unknown event_type %q", ev.Type)
	}
	return nil
}

// extractText coerces the loose Payload shapes producers emit (string,
// {"text": ...}, {"content": [...]}) into a single string for buffering.
func extractText(p any) string {
	switch v := p.(type) {
	case nil:
		return ""
	case string:
		return v
	case map[string]any:
		if s, ok := v["text"].(string); ok && s != "" {
			return s
		}
		if s, ok := v["content"].(string); ok && s != "" {
			return s
		}
		if s, ok := v["message"].(string); ok && s != "" {
			return s
		}
		if m, ok := v["message"].(map[string]any); ok {
			return extractText(m)
		}
		if arr, ok := v["content"].([]any); ok {
			var parts []string
			for _, x := range arr {
				if c, ok := x.(map[string]any); ok && c["type"] == "text" {
					if s, ok := c["text"].(string); ok {
						parts = append(parts, s)
					}
				}
			}
			return strings.Join(parts, "\n")
		}
	}
	return ""
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func shortCWD(p string) string {
	if p == "" {
		return ""
	}
	home, _ := os.UserHomeDir()
	if home != "" && strings.HasPrefix(p, home) {
		return "~" + p[len(home):]
	}
	return p
}
