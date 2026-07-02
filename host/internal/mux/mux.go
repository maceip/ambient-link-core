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
		opt.IdleDebounce = 4 * time.Second
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
			Type: proto.BroadcastThreadStarted,
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

// SweepStale retires sessions that have gone silent AND whose process is no
// longer alive. Death is process-driven (the proc watcher MarkDead's vanished
// PIDs); this is only the catch-all for sessions the watcher can't see — e.g.
// a session reconstructed purely from a JSONL transcript whose process has
// already exited. It must NEVER kill a living agent: an agent idle or waiting on
// a permission prompt for hours is alive and is exactly what we most need to
// keep on the HUD (DECISIONS.md §5 — this fixes the bug that marked waiting
// agents DEAD on a 30-minute timer).
//
// isLive reports whether a session's process is currently observed alive (in
// production: the delivery registry has a live endpoint for it). When isLive is
// nil, nothing is swept — death then comes solely from the proc watcher.
func (m *Mux) SweepStale(maxIdle time.Duration, isLive func(sessionID string) bool) int {
	if isLive == nil {
		return 0
	}
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
	reaped := 0
	for _, id := range victims {
		if isLive(id) {
			continue // alive — never kill on a timer
		}
		m.MarkDead(id)
		reaped++
	}
	return reaped
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
			SessionID:        s.id,
			ThreadID:         s.threadID,
			Label:            s.label,
			Agent:            s.agent,
			CWD:              s.cwd,
			State:            s.state,
			LastEventAt:      s.lastEventAt,
			LastSource:       s.lastSource,
			Preview:          previewFor(s),
			Awaiting:         idleAwaitingFor(s),
			LastAssistant:    s.lastAssistant,
			LastUserInput:    s.lastUserInput,
			PermissionPrompt: s.lastPermissionPrompt,
		})
	}
	return out
}

// ApplyUpstream merges a laptop-mirrored broadcast into local mux state without
// re-emitting downstream (cloud relay uses this so /status matches the laptop).
func (m *Mux) ApplyUpstream(b proto.Broadcast) {
	if b.Type == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessionForUpstreamLocked(b)
	if s == nil {
		return
	}
	at := b.At
	if at == 0 {
		at = time.Now().UnixMilli()
	}
	if b.Label != "" {
		s.label = b.Label
	}
	if b.Agent != "" {
		s.agent = b.Agent
	}
	if b.CWD != "" {
		s.cwd = b.CWD
	}
	if b.LastAssistant != "" {
		s.lastAssistant = clip(b.LastAssistant, m.opt.MaxAssistantSnippet)
	}
	if b.LastUserInput != "" {
		s.lastUserInput = clip(b.LastUserInput, m.opt.MaxAssistantSnippet)
	}
	if b.PermissionPrompt != "" {
		s.lastPermissionPrompt = clip(b.PermissionPrompt, m.opt.MaxPermissionSnippet)
	}
	switch b.Type {
	case proto.BroadcastThreadStarted:
		if s.state == proto.StateDead || s.state == proto.StateStarting {
			s.state = proto.StateStarting
		}
	case proto.BroadcastThreadBusy:
		s.state = proto.StateBusy
	case proto.BroadcastThreadIdle, proto.BroadcastHudYank:
		if b.Awaiting == "permission" {
			s.state = proto.StateAwaitingPermission
		} else {
			s.state = proto.StateIdle
		}
	case proto.BroadcastThreadEnded:
		s.state = proto.StateDead
	}
	s.lastEventAt = at
}

// SyncSessions replaces cloud mux rows from a laptop relay_hello snapshot.
func (m *Mux) SyncSessions(views []SessionView) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, v := range views {
		if v.SessionID == "" || v.State == proto.StateDead {
			continue
		}
		ev := &proto.Event{
			SessionID:  v.SessionID,
			Agent:      v.Agent,
			CWD:        v.CWD,
			Type:       proto.EventSessionStart,
			Source:     proto.ProducerHooks,
			ObservedAt: v.LastEventAt,
		}
		s, _ := m.getOrCreateLocked(ev)
		if v.Label != "" {
			s.label = v.Label
		}
		if v.Agent != "" {
			s.agent = v.Agent
		}
		if v.CWD != "" {
			s.cwd = v.CWD
		}
		s.state = v.State
		if v.LastAssistant != "" {
			s.lastAssistant = clip(v.LastAssistant, m.opt.MaxAssistantSnippet)
		}
		if v.LastUserInput != "" {
			s.lastUserInput = clip(v.LastUserInput, m.opt.MaxAssistantSnippet)
		}
		if v.PermissionPrompt != "" {
			s.lastPermissionPrompt = clip(v.PermissionPrompt, m.opt.MaxPermissionSnippet)
		}
		if v.LastEventAt != 0 {
			s.lastEventAt = v.LastEventAt
		}
	}
}

func (m *Mux) sessionForUpstreamLocked(b proto.Broadcast) *session {
	if b.SessionID != "" {
		if s, ok := m.sessions[b.SessionID]; ok {
			return s
		}
	}
	if b.Thread != "" {
		if s := m.bestSessionLocked(b.Thread); s != nil {
			return s
		}
	}
	if b.SessionID == "" {
		return nil
	}
	ev := &proto.Event{
		SessionID:  b.SessionID,
		Agent:      b.Agent,
		CWD:        b.CWD,
		Type:       proto.EventSessionStart,
		Source:     proto.ProducerHooks,
		ObservedAt: b.At,
	}
	s, _ := m.getOrCreateLocked(ev)
	if b.Label != "" {
		s.label = b.Label
	}
	return s
}

// ThreadMeta is the per-thread row in the WS hello payload.
type ThreadMeta struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Agent string `json:"agent"`
}

// ThreadsHello returns one entry per live thread (non-DEAD sessions), using the
// most recently active session when multiple share a thread id.
func (m *Mux) ThreadsHello() []ThreadMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	best := make(map[string]*session)
	for _, s := range m.sessions {
		if s.state == proto.StateDead {
			continue
		}
		if cur, ok := best[s.threadID]; !ok || s.lastEventAt > cur.lastEventAt {
			best[s.threadID] = s
		}
	}
	out := make([]ThreadMeta, 0, len(best))
	for _, s := range best {
		out = append(out, ThreadMeta{ID: s.threadID, Label: s.label, Agent: s.agent})
	}
	return out
}

// YankForThread builds a hud_yank / thread_idle-shaped payload from live mux
// state for the given thread id. Returns false when the thread is unknown.
func (m *Mux) YankForThread(threadID string) (proto.Broadcast, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.bestSessionLocked(threadID)
	if s == nil {
		return proto.Broadcast{}, false
	}
	return m.yankFromSession(s), true
}

// SessionForThread returns the best live session for a thread id. Used by the
// inject layer to resolve thread_id → session_id + agent for delivery.
func (m *Mux) SessionForThread(threadID string) (sessionID, agent string, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.bestSessionLocked(threadID)
	if s == nil {
		return "", "", false
	}
	return s.id, s.agent, true
}

// IngestUserInput records a HUD chip / composer reply for a live thread.
func (m *Mux) IngestUserInput(threadID, text string) error {
	m.mu.Lock()
	s := m.bestSessionLocked(threadID)
	m.mu.Unlock()
	if s == nil {
		return fmt.Errorf("mux: unknown thread %q", threadID)
	}
	if err := m.Ingest(proto.Event{
		SessionID:  s.id,
		Agent:      s.agent,
		CWD:        s.cwd,
		Type:       proto.EventUserPrompt,
		Payload:    map[string]any{"message": text},
		Source:     proto.ProducerHooks,
		ObservedAt: time.Now().UnixMilli(),
	}); err != nil && err != ErrDuplicate {
		return err
	}
	return nil
}

func (m *Mux) bestSessionLocked(threadID string) *session {
	tset, ok := m.threads[threadID]
	if !ok || len(tset) == 0 {
		return nil
	}
	var best *session
	for sid := range tset {
		s := m.sessions[sid]
		if s == nil || s.state == proto.StateDead {
			continue
		}
		if best == nil || s.lastEventAt > best.lastEventAt {
			best = s
		}
	}
	return best
}

func (m *Mux) yankFromSession(s *session) proto.Broadcast {
	perm := ""
	awaiting := idleAwaitingFor(s)
	if s.state == proto.StateAwaitingPermission {
		perm = s.lastPermissionPrompt
	}
	return proto.Broadcast{
		Type:             proto.BroadcastHudYank,
		Thread:           s.threadID,
		Label:            s.label,
		Agent:            s.agent,
		LastAssistant:    s.lastAssistant,
		LastUserInput:    s.lastUserInput,
		Awaiting:         awaiting,
		PermissionPrompt: perm,
		At:               time.Now().UnixMilli(),
	}
}

// SessionView is the export shape for the REST /ambient-link/status snapshot.
// It carries the same glanceable content the WS yank cards do (preview /
// awaiting / permission prompt) so polling clients (glasses app, Wear, Apple)
// reach parity with the WS surfaces instead of seeing metadata only. Snippets
// are clipped by the mux's Max*Snippet bounds.
type SessionView struct {
	SessionID   string             `json:"session_id"`
	ThreadID    string             `json:"thread_id"`
	Label       string             `json:"label"`
	Agent       string             `json:"agent"`
	CWD         string             `json:"cwd"`
	State       proto.SessionState `json:"state"`
	LastEventAt int64              `json:"last_event_at"`
	LastSource  proto.ProducerName `json:"last_source"`

	// Glanceable content (was previously WS-only).
	Preview          string `json:"preview"`
	Awaiting         string `json:"awaiting"`
	LastAssistant    string `json:"last_assistant,omitempty"`
	LastUserInput    string `json:"last_user_input,omitempty"`
	PermissionPrompt string `json:"permission_prompt,omitempty"`
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
			Type: proto.BroadcastThreadBusy,
			Thread: s.threadID, SessionID: s.id, Label: s.label, Agent: s.agent, At: at,
		})
	case proto.StateAwaitingPermission:
		s.clearIdleTimer()
		m.broadcastLocked(proto.Broadcast{
			Type:             proto.BroadcastThreadIdle,
			Thread:           s.threadID,
			SessionID:        s.id,
			Label:            s.label,
			Agent:            s.agent,
			Awaiting:         "permission",
			PermissionPrompt: s.lastPermissionPrompt,
			LastAssistant:    s.lastAssistant,
			LastUserInput:    s.lastUserInput,
			At:               at,
		})
	case proto.StateIdle:
		s.clearIdleTimer()
		m.broadcastLocked(proto.Broadcast{
			Type:          proto.BroadcastThreadIdle,
			Thread:        s.threadID,
			SessionID:     s.id,
			Label:         s.label,
			Agent:         s.agent,
			Awaiting:      idleAwaitingFor(s),
			LastAssistant: s.lastAssistant,
			LastUserInput: s.lastUserInput,
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
		Label:         s.label,
		Agent:         s.agent,
		Awaiting:      idleAwaitingFor(s),
		LastAssistant: s.lastAssistant,
		LastUserInput: s.lastUserInput,
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
	lastUserInput         string
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
		if txt := stripRedactedSnippets(extractText(ev.Payload)); txt != "" {
			s.lastAssistant = clip(txt, opt.MaxAssistantSnippet)
			s.lastUserInput = ""
		}
		s.state = proto.StateBusy
	case proto.EventUserPrompt, proto.EventToolUse:
		if txt := stripRedactedSnippets(extractText(ev.Payload)); txt != "" {
			s.lastUserInput = clip(txt, opt.MaxAssistantSnippet)
		}
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

// stripRedactedSnippets drops Cursor transcript placeholder lines so HUD
// cards keep the last human-readable assistant text.
func stripRedactedSnippets(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) == "[REDACTED]" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(line)
	}
	return strings.TrimSpace(b.String())
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

// previewFor is the single glanceable line for a session: the pending permission
// prompt if one is blocking, otherwise the last assistant text, falling back to
// the last user input. Mirrors what the WS yank cards show.
func previewFor(s *session) string {
	if s.state == proto.StateAwaitingPermission && s.lastPermissionPrompt != "" {
		return s.lastPermissionPrompt
	}
	if s.lastAssistant != "" {
		return s.lastAssistant
	}
	return s.lastUserInput
}

// idleAwaitingFor classifies why the agent surfaced on the HUD.
func idleAwaitingFor(s *session) string {
	if s.state == proto.StateAwaitingPermission {
		return "permission"
	}
	if asksQuestion(s.lastAssistant) {
		return "question"
	}
	return "done"
}

func asksQuestion(text string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(text))
	if trimmed == "" {
		return false
	}
	if strings.HasSuffix(trimmed, "?") {
		return true
	}
	cues := []string{
		"should i", "would you like", "do you want", "shall i", "can you",
		"will you", "yes or no", "y/n", "tap yes", "ready?",
	}
	for _, c := range cues {
		if strings.Contains(trimmed, c) {
			return true
		}
	}
	return false
}
