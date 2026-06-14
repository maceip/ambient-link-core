package mux

import (
	"sync"
	"testing"
	"time"

	"github.com/maceip/ambient-link-core/host/internal/proto"
)

// captureSink records every broadcast for assertion. Concurrency-safe.
type captureSink struct {
	mu     sync.Mutex
	events []proto.Broadcast
}

func (c *captureSink) Broadcast(b proto.Broadcast) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, b)
}
func (c *captureSink) snapshot() []proto.Broadcast {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]proto.Broadcast, len(c.events))
	copy(out, c.events)
	return out
}
func (c *captureSink) byType(t proto.BroadcastType) []proto.Broadcast {
	out := []proto.Broadcast{}
	for _, b := range c.snapshot() {
		if b.Type == t {
			out = append(out, b)
		}
	}
	return out
}

func newTestMux(t *testing.T, opts Options) (*Mux, *captureSink) {
	t.Helper()
	sink := &captureSink{}
	m := New(sink, opts)
	t.Cleanup(m.Close)
	return m, sink
}

func ev(t proto.EventType, sid, agent, cwd string, payload any) proto.Event {
	return proto.Event{
		SessionID: sid, Agent: agent, CWD: cwd,
		Type: t, Payload: payload,
		Source: proto.ProducerHooks, ObservedAt: time.Now().UnixMilli(),
	}
}

func TestThreadIDIsStable(t *testing.T) {
	a := ThreadIDFor("claude", "/proj/x")
	b := ThreadIDFor("claude", "/proj/x")
	if a != b {
		t.Fatalf("expected stable thread id, got %q vs %q", a, b)
	}
	if a == ThreadIDFor("claude", "/proj/y") {
		t.Fatal("different cwd should yield different thread id")
	}
	if a == ThreadIDFor("codex", "/proj/x") {
		t.Fatal("different agent should yield different thread id")
	}
}

func TestSessionLifecycleTransitions(t *testing.T) {
	m, sink := newTestMux(t, Options{IdleDebounce: 50 * time.Millisecond})

	if err := m.Ingest(ev(proto.EventSessionStart, "s1", "claude", "/a", nil)); err != nil {
		t.Fatal(err)
	}
	if err := m.Ingest(ev(proto.EventUserPrompt, "s1", "claude", "/a", "hi")); err != nil {
		t.Fatal(err)
	}
	if err := m.Ingest(ev(proto.EventStop, "s1", "claude", "/a", nil)); err != nil {
		t.Fatal(err)
	}

	starts := sink.byType(proto.BroadcastThreadStarted)
	if len(starts) != 1 {
		t.Fatalf("want 1 thread_started, got %d", len(starts))
	}
	busies := sink.byType(proto.BroadcastThreadBusy)
	if len(busies) != 1 {
		t.Fatalf("want 1 thread_busy, got %d (%v)", len(busies), sink.snapshot())
	}
	idles := sink.byType(proto.BroadcastThreadIdle)
	if len(idles) < 1 {
		t.Fatalf("want >=1 thread_idle, got %d", len(idles))
	}
	if idles[0].Awaiting != "done" {
		t.Errorf("idle.awaiting = %q, want %q", idles[0].Awaiting, "done")
	}
}

func TestDedupeCollapsesIdenticalEvents(t *testing.T) {
	m, sink := newTestMux(t, Options{DedupWindow: 500 * time.Millisecond})

	// Two SessionStart events for the same session within the window should
	// collapse into one broadcast (the first one wins).
	_ = m.Ingest(ev(proto.EventSessionStart, "s2", "claude", "/p", nil))
	err := m.Ingest(ev(proto.EventSessionStart, "s2", "claude", "/p", nil))
	if err != ErrDuplicate {
		t.Fatalf("second ingest should return ErrDuplicate, got %v", err)
	}
	starts := sink.byType(proto.BroadcastThreadStarted)
	if len(starts) != 1 {
		t.Fatalf("want 1 thread_started after dedup, got %d", len(starts))
	}
}

func TestAssistantMessagesNotDeduped(t *testing.T) {
	// Two distinct assistant chunks within the window must BOTH advance
	// state (they don't carry the same logical info). Dedup only applies
	// to state-affecting transition events.
	m, sink := newTestMux(t, Options{DedupWindow: time.Second, IdleDebounce: time.Hour})

	_ = m.Ingest(ev(proto.EventAssistantMessage, "s3", "claude", "/p", map[string]any{"text": "alpha"}))
	_ = m.Ingest(ev(proto.EventAssistantMessage, "s3", "claude", "/p", map[string]any{"text": "beta"}))

	// state should be BUSY; no idle yet (debounce is 1h).
	snap := m.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("want 1 session, got %d", len(snap))
	}
	if snap[0].State != proto.StateBusy {
		t.Fatalf("want state=BUSY, got %s", snap[0].State)
	}
	// Two assistant chunks → one thread_busy broadcast (first transition
	// from STARTING→BUSY). The second chunk doesn't transition state.
	busies := sink.byType(proto.BroadcastThreadBusy)
	if len(busies) != 1 {
		t.Fatalf("want 1 thread_busy, got %d", len(busies))
	}
}

func TestIdleDebounceFiresInferredIdle(t *testing.T) {
	m, sink := newTestMux(t, Options{IdleDebounce: 30 * time.Millisecond})

	_ = m.Ingest(ev(proto.EventUserPrompt, "s4", "claude", "/p", nil))
	// Wait past the debounce.
	time.Sleep(100 * time.Millisecond)

	idles := sink.byType(proto.BroadcastThreadIdle)
	if len(idles) == 0 {
		t.Fatalf("want >=1 inferred-idle broadcast, got 0 (%v)", sink.snapshot())
	}
	if !idles[len(idles)-1].Inferred {
		t.Errorf("last idle should be Inferred=true")
	}
}

func TestPermissionPromptCapturesSnippet(t *testing.T) {
	m, sink := newTestMux(t, Options{IdleDebounce: time.Hour})

	_ = m.Ingest(ev(proto.EventPermissionPrompt, "s5", "claude", "/p",
		map[string]any{"message": "Allow git push to origin/main?"}))

	idles := sink.byType(proto.BroadcastThreadIdle)
	if len(idles) != 1 {
		t.Fatalf("want 1 awaiting-permission idle, got %d", len(idles))
	}
	if idles[0].Awaiting != "permission" {
		t.Errorf("awaiting = %q, want permission", idles[0].Awaiting)
	}
	if idles[0].PermissionPrompt != "Allow git push to origin/main?" {
		t.Errorf("PermissionPrompt = %q", idles[0].PermissionPrompt)
	}
}

func TestMarkDeadIdempotent(t *testing.T) {
	m, sink := newTestMux(t, Options{IdleDebounce: time.Hour})
	_ = m.Ingest(ev(proto.EventSessionStart, "s6", "claude", "/p", nil))
	m.MarkDead("s6")
	m.MarkDead("s6") // second call must not double-broadcast
	ends := sink.byType(proto.BroadcastThreadEnded)
	if len(ends) != 1 {
		t.Fatalf("want 1 thread_ended, got %d", len(ends))
	}
}

func TestMultiSourceConvergesOnSameSession(t *testing.T) {
	// Hooks and JSONL fire the same logical PermissionPrompt for the same
	// session within the dedup window. Mux must produce exactly one
	// awaiting-permission idle broadcast.
	m, sink := newTestMux(t, Options{DedupWindow: 500 * time.Millisecond, IdleDebounce: time.Hour})

	now := time.Now().UnixMilli()
	_ = m.Ingest(proto.Event{
		SessionID: "s7", Agent: "claude", CWD: "/p",
		Type: proto.EventPermissionPrompt, Payload: "Allow X?",
		Source: proto.ProducerHooks, ObservedAt: now,
	})
	_ = m.Ingest(proto.Event{
		SessionID: "s7", Agent: "claude", CWD: "/p",
		Type: proto.EventPermissionPrompt, Payload: "Allow X?",
		Source: proto.ProducerJSONL, ObservedAt: now + 100,
	})

	idles := sink.byType(proto.BroadcastThreadIdle)
	if len(idles) != 1 {
		t.Fatalf("multi-source same event should produce 1 broadcast, got %d", len(idles))
	}
}

func TestSweepStaleMarksDead(t *testing.T) {
	m, sink := newTestMux(t, Options{IdleDebounce: 20 * time.Millisecond})

	_ = m.Ingest(ev(proto.EventStop, "s8", "claude", "/p", nil))
	// Session is now IDLE with lastEventAt = now.
	// SweepStale(0) marks anything older than now as dead. With cutoff=now,
	// the just-ingested session shouldn't be reaped. So pass a negative
	// duration to make the cutoff in the future, ensuring it's reaped.
	n := m.SweepStale(-time.Millisecond)
	if n != 1 {
		t.Fatalf("want 1 reaped, got %d", n)
	}
	if got := sink.byType(proto.BroadcastThreadEnded); len(got) != 1 {
		t.Fatalf("want 1 thread_ended, got %d", len(got))
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	m, _ := newTestMux(t, Options{IdleDebounce: time.Hour})
	_ = m.Ingest(ev(proto.EventSessionStart, "s9", "claude", "/p", nil))
	a := m.Snapshot()
	if len(a) != 1 {
		t.Fatalf("want 1, got %d", len(a))
	}
	a[0].Agent = "MUTATED"
	b := m.Snapshot()
	if b[0].Agent == "MUTATED" {
		t.Fatal("snapshot must return a defensive copy")
	}
}

func TestRejectsUnknownEventType(t *testing.T) {
	m, sink := newTestMux(t, Options{IdleDebounce: time.Hour})
	err := m.Ingest(proto.Event{
		SessionID: "s10", Agent: "claude", CWD: "/p",
		Type: proto.EventType("not_a_real_type"),
	})
	if err == nil {
		t.Fatal("want error for unknown event_type")
	}
	if got := sink.snapshot(); len(got) != 0 {
		t.Fatalf("want 0 broadcasts after invalid ingest, got %d", len(got))
	}
}

func TestEmptySessionIDDropped(t *testing.T) {
	m, sink := newTestMux(t, Options{IdleDebounce: time.Hour})
	err := m.Ingest(ev(proto.EventSessionStart, "", "claude", "/p", nil))
	if err == nil {
		t.Fatal("want error for empty session_id")
	}
	if got := sink.snapshot(); len(got) != 0 {
		t.Fatalf("want 0 broadcasts, got %d", len(got))
	}
}

func TestConcurrentIngestSafe(t *testing.T) {
	// Sanity test for the mu protecting all state. Race detector catches
	// any unprotected mutation.
	m, _ := newTestMux(t, Options{IdleDebounce: time.Hour})
	const N = 200
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			sid := "concurrent-" + string(rune('a'+i%26))
			_ = m.Ingest(ev(proto.EventSessionStart, sid, "claude", "/p", nil))
			_ = m.Ingest(ev(proto.EventUserPrompt, sid, "claude", "/p", nil))
			_ = m.Ingest(ev(proto.EventStop, sid, "claude", "/p", nil))
		}(i)
	}
	wg.Wait()
	// Just assert no panic + at least one session created.
	if len(m.Snapshot()) == 0 {
		t.Fatal("expected sessions after concurrent ingest")
	}
}
