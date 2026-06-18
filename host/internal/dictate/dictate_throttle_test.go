package dictate

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/maceip/ambient-link-core/host/internal/backpressure"
)

// A burst of partials inside the throttle window collapses to a single fanned-out
// partial, while begin/commit always pass — the Cosmo frame-interval lesson.
func TestPartialThrottleCollapsesBurst(t *testing.T) {
	var mu sync.Mutex
	counts := map[string]int{}
	h := &Handler{
		Fanout: func(p []byte) {
			var m map[string]any
			if err := json.Unmarshal(p, &m); err != nil {
				return
			}
			mu.Lock()
			counts[m["type"].(string)]++
			mu.Unlock()
		},
		Commit:          func(string, string) error { return nil },
		PartialThrottle: backpressure.NewThrottle(time.Second),
	}
	s := NewSessions(nil)
	send := func(v any) {
		b, _ := json.Marshal(v)
		h.Handle(s, b)
	}

	thread := "t1"
	send(map[string]any{"type": MsgBegin, "thread": thread})
	for i := 0; i < 50; i++ {
		send(map[string]any{"type": MsgPartial, "thread": thread, "text": "chunk"})
	}
	send(map[string]any{"type": MsgCommit, "thread": thread, "text": "final transcript"})

	mu.Lock()
	defer mu.Unlock()
	if counts[EvActive] != 1 {
		t.Fatalf("active=%d want 1", counts[EvActive])
	}
	if counts[EvPartial] != 1 {
		t.Fatalf("partial=%d want 1 (50-burst within 1s window must collapse)", counts[EvPartial])
	}
	if counts[EvEnd] != 1 {
		t.Fatalf("end=%d want 1 (commit must always pass)", counts[EvEnd])
	}
}

// With no throttle configured, every partial fans out (default-off compatibility).
func TestPartialNoThrottlePassesAll(t *testing.T) {
	var mu sync.Mutex
	partials := 0
	h := &Handler{
		Fanout: func(p []byte) {
			var m map[string]any
			_ = json.Unmarshal(p, &m)
			if m["type"] == EvPartial {
				mu.Lock()
				partials++
				mu.Unlock()
			}
		},
	}
	s := NewSessions(nil)
	send := func(v any) {
		b, _ := json.Marshal(v)
		h.Handle(s, b)
	}
	for i := 0; i < 5; i++ {
		send(map[string]any{"type": MsgPartial, "thread": "t", "text": "x"})
	}
	mu.Lock()
	defer mu.Unlock()
	if partials != 5 {
		t.Fatalf("partials=%d want 5 (nil throttle = no gating)", partials)
	}
}

// Each begin resets the gate so a new turn's first partial is never delayed.
func TestPartialThrottleResetsPerTurn(t *testing.T) {
	var mu sync.Mutex
	partials := 0
	h := &Handler{
		Fanout: func(p []byte) {
			var m map[string]any
			_ = json.Unmarshal(p, &m)
			if m["type"] == EvPartial {
				mu.Lock()
				partials++
				mu.Unlock()
			}
		},
		Commit:          func(string, string) error { return nil },
		PartialThrottle: backpressure.NewThrottle(time.Hour), // huge window
	}
	s := NewSessions(nil)
	send := func(v any) {
		b, _ := json.Marshal(v)
		h.Handle(s, b)
	}
	// Turn 1
	send(map[string]any{"type": MsgBegin, "thread": "t"})
	send(map[string]any{"type": MsgPartial, "thread": "t", "text": "a"})
	send(map[string]any{"type": MsgCommit, "thread": "t", "text": "a done"})
	// Turn 2 — begin must reset, so this partial passes despite the hour window.
	send(map[string]any{"type": MsgBegin, "thread": "t"})
	send(map[string]any{"type": MsgPartial, "thread": "t", "text": "b"})

	mu.Lock()
	defer mu.Unlock()
	if partials != 2 {
		t.Fatalf("partials=%d want 2 (one per turn after reset)", partials)
	}
}
