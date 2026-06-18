package backpressure

import (
	"sync"
	"time"
)

// Throttle is a per-key leading-edge rate gate. The first Allow for a key
// passes; subsequent Allows within Interval are dropped. This is the direct
// analogue of Cosmo's frame gate:
//
//	if (tsMillis - lastFrameAt < FRAME_PROCESS_INTERVAL_MS) return  // drop
//
// Use it to drop intermediate high-rate frames (camera frames, dictation
// partials) while letting the meaningful edges through (caller resets the key
// on begin/commit so the next turn's first frame is never delayed).
//
// Safe for concurrent use. An Interval <= 0 disables throttling (Allow always
// returns true), so callers can wire it in unconditionally.
type Throttle struct {
	mu       sync.Mutex
	interval time.Duration
	last     map[string]time.Time
}

// NewThrottle returns a Throttle with the given minimum interval between
// allowed events per key.
func NewThrottle(interval time.Duration) *Throttle {
	return &Throttle{interval: interval, last: make(map[string]time.Time)}
}

// Allow reports whether an event for key at time `at` may pass. When it returns
// true it records `at` as the last-allowed time for key.
func (t *Throttle) Allow(key string, at time.Time) bool {
	if t == nil || t.interval <= 0 {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	last, ok := t.last[key]
	if ok && at.Sub(last) < t.interval {
		return false
	}
	t.last[key] = at
	return true
}

// Reset forgets key so its next Allow passes immediately. Call on a logical
// boundary (e.g. a new dictation turn or capture start).
func (t *Throttle) Reset(key string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	delete(t.last, key)
	t.mu.Unlock()
}
