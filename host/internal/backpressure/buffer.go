// Package backpressure holds the vendor-neutral transit primitives extracted
// from the recovered Cosmo design (see ambient-link-google/glasses_link.md and
// ambient-link-core/ROUTING.md):
//
//   - EphemeralBuffer: a bounded, time-expiring buffer for captured/streamed
//     items (Cosmo's InMemoryEphemeralBuffer + getEphemeralBufferDurationMin).
//   - Throttle: a per-key leading-edge rate gate (Cosmo's FRAME_PROCESS_INTERVAL_MS
//     frame drop, GLASS_CAMERA_TARGET_FPS = 0.1).
//
// These are the Go realization of the contracts in ambient-link-core/contracts/.
// They keep memory flat and the WS transit lean under high-rate producers.
package backpressure

import (
	"sync"
	"time"
)

// EphemeralBuffer is a bounded, TTL-evicting FIFO. Items older than TTL (or in
// excess of Max) are dropped on Add and Snapshot. Safe for concurrent use.
type EphemeralBuffer[T any] struct {
	mu    sync.Mutex
	ttl   time.Duration
	max   int
	items []stamped[T]
}

type stamped[T any] struct {
	v  T
	at time.Time
}

// NewEphemeralBuffer returns a buffer that holds items for at most ttl and never
// retains more than max items. A non-positive max means "TTL only".
func NewEphemeralBuffer[T any](ttl time.Duration, max int) *EphemeralBuffer[T] {
	return &EphemeralBuffer[T]{ttl: ttl, max: max}
}

// TTL reports the configured retention window.
func (b *EphemeralBuffer[T]) TTL() time.Duration { return b.ttl }

// Add appends v stamped at `at`, then evicts expired/excess items.
func (b *EphemeralBuffer[T]) Add(v T, at time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.items = append(b.items, stamped[T]{v: v, at: at})
	b.evictLocked(at)
}

// Snapshot returns the live items (oldest first) as of `now`, evicting expired
// entries first. The returned slice is a copy.
func (b *EphemeralBuffer[T]) Snapshot(now time.Time) []T {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.evictLocked(now)
	out := make([]T, len(b.items))
	for i, it := range b.items {
		out[i] = it.v
	}
	return out
}

// Len returns the number of live items as of `now`.
func (b *EphemeralBuffer[T]) Len(now time.Time) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.evictLocked(now)
	return len(b.items)
}

// Clear drops everything.
func (b *EphemeralBuffer[T]) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.items = nil
}

func (b *EphemeralBuffer[T]) evictLocked(now time.Time) {
	if b.ttl > 0 {
		cutoff := now.Add(-b.ttl)
		i := 0
		for i < len(b.items) && b.items[i].at.Before(cutoff) {
			i++
		}
		if i > 0 {
			b.items = append(b.items[:0], b.items[i:]...)
		}
	}
	if b.max > 0 && len(b.items) > b.max {
		drop := len(b.items) - b.max
		b.items = append(b.items[:0], b.items[drop:]...)
	}
}
