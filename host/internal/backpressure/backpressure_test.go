package backpressure

import (
	"testing"
	"time"
)

func TestEphemeralBufferTTLEviction(t *testing.T) {
	b := NewEphemeralBuffer[int](10*time.Second, 0)
	base := time.Unix(0, 0)

	b.Add(1, base)
	b.Add(2, base.Add(3*time.Second))
	b.Add(3, base.Add(6*time.Second))

	// At t=8s nothing has expired (ttl=10s).
	if got := b.Snapshot(base.Add(8 * time.Second)); len(got) != 3 {
		t.Fatalf("t=8s len=%d want 3 (%v)", len(got), got)
	}
	// At t=12s item@0 (age 12s) is gone; @3s and @6s remain.
	got := b.Snapshot(base.Add(12 * time.Second))
	if len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("t=12s = %v want [2 3]", got)
	}
	// At t=20s everything is expired.
	if got := b.Snapshot(base.Add(20 * time.Second)); len(got) != 0 {
		t.Fatalf("t=20s len=%d want 0", len(got))
	}
}

func TestEphemeralBufferMaxCap(t *testing.T) {
	b := NewEphemeralBuffer[int](time.Hour, 3)
	base := time.Unix(0, 0)
	for i := 0; i < 6; i++ {
		b.Add(i, base.Add(time.Duration(i)*time.Millisecond))
	}
	got := b.Snapshot(base.Add(time.Second))
	if len(got) != 3 || got[0] != 3 || got[2] != 5 {
		t.Fatalf("cap=3 = %v want [3 4 5]", got)
	}
}

func TestEphemeralBufferClear(t *testing.T) {
	b := NewEphemeralBuffer[string](time.Hour, 0)
	now := time.Now()
	b.Add("a", now)
	b.Clear()
	if n := b.Len(now); n != 0 {
		t.Fatalf("after clear len=%d want 0", n)
	}
}

func TestThrottleLeadingEdge(t *testing.T) {
	th := NewThrottle(10 * time.Second)
	base := time.Unix(0, 0)

	if !th.Allow("k", base) {
		t.Fatal("first event must pass")
	}
	if th.Allow("k", base.Add(2*time.Second)) {
		t.Fatal("event within interval must be dropped")
	}
	if th.Allow("k", base.Add(9*time.Second)) {
		t.Fatal("still within interval, must drop")
	}
	if !th.Allow("k", base.Add(11*time.Second)) {
		t.Fatal("after interval must pass")
	}
}

func TestThrottlePerKeyIndependent(t *testing.T) {
	th := NewThrottle(10 * time.Second)
	base := time.Unix(0, 0)
	if !th.Allow("a", base) || !th.Allow("b", base) {
		t.Fatal("distinct keys must each pass first event")
	}
	if th.Allow("a", base.Add(time.Second)) {
		t.Fatal("key a within interval must drop")
	}
}

func TestThrottleResetPassesImmediately(t *testing.T) {
	th := NewThrottle(10 * time.Second)
	base := time.Unix(0, 0)
	th.Allow("k", base)
	th.Reset("k")
	if !th.Allow("k", base.Add(time.Second)) {
		t.Fatal("after Reset the next event must pass even within interval")
	}
}

func TestThrottleDisabled(t *testing.T) {
	th := NewThrottle(0)
	now := time.Now()
	for i := 0; i < 5; i++ {
		if !th.Allow("k", now) {
			t.Fatal("interval<=0 must always allow")
		}
	}
}
