package journal

import (
	"testing"

	"github.com/maceip/ambient-link-core/host/internal/proto"
)

func TestJournalAppendReplay(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	j, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	s1, err := j.Append(proto.Broadcast{Type: proto.BroadcastThreadStarted, Thread: "claude-abc", At: 1})
	if err != nil || s1 != 1 {
		t.Fatalf("append1: seq=%d err=%v", s1, err)
	}
	s2, err := j.Append(proto.Broadcast{Type: proto.BroadcastThreadIdle, Thread: "claude-abc", At: 2})
	if err != nil || s2 != 2 {
		t.Fatalf("append2: seq=%d err=%v", s2, err)
	}
	j2, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	if j2.Head() != 2 {
		t.Fatalf("head=%d", j2.Head())
	}
	replay, err := j2.ReplayAfter(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(replay) != 1 || replay[0].Type != proto.BroadcastThreadIdle {
		t.Fatalf("replay: %+v", replay)
	}
}
