package store

import (
	"testing"

	"github.com/maceip/ambient-link-core/host/internal/proto"
)

func openTemp(t *testing.T) *Store {
	t.Helper()
	t.Setenv("AMBIENT_LINK_HOME", t.TempDir())
	st, err := Open()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestAppendHeadReplay(t *testing.T) {
	st := openTemp(t)
	if st.Head() != 0 {
		t.Fatalf("empty head should be 0, got %d", st.Head())
	}
	seq1, err := st.Append(proto.Broadcast{Type: proto.BroadcastThreadStarted, Thread: "claude-x", SessionID: "s1", Agent: "claude", At: 100})
	if err != nil {
		t.Fatal(err)
	}
	seq2, _ := st.Append(proto.Broadcast{Type: proto.BroadcastThreadIdle, Thread: "claude-x", SessionID: "s1", Agent: "claude", LastAssistant: "done.", At: 200})
	if seq1 != 1 || seq2 != 2 || st.Head() != 2 {
		t.Fatalf("seqs=%d,%d head=%d", seq1, seq2, st.Head())
	}
	rows, err := st.ReplayAfter(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Type != proto.BroadcastThreadIdle || rows[0].LastAssistant != "done." {
		t.Fatalf("replay after 1: %+v", rows)
	}
	if all, _ := st.ReplayAfter(0); len(all) != 2 {
		t.Fatalf("replay all: %d", len(all))
	}
}

func TestInteractionHistoryAndLanded(t *testing.T) {
	st := openTemp(t)
	// Assistant turn arrives via a broadcast projection.
	_, _ = st.Append(proto.Broadcast{Type: proto.BroadcastThreadIdle, Thread: "codex-y", SessionID: "s2", Agent: "codex", LastAssistant: "ready", At: 10})
	// Human turn recorded honestly as written, later confirmed landed.
	if err := st.RecordHuman("s2", "codex-y", "codex", "run the tests", "delivered"); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkLanded("s2", "run the tests"); err != nil {
		t.Fatal(err)
	}
	hist, err := st.History("s2", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2 {
		t.Fatalf("want 2 interactions, got %d (%+v)", len(hist), hist)
	}
	if hist[0].Role != "assistant" || hist[0].Text != "ready" {
		t.Fatalf("assistant row: %+v", hist[0])
	}
	if hist[1].Role != "human" || hist[1].Status != "landed" {
		t.Fatalf("human row should be landed: %+v", hist[1])
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AMBIENT_LINK_HOME", dir)
	st, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = st.Append(proto.Broadcast{Type: proto.BroadcastThreadBusy, Thread: "t", SessionID: "s", At: 1})
	_ = st.RecordHuman("s", "t", "claude", "hi", "queued")
	_ = st.Close()

	st2, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if st2.Head() != 1 {
		t.Fatalf("head after reopen: %d", st2.Head())
	}
	hist, _ := st2.History("s", 10)
	if len(hist) != 1 || hist[0].Text != "hi" || hist[0].Status != "queued" {
		t.Fatalf("history after reopen: %+v", hist)
	}
}
