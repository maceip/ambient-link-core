package producers

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/maceip/ambient-link-core/host/internal/proto"
)

type captureIngester struct {
	mu     sync.Mutex
	events []proto.Event
}

func (c *captureIngester) Ingest(ev proto.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, ev)
	return nil
}

func (c *captureIngester) snapshot() []proto.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]proto.Event, len(c.events))
	copy(out, c.events)
	return out
}

func TestCursorSessionIDFromPath(t *testing.T) {
	p := "/Users/mac/.cursor/projects/Users-mac-ambient-link-meta/agent-transcripts/f2ad0336-6f17-4cad-bab8-3c07f2d89b39/f2ad0336-6f17-4cad-bab8-3c07f2d89b39.jsonl"
	got := cursorSessionIDFromPath(p)
	want := "f2ad0336-6f17-4cad-bab8-3c07f2d89b39"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	sub := p[:len(p)-5] + "/subagents/x.jsonl"
	if cursorSessionIDFromPath(sub) != "" {
		t.Fatal("subagent path should not match main transcript")
	}
}

func TestClassifyCursor(t *testing.T) {
	raw := json.RawMessage(`{"content":[{"type":"text","text":"hello"}]}`)
	et, payload := classifyCursor(&cursorRecord{Role: "assistant", Message: raw})
	if et == "" {
		t.Fatal("expected event type")
	}
	m, ok := payload.(map[string]any)
	if !ok || m["message"] != "hello" {
		t.Fatalf("payload: %#v", payload)
	}
}

func TestCursorMessageTextSkipsRedacted(t *testing.T) {
	raw := json.RawMessage(`{"content":[{"type":"text","text":"[REDACTED]"},{"type":"tool_use"},{"type":"text","text":"ship it"}]}`)
	if got := cursorMessageText(raw); got != "ship it" {
		t.Fatalf("got %q want %q", got, "ship it")
	}
	if got := cursorMessageText(json.RawMessage(`{"content":[{"type":"text","text":"[REDACTED]"}]}`)); got != "" {
		t.Fatalf("redacted-only should be empty, got %q", got)
	}
}

func TestCursorCWDFromPath(t *testing.T) {
	p := "/Users/mac/.cursor/projects/Users-mac-ambient-link-meta/agent-transcripts/u/u.jsonl"
	if cwd := cursorCWDFromPath(p); cwd != "/Users/mac/ambient-link-meta" {
		t.Fatalf("cwd=%q", cwd)
	}
}

func TestCodexStartupReplayPrimesCWDFromHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rollout-2026-06-15T00-00-00-019ecb71-c5ab-7ef2-b634-dfb2898a00bf.jsonl")
	header := `{"timestamp":"2026-06-15T00:00:00Z","type":"session_meta","payload":{"cwd":"C:\\Users\\mac\\ambient-link-meta"}}`
	filler := `{"timestamp":"2026-06-15T00:00:01Z","type":"event_msg","payload":{"type":"ignored"}}`
	tail := `{"timestamp":"2026-06-15T00:00:02Z","type":"event_msg","payload":{"type":"user_message","message":"hello"}}`
	if err := os.WriteFile(path, []byte(header+"\n"+filler+"\n"+tail+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	ing := &captureIngester{}
	tailer := &JSONLTailer{
		cfg: JSONLConfig{
			Format:               FormatCodex,
			Agent:                "codex",
			PollFallbackInterval: time.Millisecond,
			FileIdleClose:        2 * time.Millisecond,
			Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		ing:  ing,
		open: map[string]*fileTail{},
	}
	tailer.wg.Add(1)
	tailer.tailLoop(context.Background(), &fileTail{
		path:        path,
		replayBytes: int64(len(tail) + 2),
		done:        make(chan struct{}),
	})

	events := ing.snapshot()
	if len(events) != 1 {
		t.Fatalf("events=%d want 1: %+v", len(events), events)
	}
	if events[0].CWD != `C:\Users\mac\ambient-link-meta` {
		t.Fatalf("cwd=%q", events[0].CWD)
	}
}
