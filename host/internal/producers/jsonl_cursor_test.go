package producers

import (
	"encoding/json"
	"testing"
)

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
