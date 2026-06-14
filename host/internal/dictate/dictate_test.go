package dictate_test

import (
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/maceip/ambient-link-core/host/internal/dictate"
)

func TestHandlerBeginPartialCommit(t *testing.T) {
	var fanout [][]byte
	var committed string
	h := dictate.Handler{
		Logger: slog.Default(),
		Commit: func(threadID, text string) error {
			committed = threadID + ":" + text
			return nil
		},
		Fanout: func(payload []byte) { fanout = append(fanout, payload) },
	}
	s := dictate.NewSessions(nil)

	h.Handle(s, mustJSON(t, map[string]any{
		"type": "dictate_begin", "thread": "t1", "source": "web",
	}))
	h.Handle(s, mustJSON(t, map[string]any{
		"type": "dictate_partial", "thread": "t1", "text": "hello",
	}))
	h.Handle(s, mustJSON(t, map[string]any{
		"type": "dictate_commit", "thread": "t1", "text": "hello world", "source": "phone",
	}))

	if committed != "t1:hello world" {
		t.Fatalf("commit = %q", committed)
	}
	if len(fanout) != 3 {
		t.Fatalf("fanout frames = %d, want 3 (active, partial, end)", len(fanout))
	}
	last := map[string]any{}
	if err := json.Unmarshal(fanout[len(fanout)-1], &last); err != nil {
		t.Fatal(err)
	}
	if last["type"] != dictate.EvEnd {
		t.Fatalf("last event type = %v", last["type"])
	}
}

func TestHandlerAbort(t *testing.T) {
	var types []string
	h := dictate.Handler{
		Fanout: func(payload []byte) {
			var m map[string]any
			_ = json.Unmarshal(payload, &m)
			types = append(types, m["type"].(string))
		},
	}
	s := dictate.NewSessions(nil)

	h.Handle(s, mustJSON(t, map[string]any{"type": "dictate_begin", "thread": "t2"}))
	h.Handle(s, mustJSON(t, map[string]any{"type": "dictate_abort", "thread": "t2"}))

	if len(types) != 2 || types[0] != dictate.EvActive || types[1] != dictate.EvEnd {
		t.Fatalf("events = %v", types)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
