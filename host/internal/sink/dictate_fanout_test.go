package sink_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"nhooyr.io/websocket"

	"github.com/maceip/ambient-link-core/host/internal/dictate"
	"github.com/maceip/ambient-link-core/host/internal/sink"
)

func TestWebSocketDictateFanout(t *testing.T) {
	hub := sink.NewHub(slog.Default())
	srv := httptest.NewServer(http.HandlerFunc(hub.ServeHTTP))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	read := func() map[string]any {
		t.Helper()
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return m
	}

	// hello
	if m := read(); m["type"] != "hello" {
		t.Fatalf("first frame = %v, want hello", m["type"])
	}

	send := func(v any) {
		t.Helper()
		b, _ := json.Marshal(v)
		if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	thread := "test-dictate-thread"
	text := "hello from go dictate test"

	send(map[string]any{"type": "subscribe", "since": map[string]any{}})
	send(map[string]any{"type": dictate.MsgBegin, "thread": thread, "source": "test"})
	send(map[string]any{"type": dictate.MsgPartial, "thread": thread, "text": "hello", "source": "test"})
	send(map[string]any{"type": dictate.MsgCommit, "thread": thread, "text": text, "source": "test"})

	seen := map[string]bool{}
	for len(seen) < 3 {
		m := read()
		typ, _ := m["type"].(string)
		seen[typ] = true
	}

	for _, want := range []string{dictate.EvActive, dictate.EvPartial, dictate.EvEnd} {
		if !seen[want] {
			t.Fatalf("missing %s in %#v", want, seen)
		}
	}
}
