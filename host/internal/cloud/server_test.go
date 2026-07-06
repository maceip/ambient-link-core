package cloud

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"nhooyr.io/websocket"
)

type recordingUpstream struct{ hellos atomic.Int64 }

func (r *recordingUpstream) ApplyUpstreamBroadcast(data []byte) {}
func (r *recordingUpstream) ApplyRelayHello(data []byte)        { r.hellos.Add(1) }

// Regression for B-105: a relay_hello above the websocket default read limit
// (32KB) must not kill the peer connection. Real laptops send hellos with
// dozens of sessions carrying multi-KB snippets.
func TestPeerServerAcceptsLargeRelayHello(t *testing.T) {
	up := &recordingUpstream{}
	p := NewPeerServer(up, slog.Default())
	srv := httptest.NewServer(p)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	sessions := make([]map[string]any, 12)
	for i := range sessions {
		sessions[i] = map[string]any{
			"session_id":     "00000000-0000-4000-8000-000000000000",
			"thread_id":      "claude-x",
			"agent":          "claude",
			"state":          "IDLE",
			"last_assistant": strings.Repeat("x", 4000),
		}
	}
	frame, _ := json.Marshal(map[string]any{"type": "relay_hello", "sessions": sessions})
	if len(frame) <= 32*1024 {
		t.Fatalf("test frame too small (%d bytes) to exercise the limit", len(frame))
	}
	if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	// Prove the connection survived the oversized frame: send a follow-up and
	// wait for the upstream to have received the hello.
	deadline := time.Now().Add(5 * time.Second)
	for up.hellos.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("relay_hello never reached upstream (connection killed?)")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"thread_busy","thread":"claude-x"}`)); err != nil {
		t.Fatalf("connection dead after large hello: %v", err)
	}
}
