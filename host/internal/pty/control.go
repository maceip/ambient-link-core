package pty

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"nhooyr.io/websocket"

	"github.com/maceip/ambient-link-core/host/internal/delivery"
)

// ControlHandler is the daemon-side endpoint for `host run` clients. While a
// client is connected it registers a delivery.PTYWriter for its thread, so
// inject prefers the relay-owned PTY over console/tty adapters (DECISIONS §2b).
type ControlHandler struct {
	Logger *slog.Logger
	Token  string
}

func (h *ControlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log := h.Logger
	if log == nil {
		log = slog.Default()
	}
	if h.Token != "" && r.URL.Query().Get("token") != h.Token {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	thread := r.URL.Query().Get("thread")
	if thread == "" {
		http.Error(w, "thread required", http.StatusBadRequest)
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		log.Warn("pty: control upgrade failed", "err", err)
		return
	}
	writer := &wsWriter{conn: conn, ctx: r.Context()}
	delivery.RegisterPTY(thread, writer)
	log.Info("pty: control registered", "thread", thread)
	defer func() {
		delivery.RegisterPTY(thread, nil)
		_ = conn.Close(websocket.StatusNormalClosure, "")
		log.Info("pty: control closed", "thread", thread)
	}()
	// Block until the client disconnects; reads keep the conn live and detect
	// closure.
	for {
		if _, _, err := conn.Read(r.Context()); err != nil {
			return
		}
	}
}

// wsWriter implements delivery.PTYWriter by sending input frames down the
// control WS to the `host run` client, which types them into the PTY master.
type wsWriter struct {
	conn *websocket.Conn
	ctx  context.Context
	mu   sync.Mutex
}

func (w *wsWriter) WriteInput(text string, submit bool) error {
	payload, err := json.Marshal(map[string]any{"text": text, "submit": submit})
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()
	return w.conn.Write(ctx, websocket.MessageText, payload)
}
