// Package sink implements the WS broadcaster — the downstream the SessionMux
// fans events to. Connected clients are typically the iOS / Android relay
// apps (and during dev, the glasses web app). The hub is concurrency-safe;
// individual client goroutines own their socket I/O and never block the mux.
package sink

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"nhooyr.io/websocket"

	"github.com/maceip/ambient-link-core/host/internal/mux"
	"github.com/maceip/ambient-link-core/host/internal/proto"
)

// Hub maintains the set of connected WS clients and broadcasts events to
// them. Implements mux.Sink.
type Hub struct {
	logger *slog.Logger

	mu      sync.RWMutex
	clients map[*client]struct{}

	// hello carries the snapshot used to greet new clients. The mux pushes
	// snapshots periodically (or on demand) via SetSnapshotSource.
	snapshotFn func() []mux.SessionView
}

// NewHub returns a fresh Hub. logger may be nil (defaults to slog.Default).
func NewHub(logger *slog.Logger) *Hub {
	if logger == nil {
		logger = slog.Default()
	}
	return &Hub{
		logger:  logger,
		clients: make(map[*client]struct{}),
	}
}

// SetSnapshotSource registers a function the hub calls to build the hello
// payload for newly-connected clients. Allowed to be nil.
func (h *Hub) SetSnapshotSource(fn func() []mux.SessionView) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.snapshotFn = fn
}

// Broadcast satisfies mux.Sink. Marshals once, fans out to all clients.
// Slow clients are dropped after a per-write timeout to keep the broadcast
// path lock-free for the mux.
func (h *Hub) Broadcast(b proto.Broadcast) {
	payload, err := json.Marshal(b)
	if err != nil {
		h.logger.Error("hub: marshal broadcast", "err", err, "type", b.Type)
		return
	}
	h.mu.RLock()
	targets := make([]*client, 0, len(h.clients))
	for c := range h.clients {
		targets = append(targets, c)
	}
	h.mu.RUnlock()

	for _, c := range targets {
		c.enqueue(payload)
	}
}

// ServeHTTP upgrades incoming HTTP connections to WS and registers them as
// hub clients. CORS / origin checks belong in the caller's middleware chain.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Mobile relays + glasses web app connect from a wide range of
		// origins. Auth is done via token in the upgrade URL, not origin.
		InsecureSkipVerify: true,
	})
	if err != nil {
		h.logger.Warn("hub: ws upgrade failed", "err", err)
		return
	}
	c := newClient(conn, h.logger)
	h.register(c)
	defer h.unregister(c)

	// Greet with current snapshot if available.
	h.mu.RLock()
	snapFn := h.snapshotFn
	h.mu.RUnlock()
	if snapFn != nil {
		hello, err := json.Marshal(helloMessage{
			Type:     "hello",
			Sessions: snapFn(),
			At:       time.Now().UnixMilli(),
		})
		if err == nil {
			c.enqueue(hello)
		}
	}

	c.run(r.Context())
}

func (h *Hub) register(c *client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	count := len(h.clients)
	h.mu.Unlock()
	h.logger.Info("hub: client connected", "total", count)
}

func (h *Hub) unregister(c *client) {
	h.mu.Lock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
	}
	count := len(h.clients)
	h.mu.Unlock()
	c.close()
	h.logger.Info("hub: client disconnected", "total", count)
}

// helloMessage is the greeting frame sent to newly-connected clients.
type helloMessage struct {
	Type     string             `json:"type"`
	Sessions []mux.SessionView  `json:"sessions"`
	At       int64              `json:"at"`
}

// ── per-client state ────────────────────────────────────────────────────

// writeTimeout bounds how long the hub will wait for a single frame to
// flush to a slow client before dropping that frame. Repeated drops cause
// the client's send queue to fill and the client to be closed.
const writeTimeout = 5 * time.Second

// sendQueueDepth caps the per-client backlog. Mobile relays normally process
// frames as fast as they arrive; a deep queue here usually means the client
// is gone but TCP hasn't noticed yet.
const sendQueueDepth = 64

type client struct {
	conn   *websocket.Conn
	logger *slog.Logger
	send   chan []byte
	done   chan struct{}
	once   sync.Once
}

func newClient(conn *websocket.Conn, log *slog.Logger) *client {
	return &client{
		conn:   conn,
		logger: log,
		send:   make(chan []byte, sendQueueDepth),
		done:   make(chan struct{}),
	}
}

func (c *client) enqueue(payload []byte) {
	select {
	case c.send <- payload:
	default:
		// Queue full → assume client is dead. Close will be picked up by run().
		c.logger.Warn("hub: client send queue full, closing")
		c.close()
	}
}

func (c *client) run(ctx context.Context) {
	// Reader goroutine: discards inbound frames (clients are read-only for
	// the host today) but services ping/pong and detects close.
	go func() {
		for {
			_, _, err := c.conn.Read(ctx)
			if err != nil {
				c.close()
				return
			}
		}
	}()

	for {
		select {
		case <-c.done:
			return
		case <-ctx.Done():
			c.close()
			return
		case payload, ok := <-c.send:
			if !ok {
				return
			}
			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := c.conn.Write(wctx, websocket.MessageText, payload)
			cancel()
			if err != nil {
				c.logger.Warn("hub: client write failed", "err", err)
				c.close()
				return
			}
		}
	}
}

func (c *client) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.conn.Close(websocket.StatusNormalClosure, "")
	})
}
