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

	"github.com/maceip/ambient-link-core/host/internal/backpressure"
	"github.com/maceip/ambient-link-core/host/internal/dictate"
	"github.com/maceip/ambient-link-core/host/internal/inject"
	"github.com/maceip/ambient-link-core/host/internal/journal"
	"github.com/maceip/ambient-link-core/host/internal/mux"
	"github.com/maceip/ambient-link-core/host/internal/proto"
)

// MuxSource supplies hello thread rows, hud_yank enrichment, and HUD reply ingest.
type MuxSource interface {
	ThreadsHello() []mux.ThreadMeta
	YankForThread(threadID string) (proto.Broadcast, bool)
	IngestUserInput(threadID, text string) error
	SessionForThread(threadID string) (sessionID, agent string, ok bool)
}

// Hub maintains the set of connected WS clients and broadcasts events to
// them. Implements mux.Sink.
type Hub struct {
	logger *slog.Logger

	mu          sync.RWMutex
	clients     map[*client]struct{}
	mux         MuxSource
	journal     *journal.Journal
	bearerToken string
	relayDebug  bool
	dictate     *dictate.Sessions
	dictHandler dictate.Handler
}

// NewHub returns a fresh Hub. logger may be nil (defaults to slog.Default).
func NewHub(logger *slog.Logger) *Hub {
	if logger == nil {
		logger = slog.Default()
	}
	return &Hub{
		logger:  logger,
		clients: make(map[*client]struct{}),
		dictate: dictate.NewSessions(logger),
		dictHandler: dictate.Handler{
			Logger: logger,
			Fanout: nil, // wired on first ServeHTTP via setDictateFanout
			// Throttle the dictate_partial firehose to ~6.7 fps per thread
			// (Cosmo frame-interval lesson). begin/commit/abort bypass this and
			// reset the gate; commit carries full text so no data is lost.
			PartialThrottle: backpressure.NewThrottle(150 * time.Millisecond),
		},
	}
}

func (h *Hub) ensureDictate() {
	if h.dictHandler.Commit != nil {
		return
	}
	h.dictHandler.Logger = h.logger
	h.dictHandler.Commit = h.commitDictation
	h.dictHandler.Fanout = func(payload []byte) { h.fanout(nil, payload) }
}

func (h *Hub) commitDictation(threadID, text string) error {
	sessionID, _, ok := func() (string, string, bool) {
		h.mu.RLock()
		src := h.mux
		h.mu.RUnlock()
		if src == nil {
			return "", "", false
		}
		return src.SessionForThread(threadID)
	}()
	if err := inject.SendInput(threadID, text, true); err != nil {
		h.logger.Warn("dictate: inject failed", "thread", threadID, "err", err)
	} else if ok && sessionID != "" {
		h.logger.Info("dictate: delivered", "thread", threadID, "session", sessionID, "text", text)
	}
	h.mu.RLock()
	src := h.mux
	h.mu.RUnlock()
	if src == nil {
		return nil
	}
	return src.IngestUserInput(threadID, text)
}

// SetJournal wires durable broadcast replay for subscribe catch-up.
func (h *Hub) SetJournal(j *journal.Journal) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.journal = j
}

// SetMux wires live session state for hello snapshots and hud_yank enrichment.
func (h *Hub) SetMux(m MuxSource) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mux = m
}

// SetRelayDebug suppresses automatic session pushes (thread_idle/busy/…) to
// WS clients. Explicit hud_yank / debug yank still fan out. Mux keeps tracking.
func (h *Hub) SetRelayDebug(on bool) {
	h.mu.Lock()
	h.relayDebug = on
	h.mu.Unlock()
	if on {
		h.logger.Info("hub: relay debug — explicit cards only")
	}
}

func (h *Hub) relayDebugOn() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.relayDebug
}

func autoRelayBroadcast(typ proto.BroadcastType) bool {
	switch typ {
	case proto.BroadcastThreadIdle, proto.BroadcastThreadBusy,
		proto.BroadcastThreadStarted, proto.BroadcastThreadEnded:
		return true
	default:
		return false
	}
}

// SetBearerToken requires ?token= on WS upgrades when non-empty.
func (h *Hub) SetBearerToken(token string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.bearerToken = token
}

// Broadcast satisfies mux.Sink. Marshals once, fans out to all clients.
// Slow clients are dropped after a per-write timeout to keep the broadcast
// path lock-free for the mux.
func (h *Hub) Broadcast(b proto.Broadcast) {
	if b.At == 0 {
		b.At = time.Now().UnixMilli()
	}
	suppressFanout := h.relayDebugOn() && autoRelayBroadcast(b.Type)
	h.mu.RLock()
	j := h.journal
	h.mu.RUnlock()
	if j != nil {
		if _, err := j.Append(b); err != nil {
			h.logger.Warn("hub: journal append", "err", err)
		}
	}
	if suppressFanout {
		return
	}
	payload, err := json.Marshal(b)
	if err != nil {
		h.logger.Error("hub: marshal broadcast", "err", err, "type", b.Type)
		return
	}
	h.fanout(nil, payload)
}

// ServeHTTP upgrades incoming HTTP connections to WS and registers them as
// hub clients. CORS / origin checks belong in the caller's middleware chain.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	wantToken := h.bearerToken
	h.mu.RUnlock()
	if wantToken != "" && r.URL.Query().Get("token") != wantToken {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
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
	h.ensureDictate()
	h.register(c)
	defer h.unregister(c)

	if hello, ok := h.buildHello(); ok {
		c.enqueue(hello)
	}

	c.run(r.Context(), h)
}

func (h *Hub) buildHello() ([]byte, bool) {
	h.mu.RLock()
	src := h.mux
	h.mu.RUnlock()
	var threads []mux.ThreadMeta
	if src != nil {
		threads = src.ThreadsHello()
	}
	rows := make([]helloThread, 0, len(threads))
	for _, t := range threads {
		rows = append(rows, helloThread{ID: t.ID, Label: t.Label, Agent: t.Agent})
	}
	payload, err := json.Marshal(helloMessage{
		Type:       "hello",
		Threads:    rows,
		Cursor:     h.helloCursor(),
		RelayDebug: h.relayDebugOn(),
		At:         time.Now().UnixMilli(),
	})
	if err != nil {
		h.logger.Error("hub: marshal hello", "err", err)
		return nil, false
	}
	return payload, true
}

func (h *Hub) helloCursor() map[string]int64 {
	h.mu.RLock()
	j := h.journal
	h.mu.RUnlock()
	if j == nil {
		return map[string]int64{}
	}
	return map[string]int64{"journal": j.Head()}
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

func (h *Hub) fanout(except *client, payload []byte) {
	h.mu.RLock()
	targets := make([]*client, 0, len(h.clients))
	for c := range h.clients {
		if c != except {
			targets = append(targets, c)
		}
	}
	h.mu.RUnlock()
	for _, c := range targets {
		c.enqueue(payload)
	}
}

func (h *Hub) handleInbound(from *client, data []byte) {
	var msg struct {
		Type     string           `json:"type"`
		Thread   string           `json:"thread"`
		Text     string           `json:"text"`
		Enter    *bool            `json:"enter"`
		Key      string           `json:"key"`
		ClientID string           `json:"client_id"`
		Since    map[string]int64 `json:"since"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		h.logger.Warn("hub: bad client frame", "err", err)
		return
	}
	switch msg.Type {
	case "subscribe":
		h.replayJournal(from, msg.Since)
		return
	case "hud_yank":
		h.handleHudYank(from, msg.Thread)
	case "input":
		enter := true
		if msg.Enter != nil {
			enter = *msg.Enter
		}
		result, err := inject.SendInputResult(msg.Thread, msg.Text, enter, msg.ClientID)
		if err != nil {
			h.logger.Warn("hub: inject failed", "thread", msg.Thread, "err", err)
			h.sendInputStatus(from, inputStatusMessage{
				Type:   "input_status",
				ID:     msg.ClientID,
				Thread: msg.Thread,
				Status: "failed",
				Error:  err.Error(),
				At:     time.Now().UnixMilli(),
			})
			return
		}
		h.sendInputStatus(from, inputStatusMessage{
			Type:         "input_status",
			ID:           msg.ClientID,
			Thread:       result.ThreadID,
			SessionID:    result.SessionID,
			Status:       result.Status,
			PendingCount: result.PendingCount,
			Error:        result.Error,
			At:           time.Now().UnixMilli(),
		})
		h.mu.RLock()
		src := h.mux
		h.mu.RUnlock()
		if src != nil {
			if err := src.IngestUserInput(msg.Thread, msg.Text); err != nil {
				h.logger.Warn("hub: input ingest failed", "thread", msg.Thread, "err", err)
			} else {
				h.logger.Info("hub: input ingested", "thread", msg.Thread, "text", msg.Text)
			}
		} else {
			h.logger.Info("hub: input received", "thread", msg.Thread, "text", msg.Text)
		}
	case "special":
		if err := inject.SendSpecial(msg.Thread, msg.Key); err != nil {
			h.logger.Warn("hub: special inject failed", "thread", msg.Thread, "key", msg.Key, "err", err)
		}
	case dictate.MsgBegin, dictate.MsgPartial, dictate.MsgCommit, dictate.MsgAbort:
		h.ensureDictate()
		h.dictHandler.Handle(h.dictate, data)
	default:
		h.logger.Debug("hub: ignored client message", "type", msg.Type)
	}
}

func (h *Hub) sendInputStatus(to *client, msg inputStatusMessage) {
	if to == nil {
		return
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		h.logger.Warn("hub: input status marshal", "err", err)
		return
	}
	to.enqueue(payload)
}

func (h *Hub) replayJournal(to *client, since map[string]int64) {
	h.mu.RLock()
	j := h.journal
	h.mu.RUnlock()
	if j == nil || to == nil {
		return
	}
	after := int64(0)
	if since != nil {
		after = since["journal"]
	}
	rows, err := j.ReplayAfter(after)
	if err != nil {
		h.logger.Warn("hub: journal replay", "err", err)
		return
	}
	for _, b := range rows {
		if h.relayDebugOn() && autoRelayBroadcast(b.Type) {
			continue
		}
		payload, err := json.Marshal(b)
		if err != nil {
			continue
		}
		to.enqueue(payload)
	}
}

func (h *Hub) handleHudYank(from *client, threadID string) {
	if threadID == "" {
		return
	}
	h.mu.RLock()
	src := h.mux
	h.mu.RUnlock()
	if src == nil {
		return
	}
	yank, ok := src.YankForThread(threadID)
	if !ok {
		h.logger.Warn("hub: hud_yank unknown thread", "thread", threadID)
		return
	}
	payload, err := json.Marshal(yank)
	if err != nil {
		h.logger.Error("hub: marshal hud_yank", "err", err)
		return
	}
	if from != nil {
		from.enqueue(payload)
	}
	h.fanout(from, payload)
}

// helloMessage is the greeting frame sent to newly-connected clients.
type helloMessage struct {
	Type       string           `json:"type"`
	Threads    []helloThread    `json:"threads"`
	Cursor     map[string]int64 `json:"cursor"`
	RelayDebug bool             `json:"relay_debug,omitempty"`
	At         int64            `json:"at,omitempty"`
}

type helloThread struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Agent string `json:"agent"`
}

type inputStatusMessage struct {
	Type         string `json:"type"`
	ID           string `json:"id,omitempty"`
	Thread       string `json:"thread,omitempty"`
	SessionID    string `json:"session_id,omitempty"`
	Status       string `json:"status"`
	PendingCount int    `json:"pending_count,omitempty"`
	Error        string `json:"error,omitempty"`
	At           int64  `json:"at"`
}

// ── per-client state ────────────────────────────────────────────────────

const writeTimeout = 5 * time.Second
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
		c.logger.Warn("hub: client send queue full, closing")
		c.close()
	}
}

func (c *client) run(ctx context.Context, hub *Hub) {
	go func() {
		for {
			_, data, err := c.conn.Read(ctx)
			if err != nil {
				c.close()
				return
			}
			hub.handleInbound(c, data)
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
