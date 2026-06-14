// Package dictate orchestrates HUD voice-to-text sessions over the WS hub.
// Capture runs on clients (phone SpeechRecognizer, web SpeechRecognition, future
// on-device SODA per ~/neural/.../lib/soda/SodaSession.go). The host fans out
// partials and commits finals into the same input path as chip taps.
package dictate

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

// Handler commits finalized dictation text and fans out live UI frames.
type Handler struct {
	Logger *slog.Logger

	// Commit is called with the final transcript (typically mux.IngestUserInput + inject).
	Commit func(threadID, text string) error

	// Fanout broadcasts a JSON frame to all connected WS clients.
	Fanout func(payload []byte)
}

type session struct {
	thread string
	source string
	at     int64
}

// Sessions tracks one active dictation per thread.
type Sessions struct {
	mu   sync.Mutex
	byID map[string]session
	log  *slog.Logger
}

func NewSessions(log *slog.Logger) *Sessions {
	if log == nil {
		log = slog.Default()
	}
	return &Sessions{byID: make(map[string]session), log: log}
}

// Client message types (phone / web → host).
const (
	MsgBegin   = "dictate_begin"
	MsgPartial = "dictate_partial"
	MsgCommit  = "dictate_commit"
	MsgAbort   = "dictate_abort"
)

// Server message types (host → all clients).
const (
	EvActive  = "dictate_active"
	EvPartial = "dictate_partial"
	EvEnd     = "dictate_end"
)

type clientMsg struct {
	Type   string `json:"type"`
	Thread string `json:"thread"`
	Text   string `json:"text"`
	Source string `json:"source"` // "phone" | "web" | ""
}

func (h *Handler) Handle(sessions *Sessions, raw []byte) {
	var msg clientMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if msg.Thread == "" {
		return
	}
	switch msg.Type {
	case MsgBegin:
		sessions.begin(msg.Thread, msg.Source)
		h.emit(EvActive, msg.Thread, "", msg.Source)
	case MsgPartial:
		if msg.Text == "" {
			return
		}
		h.emit(EvPartial, msg.Thread, msg.Text, msg.Source)
	case MsgCommit:
		if msg.Text == "" {
			return
		}
		sessions.end(msg.Thread)
		if h.Commit != nil {
			if err := h.Commit(msg.Thread, msg.Text); err != nil && h.Logger != nil {
				h.Logger.Warn("dictate: commit failed", "thread", msg.Thread, "err", err)
			}
		}
		h.emit(EvEnd, msg.Thread, msg.Text, msg.Source)
	case MsgAbort:
		sessions.end(msg.Thread)
		h.emit(EvEnd, msg.Thread, "", msg.Source)
	}
}

func (s *Sessions) begin(thread, source string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if source == "" {
		source = "phone"
	}
	s.byID[thread] = session{thread: thread, source: source, at: time.Now().UnixMilli()}
}

func (s *Sessions) end(thread string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byID, thread)
}

func (h *Handler) emit(typ, thread, text, source string) {
	if h.Fanout == nil {
		return
	}
	payload, err := json.Marshal(map[string]any{
		"type":   typ,
		"thread": thread,
		"text":   text,
		"source": source,
		"at":     time.Now().UnixMilli(),
	})
	if err != nil {
		return
	}
	h.Fanout(payload)
}
