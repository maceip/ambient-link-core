// Package producers exposes POST /face-chat/ingest — virtual/manual glue only.
// Cursor Agent CLI is observed via JSONL + proc (+ hooks when installed).
package producers

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/maceip/ambient-link-core/host/internal/proto"
)

// IngestConfig configures the generic ingest endpoint.
type IngestConfig struct {
	Logger       *slog.Logger
	MaxBodyBytes int64
}

// NewIngest returns an http.Handler for POST /face-chat/ingest.
//
// Body shape:
//
//	{
//	  "session_id": "cursor-ambient-link-meta",
//	  "agent": "cursor",
//	  "cwd": "/Users/mac/ambient-link-meta",
//	  "event_type": "assistant_message",
//	  "payload": { "message": "…" }
//	}
func NewIngest(ing Ingester, cfg IngestConfig) http.Handler {
	if ing == nil {
		panic("producers: nil ingester")
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = 256 * 1024
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &ingestHandler{ing: ing, cfg: cfg}
}

type ingestHandler struct {
	ing Ingester
	cfg IngestConfig
}

func (h *ingestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.cfg.MaxBodyBytes))
	if err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	ev, err := parseIngest(raw)
	if err != nil {
		h.cfg.Logger.Warn("ingest: parse", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := h.ing.Ingest(ev); err != nil {
		h.cfg.Logger.Debug("ingest: mux", "err", err)
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseIngest(raw map[string]any) (proto.Event, error) {
	src, _ := raw["source"].(string)
	if src != "virtual" {
		return proto.Event{}, errString("ingest requires source=virtual (CLI agents use hooks/jsonl/proc)")
	}
	sid, _ := raw["session_id"].(string)
	if sid == "" {
		return proto.Event{}, errString("missing session_id")
	}
	agent, _ := raw["agent"].(string)
	if agent == "" {
		agent = "cursor"
	}
	cwd, _ := raw["cwd"].(string)
	et, _ := raw["event_type"].(string)
	if et == "" {
		et, _ = raw["type"].(string)
	}
	if et == "" {
		return proto.Event{}, errString("missing event_type")
	}
	t := proto.EventType(et)
	if !proto.ValidEventType(t) {
		return proto.Event{}, errString("unknown event_type: " + et)
	}
	at := int64(0)
	switch v := raw["observed_at"].(type) {
	case float64:
		at = int64(v)
	case json.Number:
		at, _ = v.Int64()
	}
	if at == 0 {
		at = time.Now().UnixMilli()
	}
	payload := raw["payload"]
	if payload == nil {
		payload = raw["text"]
	}
	return proto.Event{
		SessionID:  sid,
		Agent:      agent,
		CWD:        cwd,
		Type:       t,
		Payload:    payload,
		Source:     proto.ProducerHooks,
		ObservedAt: at,
	}, nil
}

type errString string

func (e errString) Error() string { return string(e) }

// PushAssistant is a helper for scripts pushing a single assistant turn.
func PushAssistant(ing Ingester, sessionID, agent, cwd, message string) error {
	return ing.Ingest(proto.Event{
		SessionID:  sessionID,
		Agent:      agent,
		CWD:        cwd,
		Type:       proto.EventAssistantMessage,
		Payload:    map[string]any{"message": message},
		Source:     proto.ProducerHooks,
		ObservedAt: time.Now().UnixMilli(),
	})
}

// ParseIngestURL returns true when path is the ingest endpoint.
func ParseIngestURL(path string) bool {
	return strings.TrimSuffix(path, "/") == "/face-chat/ingest"
}
