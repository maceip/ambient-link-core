// Package producers implements the parallel signal producers that feed the
// SessionMux. This file holds the HTTP hooks ingest: Claude Code and Codex
// CLIs POST structured lifecycle events to known paths, which we translate
// into proto.Event values and hand to the mux.
//
// Schema notes: Anthropic's Claude Code hooks documentation specifies fields
// like session_id, cwd, hook_event_name, tool_name, tool_input, message,
// etc. Codex CLI's command-handler hooks follow a similar shape. Field
// parsing is permissive: missing fields fall back to derived values, never
// throw.
package producers

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/maceip/ambient-link-core/host/internal/delivery"
	"github.com/maceip/ambient-link-core/host/internal/proto"
)

// Ingester is the subset of *mux.Mux the hooks producer needs. Lets us
// inject a fake in tests.
type Ingester interface {
	Ingest(proto.Event) error
}

// HooksConfig configures the HTTP ingest endpoints.
type HooksConfig struct {
	// BearerToken, if non-empty, must match the Authorization header on
	// every inbound POST. Constant-time compare. Generated at install time;
	// the same token is written into the user's hook config.
	BearerToken string
	// MaxBodyBytes caps each request body. Default 256 KiB.
	MaxBodyBytes int64
	Logger       *slog.Logger
	// Outbox is drained on hook events to deliver HUD replies bidirectionally.
	Outbox *delivery.Outbox
}

// NewHooks returns an http.Handler that accepts hook POSTs at:
//
//	POST /ambient-link/hooks/claude
//	POST /ambient-link/hooks/codex
//
// The handler dispatches to the appropriate parser and forwards the
// resulting proto.Event into ing.Ingest. Returns 204 on success, 400 on
// malformed body, 401 on bad token, 405 on wrong method.
func NewHooks(ing Ingester, cfg HooksConfig) http.Handler {
	if ing == nil {
		panic("producers: nil ingester")
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = 256 * 1024
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	mux := http.NewServeMux()
	mux.Handle("/ambient-link/hooks/claude", &hookHandler{ing: ing, cfg: cfg, parser: parseClaude})
	mux.Handle("/ambient-link/hooks/codex", &hookHandler{ing: ing, cfg: cfg, parser: parseCodex})
	return mux
}

// ── handler plumbing ────────────────────────────────────────────────────

type hookHandler struct {
	ing    Ingester
	cfg    HooksConfig
	parser func(map[string]any) ([]proto.Event, error)
}

func (h *hookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.authorize(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.cfg.MaxBodyBytes))
	if err != nil {
		h.cfg.Logger.Warn("hooks: read body", "err", err, "path", r.URL.Path)
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		h.cfg.Logger.Warn("hooks: unmarshal", "err", err, "path", r.URL.Path)
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	events, err := h.parser(raw)
	if err != nil {
		h.cfg.Logger.Warn("hooks: parse", "err", err, "path", r.URL.Path)
		http.Error(w, "unparseable hook payload", http.StatusBadRequest)
		return
	}
	for _, ev := range events {
		_ = h.ing.Ingest(ev) // mux logs its own warnings; duplicates not surfaced
	}
	hook, _ := raw["hook_event_name"].(string)
	sid, _ := raw["session_id"].(string)
	if resp := delivery.HookResponse(hook, sid, raw, h.cfg.Outbox); resp != nil {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *hookHandler) authorize(r *http.Request) bool {
	if h.cfg.BearerToken == "" {
		return true // unauthenticated mode (local dev / loopback only)
	}
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return subtle.ConstantTimeCompare([]byte(got), []byte(h.cfg.BearerToken)) == 1
}

// ── Claude Code parser ──────────────────────────────────────────────────

// Claude Code hook event names are stable per docs. We map a small subset
// that's reliably documented to our normalized EventType.
var claudeEventMap = map[string]proto.EventType{
	"SessionStart":     proto.EventSessionStart,
	"SessionEnd":       proto.EventSessionEnd,
	"UserPromptSubmit": proto.EventUserPrompt,
	"PreToolUse":       proto.EventToolUse,
	"PostToolUse":      proto.EventToolUse,
	"Stop":             proto.EventStop,
	"SubagentStop":     proto.EventStop,
}

func parseClaude(raw map[string]any) ([]proto.Event, error) {
	hook, _ := raw["hook_event_name"].(string)
	if hook == "" {
		return nil, errors.New("missing hook_event_name")
	}
	sid, _ := raw["session_id"].(string)
	if sid == "" {
		return nil, errors.New("missing session_id")
	}
	cwd, _ := raw["cwd"].(string)
	at := unixMillisFromAny(raw["timestamp"])
	if at == 0 {
		at = time.Now().UnixMilli()
	}

	// Notification is a special case: payload may carry a permission_prompt
	// matcher; treat the textual message as the prompt body.
	if hook == "Notification" {
		matcher, _ := raw["matcher"].(string)
		if matcher == "permission_prompt" || matcher == "" {
			return []proto.Event{{
				SessionID:  sid,
				Agent:      "claude",
				CWD:        cwd,
				Type:       proto.EventPermissionPrompt,
				Payload:    raw,
				Source:     proto.ProducerHooks,
				ObservedAt: at,
			}}, nil
		}
		// Unrecognized matcher — drop politely.
		return nil, nil
	}

	t, ok := claudeEventMap[hook]
	if !ok {
		return nil, fmt.Errorf("unrecognized hook_event_name %q", hook)
	}
	return []proto.Event{{
		SessionID:  sid,
		Agent:      "claude",
		CWD:        cwd,
		Type:       t,
		Payload:    raw,
		Source:     proto.ProducerHooks,
		ObservedAt: at,
	}}, nil
}

// ── Codex parser ────────────────────────────────────────────────────────

// Codex CLI hook event names (per developers.openai.com/codex/hooks).
var codexEventMap = map[string]proto.EventType{
	"SessionStart":      proto.EventSessionStart,
	"SessionEnd":        proto.EventSessionEnd,
	"UserPromptSubmit":  proto.EventUserPrompt,
	"PreToolUse":        proto.EventToolUse,
	"PostToolUse":       proto.EventToolUse,
	"PermissionRequest": proto.EventPermissionPrompt,
	"Stop":              proto.EventStop,
	"SubagentStop":      proto.EventStop,
}

func parseCodex(raw map[string]any) ([]proto.Event, error) {
	hook, _ := raw["hook_event_name"].(string)
	if hook == "" {
		// Codex sometimes uses "event" or "type" instead.
		if alt, _ := raw["event"].(string); alt != "" {
			hook = alt
		} else if alt, _ := raw["type"].(string); alt != "" {
			hook = alt
		}
	}
	if hook == "" {
		return nil, errors.New("missing hook event identifier")
	}
	sid, _ := raw["session_id"].(string)
	if sid == "" {
		return nil, errors.New("missing session_id")
	}
	cwd, _ := raw["cwd"].(string)
	at := unixMillisFromAny(raw["timestamp"])
	if at == 0 {
		at = time.Now().UnixMilli()
	}
	t, ok := codexEventMap[hook]
	if !ok {
		return nil, fmt.Errorf("unrecognized codex hook %q", hook)
	}
	return []proto.Event{{
		SessionID:  sid,
		Agent:      "codex",
		CWD:        cwd,
		Type:       t,
		Payload:    raw,
		Source:     proto.ProducerHooks,
		ObservedAt: at,
	}}, nil
}

// ── helpers ────────────────────────────────────────────────────────────

// unixMillisFromAny tolerates both numeric and ISO-8601-string timestamps.
// Returns 0 on anything unparseable; callers fall back to time.Now().
func unixMillisFromAny(v any) int64 {
	switch t := v.(type) {
	case float64:
		// JSON numbers — heuristic: > 10^12 → already ms; else seconds.
		if t > 1e12 {
			return int64(t)
		}
		return int64(t * 1000)
	case int64:
		if t > 1e12 {
			return t
		}
		return t * 1000
	case string:
		if ts, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return ts.UnixMilli()
		}
	}
	return 0
}

// Compile-time interface assertion.
var _ http.Handler = (*hookHandler)(nil)
