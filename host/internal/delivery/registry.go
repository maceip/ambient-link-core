// Package delivery routes HUD replies back into live agent sessions.
//
// CLI agents (claude, codex, cursor-agent) are reached via tmux or controlling
// TTY discovered by the process watcher. Virtual agents (web) enqueue to outbox.
package delivery

import (
	"sync"
	"time"
)

// Endpoint is a live delivery target for a session_id.
type Endpoint struct {
	SessionID string
	PID       int
	TTY       string
	Agent     string
	UpdatedAt int64 // unix milliseconds
}

// Registry maps session_id → live endpoint. Updated by the proc watcher each
// poll; readers use it at inject time.
type Registry struct {
	mu   sync.RWMutex
	byID map[string]Endpoint
}

func NewRegistry() *Registry {
	return &Registry{byID: make(map[string]Endpoint)}
}

func (r *Registry) Set(ep Endpoint) {
	if ep.SessionID == "" {
		return
	}
	if ep.UpdatedAt == 0 {
		ep.UpdatedAt = time.Now().UnixMilli()
	}
	r.mu.Lock()
	r.byID[ep.SessionID] = ep
	r.mu.Unlock()
}

func (r *Registry) Get(sessionID string) (Endpoint, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ep, ok := r.byID[sessionID]
	return ep, ok
}

func (r *Registry) Remove(sessionID string) {
	r.mu.Lock()
	delete(r.byID, sessionID)
	r.mu.Unlock()
}

// Snapshot returns a copy of all endpoints (for /status diagnostics).
func (r *Registry) Snapshot() []Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Endpoint, 0, len(r.byID))
	for _, ep := range r.byID {
		out = append(out, ep)
	}
	return out
}
