// Package inject delivers HUD replies into live agent sessions.
//
// Resolution is symmetric with observation: the mux picks the best session for
// a thread_id; the delivery registry (fed by the proc watcher) maps that
// session_id to a controlling TTY for CLI agents. Virtual agents land in the
// unified outbox queue.
package inject

import (
	"fmt"
	"strings"

	"github.com/maceip/ambient-link-core/host/internal/delivery"
)

// SessionLookup resolves thread_id → (session_id, agent).
type SessionLookup interface {
	SessionForThread(threadID string) (sessionID, agent string, ok bool)
}

var (
	lookup  SessionLookup
	targets *delivery.Registry
	outbox  *delivery.Outbox
)

// Init wires session resolution, live endpoints, and the durable outbox. Call
// once at host startup before any SendInput.
func Init(l SessionLookup, reg *delivery.Registry, box *delivery.Outbox) {
	lookup = l
	targets = reg
	outbox = box
}

// SendInput delivers text for the given thread.
func SendInput(threadID, text string, enter bool) error {
	if lookup == nil || outbox == nil {
		return fmt.Errorf("inject: not initialized")
	}
	sessionID, agent, ok := lookup.SessionForThread(threadID)
	if !ok {
		return fmt.Errorf("inject: unknown thread %q", threadID)
	}
	err := delivery.Deliver(sessionID, threadID, agent, text, enter, targets, outbox)
	delivery.FlushPending(targets, outbox)
	return err
}

// SendSpecial delivers a single key (e.g. y/n for permission prompts).
func SendSpecial(threadID, key string) error {
	return SendInput(threadID, key, false)
}

// AgentFromThread extracts the agent prefix from a thread id ("claude-foo" → "claude").
func AgentFromThread(threadID string) string {
	if i := strings.Index(threadID, "-"); i > 0 {
		return threadID[:i]
	}
	return threadID
}
