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
	_, err := SendInputResult(threadID, text, enter, "")
	return err
}

// SendInputResult delivers text and returns a quiet delivery status for
// clients that track reliability without blocking the user's flow.
func SendInputResult(threadID, text string, enter bool, messageID string) (delivery.Result, error) {
	var result delivery.Result
	if lookup == nil || outbox == nil {
		return result, fmt.Errorf("inject: not initialized")
	}
	sessionID, agent, ok := lookup.SessionForThread(threadID)
	if !ok {
		result.ThreadID = threadID
		result.ID = messageID
		return result, fmt.Errorf("inject: unknown thread %q", threadID)
	}
	result, err := delivery.DeliverWithResult(sessionID, threadID, agent, text, enter, targets, outbox, messageID)
	delivery.FlushPending(targets, outbox)
	return result, err
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
