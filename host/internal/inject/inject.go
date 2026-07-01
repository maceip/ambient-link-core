// Package inject delivers HUD replies into live agent sessions.
//
// Resolution is symmetric with observation: the mux picks the best session for
// a thread_id; the delivery registry (fed by the proc watcher / PTY launcher)
// maps that session_id to a channel. Delivery ALWAYS submits (DECISIONS.md §4):
// there is no "type but don't send" mode. The human turn is recorded in the
// durable store with its HONEST status — never optimistically (DECISIONS.md §4).
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

// Recorder persists human turns durably. The SQLite store satisfies it; nil
// disables recording. We only record what actually happened — status reflects
// the real delivery outcome (DECISIONS.md §4).
type Recorder interface {
	RecordHuman(sessionID, threadID, agent, text, status string) error
}

var (
	lookup   SessionLookup
	targets  *delivery.Registry
	outbox   *delivery.Outbox
	recorder Recorder
)

// Init wires session resolution, live endpoints, the durable outbox, and the
// interaction recorder. Call once at host startup before any SendInput.
func Init(l SessionLookup, reg *delivery.Registry, box *delivery.Outbox, rec Recorder) {
	lookup = l
	targets = reg
	outbox = box
	recorder = rec
}

// SendInput delivers text for the given thread. Always submits.
func SendInput(threadID, text string) error {
	_, err := SendInputResult(threadID, text, "")
	return err
}

// SendInputResult delivers text and returns a quiet delivery status for clients
// that track reliability without blocking the user's flow.
func SendInputResult(threadID, text, messageID string) (delivery.Result, error) {
	var result delivery.Result
	if lookup == nil || outbox == nil {
		return result, fmt.Errorf("inject: not initialized")
	}
	// Preferred channel: a relay-owned PTY for this thread (real stdin, cannot
	// be lost like a console-input write). DECISIONS.md §2b/§4. The PTY writer
	// existing IS proof of a live relay-owned agent, so we can deliver even
	// before the agent has written its first transcript (no mux session yet).
	sessionID, agent, _ := lookup.SessionForThread(threadID)
	if w := delivery.PTYByThread(threadID); w != nil {
		if err := w.WriteInput(text, true); err == nil {
			result.ID, result.SessionID, result.ThreadID = messageID, sessionID, threadID
			result.Status = delivery.StatusDelivered
			if recorder != nil {
				_ = recorder.RecordHuman(sessionID, threadID, agent, text, result.Status)
			}
			return result, nil
		}
		// PTY write failed (child gone?) — fall through to console/tty adapters.
	}
	if sessionID == "" {
		result.ThreadID = threadID
		result.ID = messageID
		return result, fmt.Errorf("inject: unknown thread %q", threadID)
	}
	result, err := delivery.DeliverWithResult(sessionID, threadID, agent, text, targets, outbox, messageID)
	delivery.FlushPending(targets, outbox)
	// Record the human turn with its honest status — only after a real attempt,
	// and only when we resolved a session. Never an optimistic "sent".
	if err == nil && recorder != nil && result.Status != "" {
		_ = recorder.RecordHuman(sessionID, threadID, agent, text, result.Status)
	}
	return result, err
}

// Delivered reports whether a result represents bytes actually written to the
// agent's input channel (as opposed to queued for later). Callers use this to
// decide whether to surface the turn on the live HUD.
func Delivered(r delivery.Result) bool {
	return r.Status == delivery.StatusDelivered
}

// SendSpecial delivers a single raw key (e.g. y/n for a permission prompt).
func SendSpecial(threadID, key string) error {
	if lookup == nil {
		return fmt.Errorf("inject: not initialized")
	}
	if w := delivery.PTYByThread(threadID); w != nil {
		if err := w.WriteInput(key, false); err == nil {
			return nil
		}
	}
	sessionID, _, ok := lookup.SessionForThread(threadID)
	if !ok {
		return fmt.Errorf("inject: unknown thread %q", threadID)
	}
	return delivery.TrySpecial(sessionID, key, targets)
}

// AgentFromThread extracts the agent prefix from a thread id ("claude-foo" → "claude").
func AgentFromThread(threadID string) string {
	if i := strings.Index(threadID, "-"); i > 0 {
		return threadID[:i]
	}
	return threadID
}
