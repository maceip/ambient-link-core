package delivery

import (
	"fmt"
	"log/slog"
	"strings"
	"time"
)

var logger = slog.Default()

// SetLogger wires structured logs for delivery attempts.
func SetLogger(l *slog.Logger) {
	if l != nil {
		logger = l
	}
}

const (
	StatusDelivered = "delivered"
	StatusQueued    = "queued"
)

// Result describes what happened to a reply after the relay accepted it. It is
// meant for diagnostics and quiet client tracking, not blocking UI flow.
type Result struct {
	ID           string `json:"id,omitempty"`
	SessionID    string `json:"session_id"`
	ThreadID     string `json:"thread"`
	Status       string `json:"status"`
	PendingCount int    `json:"pending_count,omitempty"`
	Error        string `json:"error,omitempty"`
}

// Deliver routes a user reply into a live terminal session. Tries adapters in
// order; on failure stores a pending message for background retry. Delivery
// ALWAYS submits (types text + Enter) — there is no "don't submit" mode
// (DECISIONS.md §4). Single-key prompt answers use TrySpecial.
func Deliver(sessionID, threadID, agent, text string, reg *Registry, box *Outbox) error {
	_, err := DeliverWithResult(sessionID, threadID, agent, text, reg, box, "")
	return err
}

// DeliverWithResult is Deliver plus a structured status for callers that track
// delivery internally.
func DeliverWithResult(sessionID, threadID, agent, text string, reg *Registry, box *Outbox, messageID string) (Result, error) {
	result := Result{ID: messageID, SessionID: sessionID, ThreadID: threadID}
	if box == nil {
		return result, fmt.Errorf("delivery: nil outbox")
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return result, fmt.Errorf("delivery: empty text")
	}
	msg := Message{
		ID: messageID, SessionID: sessionID, ThreadID: threadID, Text: text,
		At: time.Now().UnixMilli(),
	}
	agent = strings.ToLower(strings.TrimSpace(agent))
	if agent == "web" {
		if err := box.Enqueue(msg); err != nil {
			return result, err
		}
		result.Status = StatusQueued
		result.PendingCount = box.Count(sessionID)
		return result, nil
	}

	// Preserve ordering. If earlier replies are waiting, this reply joins the
	// queue instead of jumping ahead via an immediate adapter.
	if box.HasPending(sessionID) {
		if err := box.Enqueue(msg); err != nil {
			return result, err
		}
		result.Status = StatusQueued
		result.PendingCount = box.Count(sessionID)
		return result, nil
	}

	if err := TryImmediate(sessionID, text, reg); err == nil {
		result.Status = StatusDelivered
		return result, nil
	} else {
		msg.LastError = err.Error()
		if enqueueErr := box.Enqueue(msg); enqueueErr != nil {
			result.Error = enqueueErr.Error()
			return result, enqueueErr
		}
		result.Status = StatusQueued
		result.PendingCount = box.Count(sessionID)
		result.Error = err.Error()
		logger.Info("delivery: queued for retry", "session", sessionID, "thread", threadID, "err", err)
		return result, nil
	}
}

// TryImmediate runs terminal adapters when proc correlation has a live endpoint.
// It ALWAYS submits (types text + Enter). The relay-owned PTY path (preferred
// and most reliable) is resolved earlier, at the inject layer, by thread id.
func TryImmediate(sessionID, text string, reg *Registry) error {
	if reg == nil {
		return fmt.Errorf("delivery: no registry")
	}
	ep, ok := reg.Get(sessionID)
	if !ok {
		return fmt.Errorf("delivery: no live endpoint for %s", sessionID)
	}
	if ep.PID <= 0 {
		return fmt.Errorf("delivery: no live endpoint for %s", sessionID)
	}
	var attempts []string
	if err := SendProcessInput(ep.PID, text, true); err == nil {
		logger.Info("delivery: process input", "session", sessionID, "pid", ep.PID)
		return nil
	} else {
		logger.Debug("delivery: process input adapter failed", "session", sessionID, "pid", ep.PID, "err", err)
		attempts = append(attempts, err.Error())
	}
	if err := SendTmuxPID(ep.PID, text, true); err == nil {
		logger.Info("delivery: tmux", "session", sessionID, "pid", ep.PID)
		return nil
	} else {
		logger.Debug("delivery: tmux adapter failed", "session", sessionID, "pid", ep.PID, "err", err)
		attempts = append(attempts, err.Error())
	}
	if ep.TTY != "" {
		if err := WriteTTY(ep.TTY, text, true); err == nil {
			logger.Info("delivery: tty", "session", sessionID, "tty", ep.TTY)
			return nil
		} else {
			attempts = append(attempts, err.Error())
		}
	}
	return fmt.Errorf("delivery: adapters failed for %s (%s)", sessionID, strings.Join(attempts, "; "))
}

// TrySpecial delivers a single raw key (e.g. y/n for a permission prompt)
// without a trailing newline. This is a distinct human intent from sending a
// message, so it has its own path rather than an enter=false flag.
func TrySpecial(sessionID, key string, reg *Registry) error {
	if reg == nil {
		return fmt.Errorf("delivery: no registry")
	}
	ep, ok := reg.Get(sessionID)
	if !ok {
		return fmt.Errorf("delivery: no live endpoint for %s", sessionID)
	}
	if ep.PID <= 0 {
		return fmt.Errorf("delivery: no live endpoint for %s", sessionID)
	}
	if err := SendProcessInput(ep.PID, key, false); err == nil {
		return nil
	}
	if err := SendTmuxPID(ep.PID, key, false); err == nil {
		return nil
	}
	if ep.TTY != "" {
		if err := WriteTTY(ep.TTY, key, false); err == nil {
			return nil
		}
	}
	return fmt.Errorf("delivery: special key failed for %s", sessionID)
}

// RetryPending attempts immediate delivery for one queued message.
func RetryPending(sessionID string, reg *Registry, box *Outbox) bool {
	if box == nil || !box.HasPending(sessionID) {
		return false
	}
	delivered := false
	for i := 0; i < 32; i++ {
		msg, ok := box.Peek(sessionID)
		if !ok {
			return delivered
		}
		if err := TryImmediate(sessionID, msg.Text, reg); err != nil {
			box.MarkAttempt(sessionID, msg.ID, err)
			return delivered
		}
		_, _ = box.Dequeue(sessionID)
		logger.Info("delivery: retry ok", "session", sessionID, "thread", msg.ThreadID)
		delivered = true
	}
	return delivered
}

// FlushPending retries queued messages for all sessions.
func FlushPending(reg *Registry, box *Outbox) int {
	if box == nil {
		return 0
	}
	ids, err := box.ListPendingIDs()
	if err != nil {
		return 0
	}
	n := 0
	for _, id := range ids {
		if RetryPending(id, reg, box) {
			n++
		}
	}
	return n
}
