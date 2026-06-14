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

// Deliver routes a user reply into a live terminal session. Tries adapters in
// order; on failure stores one pending message per session for background retry.
func Deliver(sessionID, threadID, agent, text string, enter bool, reg *Registry, box *Outbox) error {
	if box == nil {
		return fmt.Errorf("delivery: nil outbox")
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return fmt.Errorf("delivery: empty text")
	}
	agent = strings.ToLower(strings.TrimSpace(agent))
	if agent == "web" {
		return box.Enqueue(Message{
			SessionID: sessionID, ThreadID: threadID, Text: text, Enter: enter,
			At: time.Now().UnixMilli(),
		})
	}
	if err := TryImmediate(sessionID, text, enter, reg); err == nil {
		if box.HasPending(sessionID) {
			_, _ = box.Dequeue(sessionID)
		}
		return nil
	}
	if pending, ok := box.Peek(sessionID); ok && pending.Text == text && pending.Enter == enter {
		logger.Debug("delivery: already queued", "session", sessionID, "thread", threadID)
		return nil
	}
	logger.Info("delivery: queued for retry", "session", sessionID, "thread", threadID)
	return box.Enqueue(Message{
		SessionID: sessionID, ThreadID: threadID, Text: text, Enter: enter,
		At: time.Now().UnixMilli(),
	})
}

// TryImmediate runs terminal adapters when proc correlation has a live endpoint.
func TryImmediate(sessionID, text string, enter bool, reg *Registry) error {
	if reg == nil {
		return fmt.Errorf("delivery: no registry")
	}
	ep, ok := reg.Get(sessionID)
	if !ok || ep.PID <= 0 {
		return fmt.Errorf("delivery: no live endpoint for %s", sessionID)
	}
	if err := SendTmuxPID(ep.PID, text, enter); err == nil {
		logger.Info("delivery: tmux", "session", sessionID, "pid", ep.PID)
		return nil
	}
	if ep.TTY != "" {
		if err := WriteTTY(ep.TTY, text, enter); err == nil {
			logger.Info("delivery: tty", "session", sessionID, "tty", ep.TTY)
			return nil
		}
	}
	return fmt.Errorf("delivery: adapters failed for %s", sessionID)
}

// RetryPending attempts immediate delivery for one queued message.
func RetryPending(sessionID string, reg *Registry, box *Outbox) bool {
	if box == nil || !box.HasPending(sessionID) {
		return false
	}
	msg, ok := box.Peek(sessionID)
	if !ok {
		return false
	}
	if TryImmediate(sessionID, msg.Text, msg.Enter, reg) != nil {
		return false
	}
	_, _ = box.Dequeue(sessionID)
	logger.Info("delivery: retry ok", "session", sessionID, "thread", msg.ThreadID)
	return true
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
