package delivery

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const outboxDir = ".ambient-link/outbox"

// OutboxDir returns ~/.ambient-link/outbox for diagnostics.
func OutboxDir() (string, error) { return outboxRoot() }

// Message is one HUD reply waiting for an agent adapter to consume.
type Message struct {
	SessionID string `json:"session_id"`
	ThreadID  string `json:"thread"`
	Text      string `json:"text"`
	Enter     bool   `json:"enter"`
	At        int64  `json:"at"`
}

// Outbox stores at-most-one pending message per session_id. Producers drain
// via hooks; Deliver enqueues when immediate adapters (tmux) fail.
type Outbox struct {
	mu sync.Mutex
}

func NewOutbox() *Outbox { return &Outbox{} }

func outboxRoot() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, outboxDir), nil
}

func pendingPath(sessionID string) (string, error) {
	root, err := outboxRoot()
	if err != nil {
		return "", err
	}
	safe := filepath.Base(sessionID)
	if safe == "" || safe == "." {
		return "", errors.New("delivery: invalid session id")
	}
	return filepath.Join(root, safe+".pending.json"), nil
}

// Enqueue stores the latest pending message for a session (overwrites prior).
func (o *Outbox) Enqueue(msg Message) error {
	if msg.SessionID == "" {
		return errors.New("delivery: empty session_id")
	}
	path, err := pendingPath(msg.SessionID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// Dequeue removes and returns a pending message, if any.
func (o *Outbox) Dequeue(sessionID string) (Message, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	path, err := pendingPath(sessionID)
	if err != nil {
		return Message{}, false
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Message{}, false
	}
	var msg Message
	if err := json.Unmarshal(b, &msg); err != nil {
		_ = os.Remove(path)
		return Message{}, false
	}
	_ = os.Remove(path)
	return msg, true
}

// HasPending reports whether a session has an undelivered message.
func (o *Outbox) HasPending(sessionID string) bool {
	path, err := pendingPath(sessionID)
	if err != nil {
		return false
	}
	_, err = os.Stat(path)
	return err == nil
}

// Peek reads a pending message without removing it.
func (o *Outbox) Peek(sessionID string) (Message, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	path, err := pendingPath(sessionID)
	if err != nil {
		return Message{}, false
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Message{}, false
	}
	var msg Message
	if err := json.Unmarshal(b, &msg); err != nil {
		return Message{}, false
	}
	return msg, true
}

// ListPendingIDs returns session_ids with a pending delivery file.
func (o *Outbox) ListPendingIDs() ([]string, error) {
	root, err := outboxRoot()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".pending.json") {
			continue
		}
		ids = append(ids, strings.TrimSuffix(e.Name(), ".pending.json"))
	}
	return ids, nil
}
