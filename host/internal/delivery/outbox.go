package delivery

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const outboxDir = ".ambient-link/outbox"

// OutboxDir returns ~/.ambient-link/outbox for diagnostics.
func OutboxDir() (string, error) { return outboxRoot() }

// Message is one HUD reply waiting for an agent adapter to consume.
type Message struct {
	ID        string `json:"id,omitempty"`
	SessionID string `json:"session_id"`
	ThreadID  string `json:"thread"`
	Text      string `json:"text"`
	At        int64  `json:"at"`
	Attempts  int    `json:"attempts,omitempty"`
	LastError string `json:"last_error,omitempty"`
}

// PendingMessage is the status-safe outbox view exposed over diagnostics. It
// deliberately omits the reply text.
type PendingMessage struct {
	ID        string `json:"id,omitempty"`
	SessionID string `json:"session_id"`
	ThreadID  string `json:"thread"`
	At        int64  `json:"at"`
	Attempts  int    `json:"attempts,omitempty"`
	LastError string `json:"last_error,omitempty"`
}

// PendingSession is the status-safe queue view for one session.
type PendingSession struct {
	SessionID string           `json:"session_id"`
	Count     int              `json:"count"`
	OldestAt  int64            `json:"oldest_at,omitempty"`
	Messages  []PendingMessage `json:"messages,omitempty"`
}

// Outbox stores a durable FIFO queue per session_id. Producers drain via hooks;
// Deliver enqueues when immediate adapters (tmux/tty) are unavailable.
type Outbox struct {
	mu sync.Mutex
}

func NewOutbox() *Outbox { return &Outbox{} }

func outboxRoot() (string, error) {
	if root := os.Getenv("AMBIENT_LINK_HOME"); root != "" {
		return filepath.Join(root, "outbox"), nil
	}
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

// Enqueue appends a pending message for a session, preserving send order.
func (o *Outbox) Enqueue(msg Message) error {
	if msg.SessionID == "" {
		return errors.New("delivery: empty session_id")
	}
	if msg.ID == "" {
		msg.ID = newMessageID()
	}
	if msg.At == 0 {
		msg.At = time.Now().UnixMilli()
	}
	path, err := pendingPath(msg.SessionID)
	if err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	rows, err := readQueue(path)
	if err != nil {
		return err
	}
	rows = append(rows, msg)
	return writeQueue(path, rows)
}

func newMessageID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err == nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + hex.EncodeToString(b[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func readQueue(path string) ([]Message, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	b = []byte(strings.TrimSpace(string(b)))
	if len(b) == 0 {
		return nil, nil
	}
	if b[0] == '[' {
		var rows []Message
		if err := json.Unmarshal(b, &rows); err != nil {
			return nil, err
		}
		return normalizeQueue(rows), nil
	}
	var msg Message
	if err := json.Unmarshal(b, &msg); err != nil {
		return nil, err
	}
	return normalizeQueue([]Message{msg}), nil
}

func normalizeQueue(rows []Message) []Message {
	out := rows[:0]
	for _, msg := range rows {
		if msg.SessionID == "" || strings.TrimSpace(msg.Text) == "" {
			continue
		}
		if msg.At == 0 {
			msg.At = time.Now().UnixMilli()
		}
		out = append(out, msg)
	}
	return out
}

func writeQueue(path string, rows []Message) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if len(rows) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	b, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	_ = os.Remove(path)
	return os.Rename(tmp, path)
}

// Dequeue removes and returns a pending message, if any.
func (o *Outbox) Dequeue(sessionID string) (Message, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	path, err := pendingPath(sessionID)
	if err != nil {
		return Message{}, false
	}
	rows, err := readQueue(path)
	if err != nil {
		_ = os.Remove(path)
		return Message{}, false
	}
	if len(rows) == 0 {
		return Message{}, false
	}
	msg := rows[0]
	if err := writeQueue(path, rows[1:]); err != nil {
		return Message{}, false
	}
	return msg, true
}

// HasPending reports whether a session has an undelivered message.
func (o *Outbox) HasPending(sessionID string) bool {
	return o.Count(sessionID) > 0
}

// Count returns the number of queued messages for a session.
func (o *Outbox) Count(sessionID string) int {
	path, err := pendingPath(sessionID)
	if err != nil {
		return 0
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	rows, err := readQueue(path)
	if err != nil {
		return 0
	}
	return len(rows)
}

// Peek reads a pending message without removing it.
func (o *Outbox) Peek(sessionID string) (Message, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	path, err := pendingPath(sessionID)
	if err != nil {
		return Message{}, false
	}
	rows, err := readQueue(path)
	if err != nil || len(rows) == 0 {
		return Message{}, false
	}
	return rows[0], true
}

// MarkAttempt records a failed retry attempt for the queued message.
func (o *Outbox) MarkAttempt(sessionID, messageID string, attemptErr error) {
	if sessionID == "" || messageID == "" {
		return
	}
	path, err := pendingPath(sessionID)
	if err != nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	rows, err := readQueue(path)
	if err != nil {
		return
	}
	for i := range rows {
		if rows[i].ID != messageID {
			continue
		}
		rows[i].Attempts++
		if attemptErr != nil {
			rows[i].LastError = attemptErr.Error()
		}
		_ = writeQueue(path, rows)
		return
	}
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

// Snapshot returns a status-safe copy of all pending queues.
func (o *Outbox) Snapshot() ([]PendingSession, error) {
	ids, err := o.ListPendingIDs()
	if err != nil {
		return nil, err
	}
	out := make([]PendingSession, 0, len(ids))
	for _, id := range ids {
		path, err := pendingPath(id)
		if err != nil {
			continue
		}
		o.mu.Lock()
		rows, err := readQueue(path)
		o.mu.Unlock()
		if err != nil || len(rows) == 0 {
			continue
		}
		ps := PendingSession{
			SessionID: id,
			Count:     len(rows),
			OldestAt:  rows[0].At,
			Messages:  make([]PendingMessage, 0, len(rows)),
		}
		for _, msg := range rows {
			ps.Messages = append(ps.Messages, PendingMessage{
				ID:        msg.ID,
				SessionID: msg.SessionID,
				ThreadID:  msg.ThreadID,
				At:        msg.At,
				Attempts:  msg.Attempts,
				LastError: msg.LastError,
			})
		}
		out = append(out, ps)
	}
	return out, nil
}

// PurgeOlderThan drops queued messages whose At timestamp is before cutoff.
// Returns the number of messages removed.
func (o *Outbox) PurgeOlderThan(maxAge time.Duration) (int, error) {
	if maxAge <= 0 {
		return 0, nil
	}
	cutoff := time.Now().Add(-maxAge).UnixMilli()
	ids, err := o.ListPendingIDs()
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, id := range ids {
		path, err := pendingPath(id)
		if err != nil {
			continue
		}
		o.mu.Lock()
		rows, err := readQueue(path)
		if err != nil {
			o.mu.Unlock()
			_ = os.Remove(path)
			continue
		}
		kept := rows[:0]
		for _, msg := range rows {
			if msg.At > 0 && msg.At < cutoff {
				removed++
				continue
			}
			kept = append(kept, msg)
		}
		_ = writeQueue(path, kept)
		o.mu.Unlock()
	}
	return removed, nil
}
