// Package store is the relay's durable local database.
//
// It replaces the old append-only JSONL journal with SQLite
// (modernc.org/sqlite — pure Go, no cgo, cross-platform). It is a strict
// superset of the journal: it keeps a `broadcasts` table with a monotonic
// `seq` so the WS subscribe/since replay protocol is byte-for-byte unchanged
// (see DECISIONS.md §3), AND it keeps `sessions` + `interactions` tables that
// are the actual queryable history of every agent↔human exchange.
//
// The native app remains the source of truth; this store is a durable,
// reconcilable mirror, never an authority that invents state.
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/maceip/ambient-link-core/host/internal/proto"
)

// Store is a SQLite-backed durable log. Safe for concurrent use; a single
// writer connection (WAL) keeps it simple and lock-free for readers.
type Store struct {
	db   *sql.DB
	path string
}

// Dir returns the relay state directory (~/.ambient-link or $AMBIENT_LINK_HOME).
func Dir() (string, error) {
	if root := os.Getenv("AMBIENT_LINK_HOME"); root != "" {
		return root, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".ambient-link"), nil
}

// Open creates or opens ~/.ambient-link/relay.db and applies the schema.
func Open() (*Store, error) {
	dir, err := Dir()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "relay.db")
	// WAL + busy timeout so a reader never errors under the single writer.
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(on)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// Single writer: SQLite allows one writer at a time; serializing avoids
	// "database is locked" entirely without sacrificing correctness.
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db, path: path}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: migrate: %w", err)
	}
	return s, nil
}

// Path returns the database file path (for diagnostics).
func (s *Store) Path() string { return s.path }

// Close closes the underlying database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) migrate() error {
	const schema = `
CREATE TABLE IF NOT EXISTS broadcasts (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  at         INTEGER NOT NULL,
  type       TEXT    NOT NULL,
  thread     TEXT,
  session_id TEXT,
  agent      TEXT,
  json       TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_broadcasts_thread ON broadcasts(thread);

CREATE TABLE IF NOT EXISTS sessions (
  session_id TEXT PRIMARY KEY,
  thread_id  TEXT,
  agent      TEXT,
  cwd        TEXT,
  label      TEXT,
  state      TEXT,
  first_seen INTEGER,
  last_seen  INTEGER
);

CREATE TABLE IF NOT EXISTS interactions (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  at              INTEGER NOT NULL,
  session_id      TEXT,
  thread_id       TEXT,
  agent           TEXT,
  role            TEXT,   -- 'assistant' | 'human'
  text            TEXT,
  delivery_status TEXT    -- human only: written|landed|queued|failed
);
CREATE INDEX IF NOT EXISTS idx_interactions_session ON interactions(session_id);
CREATE INDEX IF NOT EXISTS idx_interactions_thread  ON interactions(thread_id);
`
	_, err := s.db.Exec(schema)
	return err
}

// ── journal-compatible surface (used by the WS hub for subscribe/replay) ──

// Append records a broadcast and returns its monotonic sequence number. It
// mirrors journal.Append so the hub is unchanged. It also projects the
// broadcast into the sessions table and, when the broadcast carries assistant
// text, into the interactions table — so the DB is a real history, not just a
// replay buffer.
func (s *Store) Append(b proto.Broadcast) (int64, error) {
	if b.At == 0 {
		b.At = time.Now().UnixMilli()
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return 0, err
	}
	res, err := s.db.Exec(
		`INSERT INTO broadcasts (at, type, thread, session_id, agent, json) VALUES (?, ?, ?, ?, ?, ?)`,
		b.At, string(b.Type), b.Thread, b.SessionID, b.Agent, string(raw),
	)
	if err != nil {
		return 0, err
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	s.projectSession(b)
	if b.LastAssistant != "" {
		// Best-effort interaction projection; replay never depends on it.
		_, _ = s.db.Exec(
			`INSERT INTO interactions (at, session_id, thread_id, agent, role, text) VALUES (?, ?, ?, ?, 'assistant', ?)`,
			b.At, b.SessionID, b.Thread, b.Agent, b.LastAssistant,
		)
	}
	return seq, nil
}

func (s *Store) projectSession(b proto.Broadcast) {
	if b.Thread == "" && b.SessionID == "" {
		return
	}
	state := ""
	switch b.Type {
	case proto.BroadcastThreadBusy:
		state = string(proto.StateBusy)
	case proto.BroadcastThreadIdle:
		state = string(proto.StateIdle)
	case proto.BroadcastThreadEnded:
		state = string(proto.StateDead)
	case proto.BroadcastThreadStarted:
		state = string(proto.StateStarting)
	}
	_, _ = s.db.Exec(`
INSERT INTO sessions (session_id, thread_id, agent, cwd, label, state, first_seen, last_seen)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(session_id) DO UPDATE SET
  thread_id = COALESCE(NULLIF(excluded.thread_id,''), sessions.thread_id),
  agent     = COALESCE(NULLIF(excluded.agent,''), sessions.agent),
  cwd       = COALESCE(NULLIF(excluded.cwd,''), sessions.cwd),
  label     = COALESCE(NULLIF(excluded.label,''), sessions.label),
  state     = COALESCE(NULLIF(excluded.state,''), sessions.state),
  last_seen = excluded.last_seen
`,
		nz(b.SessionID, b.Thread), b.Thread, b.Agent, b.CWD, b.Label, state, b.At, b.At)
}

// Head returns the latest broadcast sequence number (0 if empty).
func (s *Store) Head() int64 {
	var seq sql.NullInt64
	_ = s.db.QueryRow(`SELECT MAX(seq) FROM broadcasts`).Scan(&seq)
	if seq.Valid {
		return seq.Int64
	}
	return 0
}

// ReplayAfter returns broadcasts with seq > after, in order.
func (s *Store) ReplayAfter(after int64) ([]proto.Broadcast, error) {
	rows, err := s.db.Query(`SELECT json FROM broadcasts WHERE seq > ? ORDER BY seq ASC`, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []proto.Broadcast
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var b proto.Broadcast
		if err := json.Unmarshal([]byte(raw), &b); err != nil {
			continue
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ── interaction history (the durable record of human↔agent exchange) ──────

// RecordHuman persists a human turn with its honest delivery status. Called
// only when delivery actually occurred (no optimistic recording — DECISIONS §4).
func (s *Store) RecordHuman(sessionID, threadID, agent, text, status string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.Exec(
		`INSERT INTO interactions (at, session_id, thread_id, agent, role, text, delivery_status) VALUES (?, ?, ?, ?, 'human', ?, ?)`,
		time.Now().UnixMilli(), sessionID, threadID, agent, text, status,
	)
	return err
}

// MarkLanded upgrades the most recent human interaction for a session whose
// text matches and whose status is not yet 'landed' — used when the agent's own
// transcript confirms the message became a user turn (DECISIONS §4).
func (s *Store) MarkLanded(sessionID, text string) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.Exec(`
UPDATE interactions SET delivery_status = 'landed'
WHERE id = (
  SELECT id FROM interactions
  WHERE role='human' AND session_id=? AND text=? AND delivery_status != 'landed'
  ORDER BY at DESC LIMIT 1
)`, sessionID, text)
	return err
}

// Interaction is a row of the durable history, oldest-first.
type Interaction struct {
	At        int64  `json:"at"`
	SessionID string `json:"session_id"`
	ThreadID  string `json:"thread_id"`
	Agent     string `json:"agent"`
	Role      string `json:"role"`
	Text      string `json:"text"`
	Status    string `json:"delivery_status,omitempty"`
}

// History returns up to limit interactions for a session, oldest-first.
func (s *Store) History(sessionID string, limit int) ([]Interaction, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.Query(`
SELECT at, session_id, thread_id, agent, role, text, COALESCE(delivery_status,'')
FROM interactions WHERE session_id = ? ORDER BY at ASC LIMIT ?`, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Interaction
	for rows.Next() {
		var it Interaction
		if err := rows.Scan(&it.At, &it.SessionID, &it.ThreadID, &it.Agent, &it.Role, &it.Text, &it.Status); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

func nz(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
