// Package journal appends mux broadcasts to a durable JSONL log so WS clients
// can catch up on sessions that appear mid-stream (subscribe replay).
package journal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/maceip/ambient-link-core/host/internal/proto"
)

// Entry is one line in ~/.ambient-link/journal.jsonl.
type Entry struct {
	Seq       int64           `json:"seq"`
	At        int64           `json:"at"`
	Broadcast proto.Broadcast `json:"broadcast"`
}

// Journal is an append-only broadcast log with monotonic sequence numbers.
type Journal struct {
	mu   sync.Mutex
	seq  int64
	path string
}

// Dir returns ~/.ambient-link.
func Dir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".ambient-link"), nil
}

// Open creates or opens the journal file.
func Open() (*Journal, error) {
	root, err := Dir()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(root, "journal.jsonl")
	j := &Journal{path: path}
	if err := j.loadHead(); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *Journal) loadHead() error {
	f, err := os.Open(j.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var e Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			continue
		}
		if e.Seq > j.seq {
			j.seq = e.Seq
		}
	}
	return sc.Err()
}

// Head returns the latest sequence number (0 if empty).
func (j *Journal) Head() int64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.seq
}

// Append records a broadcast and returns its sequence number.
func (j *Journal) Append(b proto.Broadcast) (int64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.seq++
	e := Entry{Seq: j.seq, At: b.At, Broadcast: b}
	line, err := json.Marshal(e)
	if err != nil {
		j.seq--
		return 0, err
	}
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		j.seq--
		return 0, err
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, "%s\n", line); err != nil {
		j.seq--
		return 0, err
	}
	return j.seq, nil
}

// ReplayAfter returns broadcasts with seq > after, in order.
func (j *Journal) ReplayAfter(after int64) ([]proto.Broadcast, error) {
	f, err := os.Open(j.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []proto.Broadcast
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		var e Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			continue
		}
		if e.Seq > after {
			out = append(out, e.Broadcast)
		}
	}
	return out, sc.Err()
}
