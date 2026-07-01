package delivery

import "sync"

// PTYWriter is a relay-owned pseudo-terminal master for a launched agent.
// Writing it is real stdin to the child, so it is the most reliable delivery
// channel (DECISIONS.md §2b). The PTY launcher registers one per THREAD it
// owns (keyed by thread, not session id, because the same agent+cwd may also be
// observed by the JSONL tailer under a different session id — they share a
// thread). The inject layer, which knows the thread, prefers it over the
// console/tty adapters.
type PTYWriter interface {
	// WriteInput types text into the child. When submit is true a carriage
	// return is appended so the agent receives a completed line.
	WriteInput(text string, submit bool) error
}

var (
	ptyMu      sync.RWMutex
	ptyWriters = map[string]PTYWriter{} // threadID → writer
)

// RegisterPTY records a relay-owned PTY master as the preferred delivery
// channel for a thread. Pass nil writer to unregister (on agent exit).
func RegisterPTY(threadID string, w PTYWriter) {
	if threadID == "" {
		return
	}
	ptyMu.Lock()
	if w == nil {
		delete(ptyWriters, threadID)
	} else {
		ptyWriters[threadID] = w
	}
	ptyMu.Unlock()
}

// PTYByThread returns the relay-owned PTY writer for a thread, or nil.
func PTYByThread(threadID string) PTYWriter {
	ptyMu.RLock()
	defer ptyMu.RUnlock()
	return ptyWriters[threadID]
}
