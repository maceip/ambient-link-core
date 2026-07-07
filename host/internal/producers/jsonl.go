// JSONL tailer producer.
//
// Claude Code persists every session's events to
//
//	~/.claude/projects/<sanitized-cwd>/<session-uuid>.jsonl
//
// appended in real time. The tailer:
//
//  1. Recursively discovers existing .jsonl files under the root at startup.
//  2. Uses fsnotify on the root subtree to spot new sessions / new directories.
//  3. For each file, runs a tail loop that consumes newly-appended bytes and
//     parses one JSON record per line.
//  4. Maps each record to a normalized proto.Event and hands it to the
//     Ingester. The mux dedupes against hook events of the same session.
//
// Properties:
//   - Catches sessions that have no hook config installed.
//   - Survives file rotation: if a file shrinks (unlikely) we reset to start.
//   - Survives daemon restarts: we re-scan recent files and replay a bounded
//     tail so active sessions are visible without waiting for the next write.
//   - Bounded work: one tail goroutine per active file, capped by maxOpen.
//   - Never blocks the ingester: per-file goroutines send into a buffered
//     channel that the dispatcher drains into mux.Ingest.
//
// JSONL schema (Claude Code, observed on local box):
//
//	{ sessionId, cwd, version, type: "user"|"assistant"|"tool_use"|"summary"|"system",
//	  message: { role, content }, timestamp?, ... }
package producers

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/maceip/ambient-link-core/host/internal/proto"
)

// JSONLFormat selects the per-line schema we expect in a JSONL stream.
type JSONLFormat string

const (
	// FormatClaude is Claude Code's per-line shape (sessionId, type, message).
	FormatClaude JSONLFormat = "claude"
	// FormatCodex is Codex CLI's per-line shape (timestamp, type, payload).
	FormatCodex JSONLFormat = "codex"
	// FormatCursor is Cursor Agent CLI transcripts under
	// ~/.cursor/projects/<slug>/agent-transcripts/<uuid>/<uuid>.jsonl
	FormatCursor JSONLFormat = "cursor"
)

// JSONLConfig configures the tailer.
type JSONLConfig struct {
	// Root is the directory tree to watch (e.g. ~/.claude/projects or
	// ~/.codex/sessions). Subdirectories are watched recursively.
	Root string
	// Format selects the per-line parser. Default FormatClaude.
	Format JSONLFormat
	// Agent is the tag applied to every produced event ("claude" or "codex").
	Agent string
	// PollFallbackInterval is used when fsnotify isn't available (Plan 9, etc).
	// Default 1s.
	PollFallbackInterval time.Duration
	// MaxOpenFiles caps the number of concurrent tail goroutines. New files
	// beyond this are deferred until an older one goes idle. Default 128.
	MaxOpenFiles int
	// FileIdleClose closes a tail goroutine if no bytes are appended for
	// this long. The watcher will re-open it if new bytes arrive later
	// (via fsnotify Write event). Default 5 minutes.
	FileIdleClose time.Duration
	// StaleAge filters initial-scan candidates: a .jsonl whose mtime is
	// older than this is skipped at startup. fsnotify Write events still
	// re-attach if the file later sees activity. Default 1 hour.
	StaleAge time.Duration
	// StartupReplayBytes is the bounded tail replay used for initial scan.
	// Default 1 MiB. New files still start at byte 0.
	StartupReplayBytes int64
	Logger             *slog.Logger
}

// JSONLTailer runs the tailing pipeline.
type JSONLTailer struct {
	cfg JSONLConfig
	ing Ingester
	wch *fsnotify.Watcher

	mu             sync.Mutex
	open           map[string]*fileTail
	stopped        bool
	deferredWarnAt time.Time // throttle "max open files" warnings to one per minute
	codexState     codexFileState

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// codexFileState caches per-file CWD captured from session_meta records, so
// later lines in the same file can carry the right cwd into the mux without
// re-parsing the file header. Bounded by the set of open file tails (which
// is itself capped by MaxOpenFiles).
type codexFileState struct {
	mu   sync.Mutex
	cwds map[string]string
}

func (c *codexFileState) setCWD(file, val string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cwds == nil {
		c.cwds = make(map[string]string)
	}
	c.cwds[file] = val
}
func (c *codexFileState) getCWD(file string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cwds[file]
}
func (c *codexFileState) forget(file string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cwds, file)
}

// NewJSONLTailer constructs but does not start the tailer; call Run.
func NewJSONLTailer(ing Ingester, cfg JSONLConfig) (*JSONLTailer, error) {
	if ing == nil {
		return nil, errors.New("nil ingester")
	}
	if cfg.Root == "" {
		return nil, errors.New("empty root")
	}
	if cfg.Format == "" {
		cfg.Format = FormatClaude
	}
	if cfg.Agent == "" {
		switch cfg.Format {
		case FormatCodex:
			cfg.Agent = "codex"
		case FormatCursor:
			cfg.Agent = "cursor"
		default:
			cfg.Agent = "claude"
		}
	}
	if cfg.PollFallbackInterval == 0 {
		cfg.PollFallbackInterval = time.Second
	}
	if cfg.MaxOpenFiles == 0 {
		cfg.MaxOpenFiles = 128
	}
	if cfg.FileIdleClose == 0 {
		cfg.FileIdleClose = 5 * time.Minute
	}
	if cfg.StaleAge == 0 {
		cfg.StaleAge = time.Hour
	}
	if cfg.StartupReplayBytes == 0 {
		cfg.StartupReplayBytes = 1024 * 1024
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	t := &JSONLTailer{
		cfg:  cfg,
		ing:  ing,
		open: make(map[string]*fileTail),
	}
	return t, nil
}

// Run starts the tailer and blocks until ctx is cancelled.
func (t *JSONLTailer) Run(ctx context.Context) error {
	if _, err := os.Stat(t.cfg.Root); err != nil {
		// Root may not exist yet if the user hasn't started any claude
		// session. That's fine — we'll bail with a notice; caller can
		// re-launch us once the dir appears.
		t.cfg.Logger.Warn("jsonl: root does not exist; tailer idle", "root", t.cfg.Root)
		<-ctx.Done()
		return nil
	}

	wch, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	t.wch = wch
	defer wch.Close()

	ctx, t.cancel = context.WithCancel(ctx)
	defer t.cancel()

	// Watch every existing subdirectory; new ones are watched as they appear.
	if err := t.watchTree(t.cfg.Root); err != nil {
		return err
	}
	// Open all existing .jsonl files, but tail from EOF (we don't replay
	// history; the mux only cares about going-forward state).
	t.scanAndOpen(t.cfg.Root)

	for {
		select {
		case <-ctx.Done():
			t.closeAll()
			t.wg.Wait()
			return nil

		case ev, ok := <-wch.Events:
			if !ok {
				return nil
			}
			t.handleFsEvent(ctx, ev)

		case err, ok := <-wch.Errors:
			if !ok {
				return nil
			}
			t.cfg.Logger.Warn("jsonl: fsnotify error", "err", err)
		}
	}
}

func (t *JSONLTailer) handleFsEvent(ctx context.Context, ev fsnotify.Event) {
	switch {
	case ev.Op&fsnotify.Create != 0:
		info, err := os.Stat(ev.Name)
		if err != nil {
			return
		}
		if info.IsDir() {
			_ = t.wch.Add(ev.Name)
			// Stat-walk in case there were already .jsonl files inside.
			t.scanAndOpen(ev.Name)
			return
		}
		if strings.HasSuffix(ev.Name, ".jsonl") {
			if !shouldTailCursorPath(t.cfg.Format, ev.Name) {
				return
			}
			t.openFile(ctx, ev.Name, true, 0) // new file, start at 0
		}

	case ev.Op&fsnotify.Write != 0:
		if strings.HasSuffix(ev.Name, ".jsonl") {
			if !shouldTailCursorPath(t.cfg.Format, ev.Name) {
				return
			}
			t.openFile(ctx, ev.Name, false, 0) // existing file write: pick up at EOF
		}

	case ev.Op&fsnotify.Remove != 0, ev.Op&fsnotify.Rename != 0:
		t.closeFile(ev.Name)
	}
}

func (t *JSONLTailer) watchTree(root string) error {
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // best-effort
		}
		if d.IsDir() {
			_ = t.wch.Add(p)
		}
		return nil
	})
}

// scanAndOpen attaches to files written within StaleAge and replays a bounded
// tail so daemon restarts recover recent session state. Historical .jsonl files
// are skipped to keep the open-fd budget for actually-active sessions. If a
// stale file later gets a Write event from fsnotify, openFile will pick it up.
func (t *JSONLTailer) scanAndOpen(root string) {
	cutoff := time.Now().Add(-t.cfg.StaleAge)
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(p, ".jsonl") {
			return nil
		}
		if t.cfg.Format == FormatCursor && !shouldTailCursorPath(t.cfg.Format, p) {
			return nil
		}
		info, err := d.Info()
		if err != nil || info.ModTime().Before(cutoff) {
			return nil
		}
		t.openFile(context.Background(), p, false, t.cfg.StartupReplayBytes)
		return nil
	})
}

// Attach force-opens one transcript regardless of the initial-scan StaleAge
// window, replaying a bounded tail so the session's cwd/label/preview come
// back. Used when the proc watcher sees a live process for a session the mux
// doesn't know — e.g. a quiet-but-alive agent after a relay restart, which
// otherwise stays invisible even though its delivery endpoint is registered.
func (t *JSONLTailer) Attach(path string) {
	if path == "" {
		return
	}
	if _, err := os.Stat(path); err != nil {
		return
	}
	t.openFile(context.Background(), path, false, t.cfg.StartupReplayBytes)
}

func (t *JSONLTailer) openFile(ctx context.Context, path string, fromStart bool, replayBytes int64) {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}
	if _, ok := t.open[path]; ok {
		// Already tailing.
		t.mu.Unlock()
		return
	}
	if len(t.open) >= t.cfg.MaxOpenFiles {
		throttleWarn := time.Since(t.deferredWarnAt) > time.Minute
		if throttleWarn {
			t.deferredWarnAt = time.Now()
		}
		t.mu.Unlock()
		if throttleWarn {
			t.cfg.Logger.Warn("jsonl: max open files reached, deferring further opens", "open", t.cfg.MaxOpenFiles, "example", path)
		}
		return
	}
	ft := &fileTail{path: path, fromStart: fromStart, replayBytes: replayBytes, done: make(chan struct{})}
	t.open[path] = ft
	t.wg.Add(1)
	t.mu.Unlock()

	go t.tailLoop(ctx, ft)
}

func (t *JSONLTailer) closeFile(path string) {
	t.mu.Lock()
	ft, ok := t.open[path]
	if ok {
		delete(t.open, path)
	}
	t.mu.Unlock()
	if ok {
		ft.stop()
	}
	t.codexState.forget(path)
}

func (t *JSONLTailer) closeAll() {
	t.mu.Lock()
	t.stopped = true
	tails := make([]*fileTail, 0, len(t.open))
	for _, ft := range t.open {
		tails = append(tails, ft)
	}
	t.open = map[string]*fileTail{}
	t.mu.Unlock()
	for _, ft := range tails {
		ft.stop()
	}
}

// tailLoop owns one file. It seeks to EOF, byte 0, or a bounded replay window,
// then reads appended JSONL records until the file goes idle or ctx is done.
func (t *JSONLTailer) tailLoop(ctx context.Context, ft *fileTail) {
	defer t.wg.Done()
	defer func() {
		t.mu.Lock()
		delete(t.open, ft.path)
		t.mu.Unlock()
	}()

	f, err := os.Open(ft.path)
	if err != nil {
		t.cfg.Logger.Warn("jsonl: open failed", "path", ft.path, "err", err)
		return
	}
	defer f.Close()

	if t.cfg.Format == FormatCodex && !ft.fromStart {
		t.primeCodexCWD(ft.path)
	}

	discardPartial := false
	if !ft.fromStart {
		if ft.replayBytes > 0 {
			info, err := f.Stat()
			if err != nil {
				t.cfg.Logger.Warn("jsonl: stat failed", "path", ft.path, "err", err)
				return
			}
			start := info.Size() - ft.replayBytes
			if start > 0 {
				if _, err := f.Seek(start, io.SeekStart); err != nil {
					t.cfg.Logger.Warn("jsonl: seek replay failed", "path", ft.path, "err", err)
					return
				}
				discardPartial = true
			} else if _, err := f.Seek(0, io.SeekStart); err != nil {
				t.cfg.Logger.Warn("jsonl: seek start failed", "path", ft.path, "err", err)
				return
			}
		} else {
			if _, err := f.Seek(0, io.SeekEnd); err != nil {
				t.cfg.Logger.Warn("jsonl: seek end failed", "path", ft.path, "err", err)
				return
			}
		}
	}

	rd := bufio.NewReaderSize(f, 64*1024)
	if discardPartial {
		_, _ = rd.ReadString('\n')
	}
	idleSince := time.Now()
	poll := time.NewTicker(t.cfg.PollFallbackInterval)
	defer poll.Stop()

	for {
		line, err := rd.ReadString('\n')
		if line != "" {
			idleSince = time.Now()
			t.dispatchLine(ft.path, strings.TrimRight(line, "\r\n"))
		}
		if err == io.EOF {
			select {
			case <-ctx.Done():
				return
			case <-ft.done:
				return
			case <-poll.C:
				if time.Since(idleSince) > t.cfg.FileIdleClose {
					return
				}
				continue
			}
		}
		if err != nil {
			t.cfg.Logger.Warn("jsonl: read failed", "path", ft.path, "err", err)
			return
		}
	}
}

// dispatchLine parses one JSONL record (in whichever Format) and turns it
// into a proto.Event. Unknown shapes are dropped silently — JSONL is full
// of housekeeping lines we don't care about.
func (t *JSONLTailer) dispatchLine(file string, line string) {
	if line == "" {
		return
	}
	switch t.cfg.Format {
	case FormatCodex:
		t.dispatchCodex(file, line)
	case FormatCursor:
		t.dispatchCursor(file, line)
	default:
		t.dispatchClaude(line)
	}
}

func (t *JSONLTailer) dispatchClaude(line string) {
	var rec claudeRecord
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.cfg.Logger.Debug("jsonl: bad claude line", "err", err)
		return
	}
	if rec.SessionID == "" {
		return
	}
	eventType := classifyClaude(&rec)
	if eventType == "" {
		return
	}
	at := time.Now().UnixMilli()
	if rec.Timestamp != "" {
		if ts, err := time.Parse(time.RFC3339Nano, rec.Timestamp); err == nil {
			at = ts.UnixMilli()
		}
	}
	_ = t.ing.Ingest(proto.Event{
		SessionID: rec.SessionID, Agent: t.cfg.Agent, CWD: rec.CWD,
		Type: eventType, Payload: decodeRawPayload(rec.Message),
		Source: proto.ProducerJSONL, ObservedAt: at,
	})
}

// decodeRawPayload turns a raw JSON fragment into the map/string shapes the
// mux's extractText understands. Passing json.RawMessage through untouched
// yields a []byte extractText can't read, so snippets (preview, last
// assistant/user text, landed confirmation) silently come out empty.
func decodeRawPayload(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil
	}
	return v
}

// dispatchCodex parses one line of a Codex rollout-*.jsonl file. Session ID
// is extracted from the filename (Codex doesn't repeat it per line); cwd
// comes from the first session_meta record we see (we buffer it per file).
func (t *JSONLTailer) dispatchCodex(file, line string) {
	var rec codexRecord
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.cfg.Logger.Debug("jsonl: bad codex line", "err", err)
		return
	}
	sid := codexSessionIDFromPath(file)
	if sid == "" {
		return
	}
	if rec.Type == "session_meta" {
		// Capture cwd for this file so subsequent lines have CWD context.
		if cwd, _ := payloadString(rec.Payload, "cwd"); cwd != "" {
			t.codexState.setCWD(file, cwd)
		}
	}
	eventType, eventPayload := classifyCodex(&rec)
	if eventType == "" {
		return
	}
	if rm, ok := eventPayload.(json.RawMessage); ok {
		eventPayload = decodeRawPayload(rm)
	}
	cwd := t.codexState.getCWD(file)
	at := time.Now().UnixMilli()
	if rec.Timestamp != "" {
		if ts, err := time.Parse(time.RFC3339Nano, rec.Timestamp); err == nil {
			at = ts.UnixMilli()
		}
	}
	_ = t.ing.Ingest(proto.Event{
		SessionID: sid, Agent: t.cfg.Agent, CWD: cwd,
		Type: eventType, Payload: eventPayload,
		Source: proto.ProducerJSONL, ObservedAt: at,
	})
}

func (t *JSONLTailer) primeCodexCWD(file string) {
	if t.codexState.getCWD(file) != "" {
		return
	}
	f, err := os.Open(file)
	if err != nil {
		return
	}
	defer f.Close()
	rd := bufio.NewReaderSize(io.LimitReader(f, 4*1024*1024), 64*1024)
	line, err := rd.ReadString('\n')
	if err != nil && line == "" {
		return
	}
	var rec codexRecord
	if err := json.Unmarshal([]byte(strings.TrimRight(line, "\r\n")), &rec); err != nil {
		return
	}
	if rec.Type != "session_meta" {
		return
	}
	if cwd, _ := payloadString(rec.Payload, "cwd"); cwd != "" {
		t.codexState.setCWD(file, cwd)
	}
}

// classifyClaude maps Claude's record types into normalized events.
func classifyClaude(rec *claudeRecord) proto.EventType {
	switch rec.Type {
	case "user":
		return proto.EventUserPrompt
	case "assistant":
		return proto.EventAssistantMessage
	case "tool_use", "tool_result":
		return proto.EventToolUse
	case "summary", "session_end":
		return proto.EventSessionEnd
	}
	return ""
}

// classifyCodex maps Codex's nested {type, payload.type} into normalized
// events. Returns the inner payload as the event payload for snippet
// extraction.
func classifyCodex(rec *codexRecord) (proto.EventType, any) {
	switch rec.Type {
	case "session_meta":
		return proto.EventSessionStart, rec.Payload
	case "event_msg":
		inner, _ := payloadString(rec.Payload, "type")
		switch inner {
		case "task_started":
			return proto.EventUserPrompt, rec.Payload
		case "task_complete", "task_finished":
			return proto.EventStop, rec.Payload
		case "agent_message", "agent_reasoning", "stream_event":
			return proto.EventAssistantMessage, rec.Payload
		case "user_message":
			return proto.EventUserPrompt, rec.Payload
		case "exec_command_begin", "exec_command_end",
			"tool_call_begin", "tool_call_end", "patch_apply_begin", "patch_apply_end":
			return proto.EventToolUse, rec.Payload
		case "approval_request", "exec_approval_request":
			return proto.EventPermissionPrompt, rec.Payload
		case "session_end":
			return proto.EventSessionEnd, rec.Payload
		}
	}
	return "", nil
}

// claudeRecord captures the small subset of Claude's per-line schema we
// actually consume. Unknown fields are ignored.
type claudeRecord struct {
	SessionID string          `json:"sessionId"`
	CWD       string          `json:"cwd"`
	Type      string          `json:"type"`
	Timestamp string          `json:"timestamp"`
	Message   json.RawMessage `json:"message"`
}

// codexRecord is Codex's wire shape.
type codexRecord struct {
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

// codexSessionIDFromPath extracts the UUID from a Codex rollout-...jsonl
// filename. Returns "" if no UUID found.
func codexSessionIDFromPath(file string) string {
	base := filepath.Base(file)
	if m := codexFileUUID.FindStringSubmatch(base); m != nil {
		return m[1]
	}
	return ""
}

var codexFileUUID = regexp.MustCompile(`([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$`)

var cursorTranscriptDir = regexp.MustCompile(`/agent-transcripts/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/`)

var cursorProjectDir = regexp.MustCompile(`/\.cursor/projects/([^/]+)/agent-transcripts/`)

func shouldTailCursorPath(format JSONLFormat, path string) bool {
	if format != FormatCursor {
		return true
	}
	if strings.Contains(filepath.ToSlash(path), "/subagents/") {
		return false
	}
	return cursorSessionIDFromPath(path) != ""
}

func (t *JSONLTailer) dispatchCursor(file, line string) {
	sid := cursorSessionIDFromPath(file)
	if sid == "" {
		return
	}
	var rec cursorRecord
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.cfg.Logger.Debug("jsonl: bad cursor line", "err", err)
		return
	}
	eventType, payload := classifyCursor(&rec)
	if eventType == "" {
		return
	}
	cwd := cursorCWDFromPath(file)
	_ = t.ing.Ingest(proto.Event{
		SessionID: sid, Agent: t.cfg.Agent, CWD: cwd,
		Type: eventType, Payload: payload,
		Source: proto.ProducerJSONL, ObservedAt: time.Now().UnixMilli(),
	})
}

type cursorRecord struct {
	Role    string          `json:"role"`
	Message json.RawMessage `json:"message"`
}

func classifyCursor(rec *cursorRecord) (proto.EventType, any) {
	switch rec.Role {
	case "user":
		return proto.EventUserPrompt, cursorMessageText(rec.Message)
	case "assistant":
		return proto.EventAssistantMessage, map[string]any{"message": cursorMessageText(rec.Message)}
	}
	return "", nil
}

func cursorMessageText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var m struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}
	var b strings.Builder
	for _, c := range m.Content {
		if c.Type != "text" || c.Text == "" || isCursorRedactedSnippet(c.Text) {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(c.Text)
	}
	return b.String()
}

// Cursor JSONL often interleaves tool_use blocks with placeholder text chunks.
func isCursorRedactedSnippet(s string) bool {
	return strings.TrimSpace(s) == "[REDACTED]"
}

func cursorSessionIDFromPath(file string) string {
	m := cursorTranscriptDir.FindStringSubmatch(filepath.ToSlash(file))
	if m == nil {
		return ""
	}
	id := m[1]
	base := strings.TrimSuffix(filepath.Base(file), ".jsonl")
	if base != id {
		return ""
	}
	return id
}

func cursorCWDFromPath(file string) string {
	m := cursorProjectDir.FindStringSubmatch(filepath.ToSlash(file))
	if m == nil {
		return ""
	}
	slug := m[1]
	// Windows project slugs carry a leading drive-letter segment, e.g.
	// "C-Users-mac-proj"; strip it so the "Users-" decode below applies.
	if len(slug) >= 2 && slug[1] == '-' {
		if c := slug[0]; (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
			slug = slug[2:]
		}
	}
	if !strings.HasPrefix(slug, "Users-") {
		return ""
	}
	body := strings.TrimPrefix(slug, "Users-")
	i := strings.Index(body, "-")
	if i < 0 {
		return ""
	}
	return path.Join("/Users", body[:i], body[i+1:])
}

// payloadString pulls a string field from a json.RawMessage payload.
func payloadString(p json.RawMessage, key string) (string, bool) {
	if len(p) == 0 {
		return "", false
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(p, &m); err != nil {
		return "", false
	}
	raw, ok := m[key]
	if !ok {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

// fileTail tracks one open .jsonl file.
type fileTail struct {
	path        string
	fromStart   bool
	replayBytes int64
	done        chan struct{}
	once        sync.Once
}

func (ft *fileTail) stop() {
	ft.once.Do(func() { close(ft.done) })
}
