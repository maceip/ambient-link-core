// Command host runs the laptop-side daemon that produces session events for
// the face-chat mobile relays. It wires the SessionMux to three parallel
// producers (hooks ingest, JSONL tailer, process watcher) and a WS sink.
//
// Usage:
//
//	host serve         start the daemon (default)
//	host install       write hook configs and a launchd/systemd unit
//	host uninstall     reverse of install
//	host status        query a running daemon over its local HTTP API
//
// Run `host -h` for flags.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/maceip/ambient-link-core/host/internal/delivery"
	"github.com/maceip/ambient-link-core/host/internal/discovery"
	"github.com/maceip/ambient-link-core/host/internal/installer"
	"github.com/maceip/ambient-link-core/host/internal/inject"
	"github.com/maceip/ambient-link-core/host/internal/journal"
	"github.com/maceip/ambient-link-core/host/internal/mux"
	"github.com/maceip/ambient-link-core/host/internal/pair"
	"github.com/maceip/ambient-link-core/host/internal/proto"
	"github.com/maceip/ambient-link-core/host/internal/producers"
	"github.com/maceip/ambient-link-core/host/internal/sink"
	"github.com/maceip/ambient-link-core/host/internal/webapp"
)

const (
	defaultListen = "0.0.0.0:5181"
	shutdownGrace = 5 * time.Second
)

func main() {
	cmd := "serve"
	if len(os.Args) > 1 && os.Args[1] != "" && os.Args[1][0] != '-' {
		cmd = os.Args[1]
		os.Args = append([]string{os.Args[0]}, os.Args[2:]...)
	}
	switch cmd {
	case "serve":
		if err := runServe(); err != nil {
			fatal(err)
		}
	case "install":
		if err := runInstall(); err != nil {
			fatal(err)
		}
	case "uninstall":
		if err := runUninstall(); err != nil {
			fatal(err)
		}
	case "status":
		if err := runStatus(); err != nil {
			fatal(err)
		}
	case "pair":
		if err := runPair(); err != nil {
			fatal(err)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\nrun `host -h` for help\n", cmd)
		os.Exit(64)
	}
}

func runInstall() error {
	var (
		hostURL = flag.String("host-url", pair.DefaultHookURL(), "URL the agent hooks POST to")
		token   = flag.String("token", "", "bearer token (auto-generated if empty)")
		noSvc   = flag.Bool("no-service", false, "skip writing launchd/systemd unit")
	)
	flag.Parse()

	res, err := installer.Install(installer.Options{
		HostURL:     *hostURL,
		BearerToken: *token,
		SkipService: *noSvc,
	})
	if err != nil {
		return err
	}
	fmt.Println("host install — summary")
	fmt.Println("  claude settings:", boolWord(res.ClaudeSettingsModified, "wrote", "already current"))
	fmt.Println("  codex hooks:    ", boolWord(res.CodexHooksModified, "wrote", "already current"))
	if res.ServiceUnitPath != "" {
		fmt.Println("  service unit:   ", res.ServiceUnitPath, boolWord(res.ServiceUnitCreated, "(wrote)", "(already current)"))
	}
	fmt.Println("  bearer token:   ", res.BearerToken)
	fmt.Println()
	fmt.Println("next: restart the host daemon with `-token`", res.BearerToken)
	fmt.Println("      then start a `claude` or `codex` session — events should arrive at", *hostURL+"/face-chat/status")
	return nil
}

func runUninstall() error {
	if err := installer.Uninstall(installer.Options{}); err != nil {
		return err
	}
	fmt.Println("host uninstall — reverted hook entries and removed service unit (if any)")
	return nil
}

func runStatus() error {
	url := flag.String("host-url", "http://127.0.0.1:5181", "running host's status URL base")
	flag.Parse()
	resp, err := http.Get(*url + "/face-chat/status")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(os.Stdout, resp.Body)
	return err
}

func boolWord(b bool, yes, no string) string {
	if b {
		return yes
	}
	return no
}

func runServe() error {
	var (
		listen          = flag.String("listen", defaultListen, "host:port for the HTTP+WS server")
		token           = flag.String("token", "", "bearer token required on /face-chat/hooks/* POSTs (empty = disabled)")
		logLvl          = flag.String("log", "info", "log level: debug | info | warn | error")
		relayDebug      = flag.Bool("relay-debug", false, "suppress auto thread_idle/busy to clients; explicit hud_yank only")
		webRoot         = flag.String("web-root", defaultWebRoot(), "glasses companion SPA directory (empty = disabled)")
		jsonlRoot      = flag.String("jsonl-root", defaultClaudeJSONLRoot(), "root of Claude Code session JSONL files; empty disables Claude JSONL tailer")
		codexJSONLRoot = flag.String("codex-jsonl-root", defaultCodexJSONLRoot(), "root of Codex session JSONL files; empty disables Codex JSONL tailer")
		cursorJSONLRoot = flag.String("cursor-jsonl-root", defaultCursorJSONLRoot(), "root of Cursor Agent transcript JSONL; empty disables Cursor JSONL tailer")
	)
	flag.Parse()

	logger := newLogger(*logLvl)

	hub := sink.NewHub(logger)
	hub.SetBearerToken(*token)
	hub.SetRelayDebug(*relayDebug)
	jlog, err := journal.Open()
	if err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	hub.SetJournal(jlog)
	m := mux.New(hub, mux.Options{Logger: logger})
	hub.SetMux(m)

	reg := delivery.NewRegistry()
	box := delivery.NewOutbox()
	delivery.SetLogger(logger)
	inject.Init(m, reg, box)

	hooksHandler := producers.NewHooks(m, producers.HooksConfig{
		BearerToken: *token,
		Logger:      logger,
		Outbox:      box,
	})
	ingestHandler := producers.NewIngest(m, producers.IngestConfig{Logger: logger})

	root := http.NewServeMux()
	root.Handle("/face-chat/ws", hub)
	root.Handle("/face-chat/hooks/", hooksHandler)
	root.Handle("/face-chat/ingest", ingestHandler)
	root.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	root.HandleFunc("/face-chat/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, map[string]any{
			"sessions":    m.Snapshot(),
			"delivery":    reg.Snapshot(),
			"relay_debug": *relayDebug,
			"journal":     jlog.Head(),
			"now":         time.Now().UnixMilli(),
		})
	})
	root.HandleFunc("/face-chat/pair", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		p, err := pair.Build(*listen, *token, "ws")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, p)
	})
	// Debug: pop glasses HUD from curl (keyboard / automation, no phone UI).
	root.HandleFunc("/face-chat/debug/yank", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Thread        string `json:"thread"`
			Label         string `json:"label"`
			Agent         string `json:"agent"`
			LastAssistant string `json:"lastAssistant"`
			Awaiting      string `json:"awaiting"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Thread == "" {
			req.Thread = "cursor"
		}
		if req.Label == "" {
			req.Label = req.Thread
		}
		if req.Agent == "" {
			req.Agent = "cursor"
		}
		if req.Awaiting == "" {
			req.Awaiting = "done"
		}
		if req.LastAssistant == "" {
			http.Error(w, "lastAssistant required", http.StatusBadRequest)
			return
		}
		hub.Broadcast(proto.Broadcast{
			Type:          proto.BroadcastHudYank,
			Thread:        req.Thread,
			Label:         req.Label,
			Agent:         req.Agent,
			LastAssistant: req.LastAssistant,
			Awaiting:      req.Awaiting,
			At:            time.Now().UnixMilli(),
		})
		writeJSON(w, map[string]string{"ok": "yank"})
	})
	root.HandleFunc("/face-chat/debug/input", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Thread string `json:"thread"`
			Text   string `json:"text"`
			Enter  *bool  `json:"enter"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		enter := true
		if req.Enter != nil {
			enter = *req.Enter
		}
		if req.Thread == "" || req.Text == "" {
			http.Error(w, "thread and text required", http.StatusBadRequest)
			return
		}
		if err := inject.SendInput(req.Thread, req.Text, enter); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		_ = m.IngestUserInput(req.Thread, req.Text)
		writeJSON(w, map[string]string{"ok": "input"})
	})
	if *webRoot != "" {
		if st, err := os.Stat(*webRoot); err != nil || !st.IsDir() {
			return fmt.Errorf("web-root %q: %w", *webRoot, err)
		}
		root.Handle("/", webapp.Dir(*webRoot))
		logger.Info("host: serving web companion", "root", *webRoot)
	}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           root,
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := discovery.Advertise(ctx, *listen, *token, logger); err != nil {
		logger.Warn("host: mdns advertise failed", "err", err)
	} else {
		logger.Info("host: mdns advertising", "type", discovery.ServiceType)
	}

	// JSONL tailers — one per agent, both always-on. They share the mux,
	// so a session that fires both hook events and JSONL writes gets deduped
	// by the mux on (session_id, event_type) within DedupWindow.
	if *jsonlRoot != "" {
		if err := startJSONLTailer(ctx, m, *jsonlRoot, producers.FormatClaude, "claude", logger); err != nil {
			return err
		}
	}
	if *codexJSONLRoot != "" {
		if err := startJSONLTailer(ctx, m, *codexJSONLRoot, producers.FormatCodex, "codex", logger); err != nil {
			return err
		}
	}
	if *cursorJSONLRoot != "" {
		if err := startJSONLTailer(ctx, m, *cursorJSONLRoot, producers.FormatCursor, "cursor", logger); err != nil {
			return err
		}
	}

	// Process watcher
	procW, err := producers.NewProcWatcher(m, producers.ProcConfig{
		Logger:   logger,
		Registry: reg,
		OnSessionLive: func(sessionID string) {
			if delivery.RetryPending(sessionID, reg, box) {
				logger.Info("delivery: retry on live session", "session", sessionID)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("proc watcher: %w", err)
	}
	go func() {
		if err := procW.Run(ctx); err != nil {
			logger.Error("proc: watcher exited", "err", err)
		}
	}()
	logger.Info("proc: watcher started")

	go func() {
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if n := delivery.FlushPending(reg, box); n > 0 {
					logger.Info("delivery: retry flush", "count", n)
				}
			}
		}
	}()

	// Stale-session reaper
	go runStaleReaper(ctx, m, 30*time.Minute, 5*time.Minute, logger)

	go func() {
		logger.Info("host: listening",
			"addr", *listen,
			"endpoints", []string{
				"/face-chat/ws",
				"/face-chat/ingest",
				"/face-chat/pair",
				"/face-chat/hooks/claude",
				"/face-chat/hooks/codex",
				"/face-chat/status",
				"/healthz",
			},
			"auth", boolStr(*token != ""),
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("host: listen failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("host: shutdown signal received")

	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		logger.Warn("host: shutdown error", "err", err)
	}
	m.Close()
	logger.Info("host: stopped")
	return nil
}

// newLogger returns a slog logger writing JSON to stderr at the requested
// level. Unknown levels default to info.
// runStaleReaper periodically calls mux.SweepStale to retire sessions that
// have had no events for at least maxIdle. It's the catch-all behind the
// PID-based proc watcher.
func runStaleReaper(ctx context.Context, m *mux.Mux, maxIdle, interval time.Duration, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := m.SweepStale(maxIdle); n > 0 {
				log.Info("mux: reaped stale sessions", "count", n, "max_idle", maxIdle.String())
			}
		}
	}
}

// defaultClaudeJSONLRoot is ~/.claude/projects on Unix-likes; empty if HOME is unset.
func defaultClaudeJSONLRoot() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return home + "/.claude/projects"
}

// defaultCodexJSONLRoot is ~/.codex/sessions on Unix-likes.
func defaultCodexJSONLRoot() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return home + "/.codex/sessions"
}

// defaultCursorJSONLRoot is ~/.cursor/projects on Unix-likes.
func defaultCursorJSONLRoot() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return home + "/.cursor/projects"
}

func defaultWebRoot() string {
	// Sibling repo layout: ambient-link-core/host ↔ ambient-link-meta/web
	if home, err := os.UserHomeDir(); err == nil {
		candidate := home + "/ambient-link-meta/web"
		if st, err := os.Stat(candidate); err == nil && st.IsDir() {
			return candidate
		}
	}
	// Dev: host/cmd/host → ../../../ambient-link-meta/web won't work from module;
	// try relative to cwd when launched from ambient-link-meta.
	if st, err := os.Stat("../ambient-link-meta/web"); err == nil && st.IsDir() {
		abs, _ := filepath.Abs("../ambient-link-meta/web")
		return abs
	}
	if st, err := os.Stat("../../ambient-link-meta/web"); err == nil && st.IsDir() {
		abs, _ := filepath.Abs("../../ambient-link-meta/web")
		return abs
	}
	return "/Users/mac/ambient-link-meta/web"
}

func runPair() error {
	statusURL := flag.String("status-url", "http://127.0.0.1:5181", "running daemon base URL")
	listen := flag.String("listen", defaultListen, "listen addr if daemon is down")
	token := flag.String("token", "", "bearer token if daemon is down")
	flag.Parse()

	var p pair.Payload
	var err error
	if payload, fetchErr := pair.FetchRunning(*statusURL); fetchErr == nil {
		p = payload
		fmt.Println("(from running daemon)")
	} else {
		p, err = pair.Build(*listen, *token, "ws")
		if err != nil {
			return err
		}
		fmt.Println("(generated offline — start daemon with -listen 0.0.0.0:5181)")
	}
	fmt.Println()
	fmt.Println("Pair URL (QR / phone scan):")
	fmt.Println(p.PairURL)
	fmt.Println()
	fmt.Println("WebSocket:", p.WSURL)
	if p.Token != "" {
		fmt.Println("Token:    ", p.Token)
	}
	fmt.Println()
	fmt.Println("Glasses web app:", strings.Replace(*statusURL, "http://", "http://", 1)+"/")
	return nil
}

// startJSONLTailer constructs and launches a tailer goroutine for one agent.
func startJSONLTailer(ctx context.Context, m *mux.Mux, root string, format producers.JSONLFormat, agent string, log *slog.Logger) error {
	tailer, err := producers.NewJSONLTailer(m, producers.JSONLConfig{
		Root:   root,
		Format: format,
		Agent:  agent,
		Logger: log,
	})
	if err != nil {
		return fmt.Errorf("jsonl tailer (%s): %w", agent, err)
	}
	go func() {
		if err := tailer.Run(ctx); err != nil {
			log.Error("jsonl: tailer exited", "agent", agent, "err", err)
		}
	}()
	log.Info("jsonl: tailer started", "agent", agent, "root", root)
	return nil
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

func boolStr(b bool) string {
	if b {
		return "required"
	}
	return "disabled"
}

func writeJSON(w http.ResponseWriter, v any) {
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "encode", http.StatusInternalServerError)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "host:", err)
	os.Exit(1)
}
