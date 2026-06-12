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
	"syscall"
	"time"

	"github.com/maceip/ambient-link-core/host/internal/installer"
	"github.com/maceip/ambient-link-core/host/internal/mux"
	"github.com/maceip/ambient-link-core/host/internal/producers"
	"github.com/maceip/ambient-link-core/host/internal/sink"
)

const (
	defaultListen = "127.0.0.1:5181"
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
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\nrun `host -h` for help\n", cmd)
		os.Exit(64)
	}
}

func runInstall() error {
	var (
		hostURL = flag.String("host-url", "http://127.0.0.1:5181", "URL the agent hooks POST to")
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
		listen     = flag.String("listen", defaultListen, "host:port for the HTTP+WS server")
		token      = flag.String("token", "", "bearer token required on /face-chat/hooks/* POSTs (empty = disabled)")
		logLvl     = flag.String("log", "info", "log level: debug | info | warn | error")
		jsonlRoot      = flag.String("jsonl-root", defaultClaudeJSONLRoot(), "root of Claude Code session JSONL files; empty disables Claude JSONL tailer")
		codexJSONLRoot = flag.String("codex-jsonl-root", defaultCodexJSONLRoot(), "root of Codex session JSONL files; empty disables Codex JSONL tailer")
	)
	flag.Parse()

	logger := newLogger(*logLvl)

	hub := sink.NewHub(logger)
	m := mux.New(hub, mux.Options{Logger: logger})
	hub.SetSnapshotSource(m.Snapshot)

	hooksHandler := producers.NewHooks(m, producers.HooksConfig{
		BearerToken: *token,
		Logger:      logger,
	})

	root := http.NewServeMux()
	root.Handle("/face-chat/ws", hub)
	root.Handle("/face-chat/hooks/", hooksHandler)
	root.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	root.HandleFunc("/face-chat/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeJSON(w, map[string]any{
			"sessions": m.Snapshot(),
			"now":      time.Now().UnixMilli(),
		})
	})

	srv := &http.Server{
		Addr:              *listen,
		Handler:           root,
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	// Process watcher (correlates agent PIDs to session JSONL files; marks
	// sessions dead when their owning PID disappears).
	procW, err := producers.NewProcWatcher(m, producers.ProcConfig{Logger: logger})
	if err != nil {
		return fmt.Errorf("proc watcher: %w", err)
	}
	go func() {
		if err := procW.Run(ctx); err != nil {
			logger.Error("proc: watcher exited", "err", err)
		}
	}()
	logger.Info("proc: watcher started")

	// Stale-session reaper — defense in depth behind the proc watcher.
	go runStaleReaper(ctx, m, 30*time.Minute, 5*time.Minute, logger)

	go func() {
		logger.Info("host: listening",
			"addr", *listen,
			"endpoints", []string{
				"/face-chat/ws",
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
