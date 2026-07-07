// Command host runs the laptop-side daemon that produces session events for
// the ambient-link mobile relays. It wires the SessionMux to three parallel
// producers (hooks ingest, JSONL tailer, process watcher) and a WS sink.
//
// Usage — just run it. `host` (or `host serve`) starts the daemon with sane
// defaults and needs no flags. The rare overrides are environment variables so
// the command line stays empty:
//
//	AMBIENT_LINK_LISTEN       bind address (default 0.0.0.0:5181)
//	AMBIENT_LINK_TOKEN        bearer token for /hooks + WS (default: disabled)
//	AMBIENT_LINK_LOG          log level: debug|info|warn|error (default info)
//	AMBIENT_LINK_RELAY_DEBUG  1 to suppress auto idle/busy cards
//	AMBIENT_LINK_WEB_ROOT     override path to the glasses web app to serve
//	AMBIENT_LINK_CLOUD        wss:// cloud relay to bridge to (G5; off if unset)
//	AMBIENT_LINK_HOME         state dir for relay.db + outbox (default ~/.ambient-link)
//
// Other subcommands:
//
//	host run <agent>   launch an agent under a relay-owned PTY (reliable delivery)
//	host install       write hook configs and a launchd/systemd unit
//	host uninstall     reverse of install
//	host status        query a running daemon over its local HTTP API
//
// All agent↔human interaction is recorded in a local SQLite database
// (~/.ambient-link/relay.db). The native app remains source of truth; the DB is
// a durable, reconcilable mirror. See DECISIONS.md for the full rationale.
//
// Only one relay runs at a time: it binds the port as its lock and, if the port
// is already taken, prints which PID to stop. It never kills another process.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/maceip/ambient-link-core/host/internal/cloud"
	"github.com/maceip/ambient-link-core/host/internal/delivery"
	"github.com/maceip/ambient-link-core/host/internal/discovery"
	"github.com/maceip/ambient-link-core/host/internal/inject"
	"github.com/maceip/ambient-link-core/host/internal/installer"
	"github.com/maceip/ambient-link-core/host/internal/mux"
	"github.com/maceip/ambient-link-core/host/internal/pair"
	"github.com/maceip/ambient-link-core/host/internal/producers"
	"github.com/maceip/ambient-link-core/host/internal/proto"
	"github.com/maceip/ambient-link-core/host/internal/pty"
	"github.com/maceip/ambient-link-core/host/internal/sink"
	"github.com/maceip/ambient-link-core/host/internal/store"
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
	case "run":
		if err := runAgent(os.Args[1:]); err != nil {
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
	fmt.Println("      then start a `claude` or `codex` session — events should arrive at", *hostURL+"/ambient-link/status")
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
	resp, err := http.Get(*url + "/ambient-link/status")
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
	flag.Parse()

	// No flags: bare `host` just works. The handful of rare overrides live in
	// environment variables so the command line stays empty.
	listen := envOr("AMBIENT_LINK_LISTEN", defaultListen)
	token := os.Getenv("AMBIENT_LINK_TOKEN")
	relayDebug := envBool("AMBIENT_LINK_RELAY_DEBUG")
	// AMBIENT_LINK_ROLE=proxy → Cloud Assist proxy (RESTART-DECISION Option A):
	// no tailers, no proc watcher, no reaper, no local ingestion, no mDNS.
	// Sessions exist only while a laptop peer is connected and are dropped the
	// moment it disconnects. The cloud never invents or retains state.
	proxyRole := strings.EqualFold(strings.TrimSpace(os.Getenv("AMBIENT_LINK_ROLE")), "proxy")
	webRoot := defaultWebRoot()
	jsonlRoot := defaultClaudeJSONLRoot()
	codexJSONLRoot := defaultCodexJSONLRoot()
	cursorJSONLRoot := defaultCursorJSONLRoot()

	logger := newLogger(envOr("AMBIENT_LINK_LOG", "info"))

	// Single-instance lock — intentionally dumb. Binding the port IS the lock:
	// if it's taken, another relay (or process) already owns it, so we say so
	// and exit. No killing, no version arbitration. To run a newer build, stop
	// the old one first. A lock file records our PID so the message can name it.
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		msg := fmt.Sprintf("cannot start: %s is already in use", listen)
		if pid := readLockPID(); pid > 0 {
			msg = fmt.Sprintf("a relay is already running (pid %d). Stop it first, then run `host` again.", pid)
		}
		fmt.Fprintln(os.Stderr, "host:", msg)
		return nil
	}
	writeLock(listen)
	defer removeLock()
	logger.Info("relay: starting", "pid", os.Getpid(), "rev", buildRev(), "addr", listen)

	hub := sink.NewHub(logger)
	hub.SetBearerToken(token)
	hub.SetRelayDebug(relayDebug)
	if proxyRole {
		hub.SetEphemeralSessions(true)
		logger.Info("relay: proxy role — sessions live only while a laptop peer is connected")
	}
	st, err := store.Open()
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer st.Close()
	logger.Info("store: opened", "path", st.Path())
	hub.SetJournal(st)
	m := mux.New(hub, mux.Options{Logger: logger})
	hub.SetMux(m)

	cfg := loadHostConfig()

	reg := delivery.NewRegistry()
	box := delivery.NewOutbox()
	delivery.SetLogger(logger)
	inject.Init(m, reg, box, st)

	if n, err := box.PurgeOlderThan(7 * 24 * time.Hour); err != nil {
		logger.Warn("outbox: purge stale failed", "err", err)
	} else if n > 0 {
		logger.Info("outbox: purged stale messages", "count", n)
	}

	m.SetUserPromptLandHook(func(sessionID, threadID, text string) {
		if err := st.MarkLanded(sessionID, text); err != nil {
			logger.Warn("store: mark landed", "session", sessionID, "err", err)
		}
		hub.FanoutInputStatus(sink.InputStatusLanded(threadID, sessionID))
	})

	hooksHandler := producers.NewHooks(m, producers.HooksConfig{
		BearerToken: token,
		Logger:      logger,
		Outbox:      box,
	})
	ingestHandler := producers.NewIngest(m, producers.IngestConfig{Logger: logger})

	root := http.NewServeMux()
	root.Handle("/ambient-link/ws", hub)
	root.Handle("/face-chat/ws", hub)
	cloudPeer := cloud.NewPeerServer(hub, logger)
	hub.SetCloudPeer(cloudPeer)
	if proxyRole {
		cloudPeer.OnDisconnect = hub.DropSyncedSessions
	}
	root.Handle("/ambient-link/relay", cloudPeer)
	var macCloudBridge *cloud.Bridge
	if proxyRole {
		// The proxy owns no sessions, so nothing may inject state locally —
		// hooks/ingest posts here would fabricate sessions no laptop can verify.
		rejectLocal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "proxy role: local ingestion disabled; connect a laptop relay", http.StatusForbidden)
		})
		root.Handle("/ambient-link/hooks/", rejectLocal)
		root.Handle("/ambient-link/ingest", rejectLocal)
		root.Handle("/ambient-link/debug/", rejectLocal)
	} else {
		root.Handle("/ambient-link/hooks/", hooksHandler)
		root.Handle("/ambient-link/ingest", ingestHandler)
	}
	// Control channel for `host run <agent>` (relay-owned PTY mode).
	root.Handle("/ambient-link/pty", &pty.ControlHandler{Logger: logger, Token: token})
	root.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	root.HandleFunc("/ambient-link/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		outbox, _ := box.Snapshot()
		bridgeUp := macCloudBridge != nil && macCloudBridge.Connected()
		laptopPeer := cloudPeer.Connected()
		writeJSON(w, map[string]any{
			"sessions":    m.Snapshot(),
			"delivery":    reg.Snapshot(),
			"outbox":      outbox,
			"relay_debug": relayDebug,
			// Mac → cloud uplink (true on your laptop when AMBIENT_LINK_CLOUD is set).
			"cloud_bridge_connected": bridgeUp,
			// Laptop → this server (true on public.computer when your Mac is linked).
			"laptop_peer_connected": laptopPeer,
			// Deprecated: same as laptop_peer_connected. Misleading on Mac localhost.
			"cloud_peer":  laptopPeer,
			"journal":     st.Head(),
			"db":          st.Path(),
			"default_cwd": cfg.defaultCwd(),
			"now":         time.Now().UnixMilli(),
			// Observability only (no logic depends on these): answers
			// "is the running relay current?".
			"pid":     os.Getpid(),
			"version": buildRev(),
			"role":    map[bool]string{true: "proxy", false: "local"}[proxyRole],
		})
	})
	// Cross-surface config. The Android app POSTs the default working directory
	// here; the glasses web app reads it from /status to prefill a new session.
	root.HandleFunc("/ambient-link/config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, map[string]any{"default_cwd": cfg.defaultCwd()})
		case http.MethodPost:
			var body struct {
				DefaultCwd string `json:"default_cwd"`
				Create     bool   `json:"create"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			trimmed := strings.TrimSpace(body.DefaultCwd)
			if trimmed == "" {
				cfg.setDefaultCwd("")
				writeJSON(w, map[string]any{"ok": true, "exists": true, "default_cwd": ""})
				return
			}
			resolved, err := resolveMacCwd(trimmed)
			if err != nil {
				writeJSON(w, map[string]any{"ok": false, "error": err.Error()})
				return
			}
			st, statErr := os.Stat(resolved)
			if statErr != nil {
				if !os.IsNotExist(statErr) {
					writeJSON(w, map[string]any{"ok": false, "error": statErr.Error()})
					return
				}
				if !body.Create {
					writeJSON(w, map[string]any{
						"ok":            false,
						"exists":        false,
						"default_cwd":   trimmed,
						"resolved_path": resolved,
					})
					return
				}
				if mkErr := os.MkdirAll(resolved, 0o755); mkErr != nil {
					writeJSON(w, map[string]any{
						"ok":            false,
						"exists":        false,
						"resolved_path": resolved,
						"error":         mkErr.Error(),
					})
					return
				}
			} else if !st.IsDir() {
				writeJSON(w, map[string]any{
					"ok":            false,
					"exists":        false,
					"resolved_path": resolved,
					"error":         "not a directory",
				})
				return
			}
			cfg.setDefaultCwd(trimmed)
			logger.Info("config: default_cwd set", "value", cfg.defaultCwd(), "resolved", resolved)
			writeJSON(w, map[string]any{
				"ok":            true,
				"exists":        true,
				"default_cwd":   cfg.defaultCwd(),
				"resolved_path": resolved,
			})
		default:
			w.Header().Set("Allow", "GET, POST")
			w.WriteHeader(http.StatusMethodNotAllowed)
			writeJSON(w, map[string]any{"error": "method not allowed"})
		}
	})
	root.HandleFunc("/ambient-link/sessions", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, map[string]any{
				"sessions": m.Snapshot(),
				"now":      time.Now().UnixMilli(),
			})
		case http.MethodPost:
			w.WriteHeader(http.StatusNotImplemented)
			writeJSON(w, map[string]any{
				"error": "session creation is not wired on this host yet; start the agent in a terminal first",
			})
		default:
			w.Header().Set("Allow", "GET, POST")
			w.WriteHeader(http.StatusMethodNotAllowed)
			writeJSON(w, map[string]any{"error": "method not allowed"})
		}
	})
	root.HandleFunc("/ambient-link/pair", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var p pair.Payload
		var err error
		if r.Host != "" {
			wsScheme := "ws"
			if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
				wsScheme = "wss"
			}
			p = pair.BuildForHost(r.Host, token, wsScheme)
		} else {
			p, err = pair.Build(listen, token, "ws")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		writeJSON(w, p)
	})
	// Debug: pop glasses HUD from curl (keyboard / automation, no phone UI).
	// Registered only on a local relay: ServeMux prefers the most specific
	// pattern, so registering these in proxy role would bypass the
	// /ambient-link/debug/ reject handler above and let curl fabricate state.
	if !proxyRole {
		root.HandleFunc("/ambient-link/debug/yank", func(w http.ResponseWriter, r *http.Request) {
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
		root.HandleFunc("/ambient-link/debug/input", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "POST only", http.StatusMethodNotAllowed)
				return
			}
			var req struct {
				Thread string `json:"thread"`
				Text   string `json:"text"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if req.Thread == "" || req.Text == "" {
				http.Error(w, "thread and text required", http.StatusBadRequest)
				return
			}
			// Delivery ALWAYS submits. The human turn is recorded by inject in the
			// store with its honest status; we only echo it onto the live HUD when
			// it was actually written to the agent — no false "sent" (DECISIONS §4).
			result, err := inject.SendInputResult(req.Thread, req.Text, "")
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			if inject.Delivered(result) {
				_ = m.IngestUserInput(req.Thread, req.Text)
			}
			writeJSON(w, map[string]any{"ok": "input", "delivery": result})
		})
	}
	if webRoot != "" {
		if st, err := os.Stat(webRoot); err != nil || !st.IsDir() {
			return fmt.Errorf("web-root %q: %w", webRoot, err)
		}
		root.Handle("/ambient-link/", http.StripPrefix("/ambient-link", webapp.Dir(webRoot)))
		root.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" {
				http.Redirect(w, r, "/ambient-link/", http.StatusFound)
				return
			}
			http.NotFound(w, r)
		})
		logger.Info("host: serving web companion", "root", webRoot, "base", "/ambient-link/")
	}

	srv := &http.Server{
		Addr:              listen,
		Handler:           root,
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Everything below observes THIS machine (mDNS, transcript tailers, proc
	// watcher, delivery retry, stale reaper). A Cloud Assist proxy owns no
	// sessions and observes nothing locally — its only inputs are the laptop
	// peer socket and web clients — so none of it is even constructed there.
	if !proxyRole {
		if err := discovery.Advertise(ctx, listen, token, logger); err != nil {
			logger.Warn("host: mdns advertise failed", "err", err)
		} else {
			logger.Info("host: mdns advertising", "type", discovery.ServiceType)
		}

		// JSONL tailers — one per agent, both always-on. They share the mux,
		// so a session that fires both hook events and JSONL writes gets deduped
		// by the mux on (session_id, event_type) within DedupWindow.
		var claudeTailer, codexTailer, cursorTailer *producers.JSONLTailer
		if jsonlRoot != "" {
			if claudeTailer, err = startJSONLTailer(ctx, m, jsonlRoot, producers.FormatClaude, "claude", logger); err != nil {
				return err
			}
		}
		if codexJSONLRoot != "" {
			if codexTailer, err = startJSONLTailer(ctx, m, codexJSONLRoot, producers.FormatCodex, "codex", logger); err != nil {
				return err
			}
		}
		if cursorJSONLRoot != "" {
			if cursorTailer, err = startJSONLTailer(ctx, m, cursorJSONLRoot, producers.FormatCursor, "cursor", logger); err != nil {
				return err
			}
		}

		// Process watcher
		procW, err := producers.NewProcWatcher(m, producers.ProcConfig{
			Logger:   logger,
			Registry: reg,
			LiveSessions: func() []producers.LiveSession {
				views := m.Snapshot()
				out := make([]producers.LiveSession, 0, len(views))
				for _, v := range views {
					out = append(out, producers.LiveSession{
						SessionID: v.SessionID,
						Agent:     v.Agent,
						CWD:       v.CWD,
						State:     v.State,
					})
				}
				return out
			},
			OnSessionLive: func(sessionID string) {
				if delivery.RetryPending(sessionID, reg, box) {
					logger.Info("delivery: retry on live session", "session", sessionID)
				}
				// The process is alive but the mux doesn't know the session —
				// e.g. a quiet-but-alive agent after a relay restart (initial
				// scan skips transcripts older than StaleAge). Resurrect it
				// from its transcript so the list matches reality.
				if !m.HasSession(sessionID) {
					if path, tailer := findSessionTranscript(sessionID, jsonlRoot, codexJSONLRoot, cursorJSONLRoot, claudeTailer, codexTailer, cursorTailer); tailer != nil {
						logger.Info("proc: resurrecting live session from transcript", "session", sessionID, "path", path)
						tailer.Attach(path)
					}
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

		// Stale-session reaper — catch-all only. A session is reaped solely when
		// its process is NOT alive (registry has no live endpoint). A living agent,
		// idle or waiting for hours, is never killed on a timer (DECISIONS.md §5).
		isLive := func(sessionID string) bool {
			_, ok := reg.Get(sessionID)
			return ok
		}
		go runStaleReaper(ctx, m, 30*time.Minute, 5*time.Minute, isLive, logger)
	}

	// Cloud reverse channel (G5). Optional: only when AMBIENT_LINK_CLOUD is set.
	// Dials out to the cloud relay, mirrors broadcasts up, and accepts
	// input/special coming down — routed through the same delivery path as a LAN
	// client. LAN works with no config; the cloud is a backup transport and the
	// native app stays source of truth (DECISIONS.md §6).
	if cloudURL := strings.TrimSpace(os.Getenv("AMBIENT_LINK_CLOUD")); cloudURL != "" {
		macCloudBridge = cloud.New(cloud.Config{
			URL:    cloudURL,
			Token:  token,
			Logger: logger,
			Deliver: func(thread, text string) (string, error) {
				res, err := inject.SendInputResult(thread, text, "")
				if err != nil {
					return "failed", err
				}
				if inject.Delivered(res) {
					_ = m.IngestUserInput(thread, text)
				}
				return res.Status, nil
			},
			Special:  inject.SendSpecial,
			Snapshot: func() []byte { return cloudSnapshot(m, st) },
		})
		hub.SetCloud(macCloudBridge)
		go macCloudBridge.Run(ctx)
		logger.Info("cloud: reverse channel enabled", "url", cloudURL)
	}

	go func() {
		logger.Info("host: listening",
			"addr", listen,
			"endpoints", []string{
				"/ambient-link/ws",
				"/ambient-link/relay",
				"/face-chat/ws",
				"/ambient-link/ingest",
				"/ambient-link/pair",
				"/ambient-link/hooks/claude",
				"/ambient-link/hooks/codex",
				"/ambient-link/status",
				"/healthz",
			},
			"auth", boolStr(token != ""),
		)
		// ln was acquired up front as the single-instance lock.
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("host: serve failed", "err", err)
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
func runStaleReaper(ctx context.Context, m *mux.Mux, maxIdle, interval time.Duration, isLive func(string) bool, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := m.SweepStale(maxIdle, isLive); n > 0 {
				log.Info("mux: reaped dead sessions (process gone)", "count", n, "max_idle", maxIdle.String())
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

// defaultWebRoot locates the glasses web app to serve. It is per-host correct:
// an explicit AMBIENT_LINK_WEB_ROOT wins; otherwise the sibling-repo layout
// (~/ambient-link-meta/web or ../ambient-link-meta/web) is probed. There is no
// hard-coded absolute path — if nothing is found we return "" and simply don't
// serve the web companion (DECISIONS: no machine-specific paths).
func defaultWebRoot() string {
	if v := strings.TrimSpace(os.Getenv("AMBIENT_LINK_WEB_ROOT")); v != "" {
		return v
	}
	candidates := []string{}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		candidates = append(candidates, filepath.Join(home, "ambient-link-meta", "web"))
	}
	candidates = append(candidates, "../ambient-link-meta/web", "../../ambient-link-meta/web")
	for _, c := range candidates {
		if st, err := os.Stat(c); err == nil && st.IsDir() {
			abs, _ := filepath.Abs(c)
			return abs
		}
	}
	return ""
}

// cloudSnapshot builds the initial state frame the cloud bridge sends upstream
// on each (re)connect, so a remote client sees current sessions immediately.
func cloudSnapshot(m *mux.Mux, st *store.Store) []byte {
	payload, err := json.Marshal(map[string]any{
		"type":     "relay_hello",
		"threads":  m.ThreadsHello(),
		"sessions": m.Snapshot(),
		"cursor":   map[string]int64{"journal": st.Head()},
		"at":       time.Now().UnixMilli(),
	})
	if err != nil {
		return nil
	}
	return payload
}

// runAgent implements `host run <agent> [args...]` — launch an agent under a
// relay-owned PTY for reliable delivery (DECISIONS.md §2b).
func runAgent(rest []string) error {
	if len(rest) == 0 {
		return fmt.Errorf("usage: host run <agent> [args...]   (e.g. host run claude)")
	}
	agent := rest[0]
	args := rest[1:]
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	thread := mux.ThreadIDFor(canonicalAgent(agent), cwd)
	listen := envOr("AMBIENT_LINK_LISTEN", defaultListen)
	_, port, err := net.SplitHostPort(listen)
	if err != nil || port == "" {
		port = "5181"
	}
	wsURL := "ws://127.0.0.1:" + port + "/ambient-link/pty"
	token := os.Getenv("AMBIENT_LINK_TOKEN")
	logger := newLogger(envOr("AMBIENT_LINK_LOG", "info"))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Info("run: launching agent under relay PTY", "agent", agent, "thread", thread, "daemon", wsURL)
	return pty.Run(ctx, agent, args, wsURL, token, thread, logger)
}

// canonicalAgent maps an executable name to the agent label the JSONL tailers
// use, so a PTY-launched agent shares a thread with its observed transcript.
func canonicalAgent(exe string) string {
	e := strings.ToLower(filepath.Base(exe))
	e = strings.TrimSuffix(e, ".exe")
	e = strings.TrimSuffix(e, ".cmd")
	switch {
	case strings.Contains(e, "cursor"):
		return "cursor"
	case strings.Contains(e, "claude"):
		return "claude"
	case strings.Contains(e, "codex"):
		return "codex"
	default:
		return e
	}
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
	fmt.Println("Glasses web app:", strings.TrimRight(*statusURL, "/")+"/ambient-link/")
	return nil
}

// startJSONLTailer constructs and launches a tailer goroutine for one agent.
func startJSONLTailer(ctx context.Context, m *mux.Mux, root string, format producers.JSONLFormat, agent string, log *slog.Logger) (*producers.JSONLTailer, error) {
	tailer, err := producers.NewJSONLTailer(m, producers.JSONLConfig{
		Root:   root,
		Format: format,
		Agent:  agent,
		Logger: log,
	})
	if err != nil {
		return nil, fmt.Errorf("jsonl tailer (%s): %w", agent, err)
	}
	go func() {
		if err := tailer.Run(ctx); err != nil {
			log.Error("jsonl: tailer exited", "agent", agent, "err", err)
		}
	}()
	log.Info("jsonl: tailer started", "agent", agent, "root", root)
	return tailer, nil
}

// findSessionTranscript locates a session's transcript by uuid across the
// agents' on-disk layouts. Returns the path and the tailer that owns it.
func findSessionTranscript(sessionID string, claudeRoot, codexRoot, cursorRoot string, claudeT, codexT, cursorT *producers.JSONLTailer) (string, *producers.JSONLTailer) {
	if claudeT != nil && claudeRoot != "" {
		if hits, _ := filepath.Glob(filepath.Join(claudeRoot, "*", sessionID+".jsonl")); len(hits) > 0 {
			return hits[0], claudeT
		}
	}
	if codexT != nil && codexRoot != "" {
		// rollout files nest by date: <root>/YYYY/MM/DD/rollout-...-<uuid>.jsonl
		for _, pat := range []string{
			filepath.Join(codexRoot, "*", "*", "*", "rollout-*"+sessionID+".jsonl"),
			filepath.Join(codexRoot, "rollout-*"+sessionID+".jsonl"),
		} {
			if hits, _ := filepath.Glob(pat); len(hits) > 0 {
				return hits[0], codexT
			}
		}
	}
	if cursorT != nil && cursorRoot != "" {
		if hits, _ := filepath.Glob(filepath.Join(cursorRoot, "*", "agent-transcripts", sessionID, sessionID+".jsonl")); len(hits) > 0 {
			return hits[0], cursorT
		}
	}
	return "", nil
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

// --- single-instance lock -------------------------------------------------
//
// The lock is deliberately trivial: binding the TCP port in runServe IS the
// lock (the OS guarantees one binder). The lock file below only records the
// running PID so a conflict message can name the process to stop. There is no
// version arbitration and nothing ever gets killed.

func lockPath() string {
	return filepath.Join(os.TempDir(), "ambient-link-relay.lock")
}

func writeLock(listen string) {
	_ = os.WriteFile(lockPath(), []byte(fmt.Sprintf("%d\n%s\n", os.Getpid(), listen)), 0o644)
}

func removeLock() { _ = os.Remove(lockPath()) }

// readLockPID returns the PID recorded in the lock file, or 0 if absent. It is
// advisory only — used to make the "already running" message actionable.
func readLockPID() int {
	b, err := os.ReadFile(lockPath())
	if err != nil {
		return 0
	}
	line := strings.SplitN(string(b), "\n", 2)[0]
	pid, _ := strconv.Atoi(strings.TrimSpace(line))
	return pid
}

// buildRev returns the short VCS revision baked in by `go build`, for
// read-only "is this the current relay?" observability on /status.
func buildRev() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				if len(s.Value) > 12 {
					return s.Value[:12]
				}
				return s.Value
			}
		}
	}
	return "unknown"
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// stateHome is the relay's on-disk state dir (~/.ambient-link by default),
// matching where the store keeps relay.db.
func stateHome() string {
	if v := strings.TrimSpace(os.Getenv("AMBIENT_LINK_HOME")); v != "" {
		return v
	}
	if h, err := os.UserHomeDir(); err == nil && h != "" {
		return filepath.Join(h, ".ambient-link")
	}
	return "."
}

// hostConfig is a tiny persisted store for cross-surface settings. Today it
// holds the default working directory the glasses web app prefills for a new
// session; the Android app sets it via POST /ambient-link/config.
type hostConfig struct {
	mu         sync.Mutex
	DefaultCwd string `json:"default_cwd"`
	path       string
}

func loadHostConfig() *hostConfig {
	c := &hostConfig{path: filepath.Join(stateHome(), "config.json")}
	if b, err := os.ReadFile(c.path); err == nil {
		_ = json.Unmarshal(b, c)
	}
	return c
}

func (c *hostConfig) defaultCwd() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.DefaultCwd
}

func (c *hostConfig) setDefaultCwd(v string) {
	c.mu.Lock()
	c.DefaultCwd = v
	b, _ := json.Marshal(c)
	path := c.path
	c.mu.Unlock()
	if path != "" {
		_ = os.MkdirAll(filepath.Dir(path), 0o755)
		_ = os.WriteFile(path, b, 0o644)
	}
}

// resolveMacCwd expands ~ and relative paths against the relay host user's home.
func resolveMacCwd(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	raw = strings.ReplaceAll(raw, "～", "~")
	if strings.HasPrefix(raw, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("cannot expand ~: %w", err)
		}
		rest := strings.TrimPrefix(raw, "~")
		rest = strings.TrimPrefix(rest, "/")
		if rest == "" {
			return home, nil
		}
		return filepath.Join(home, rest), nil
	}
	if filepath.IsAbs(raw) {
		return filepath.Clean(raw), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Clean(raw), nil
	}
	return filepath.Join(home, raw), nil
}
