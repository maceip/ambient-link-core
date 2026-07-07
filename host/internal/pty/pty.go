// Package pty implements relay-owned PTY mode (DECISIONS.md §2b).
//
// Two roles share this package:
//
//   - Client (`host run <agent>`): opens a pseudo-terminal, launches the agent
//     attached to it, and proxies the user's real terminal <-> PTY so the human
//     uses the agent exactly as normal. It also opens a control WS back to the
//     running daemon; when the daemon needs to deliver a human message to this
//     thread, it sends it down that WS and the client types it into the PTY
//     master. Because that is real child stdin, delivery cannot be lost the way
//     a console-input write into a process we don't own can.
//
//   - Daemon (ControlHandler): accepts the control WS, and for the duration of
//     the connection registers a delivery.PTYWriter for the thread so inject
//     prefers it over the console/tty adapters.
//
// Session observation (state, transcript) still flows through the existing
// JSONL tailer + proc watcher — the PTY wrapper's only job is a reliable INPUT
// channel, so it stays small.
package pty

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"time"

	xpty "github.com/aymanbagabas/go-pty"
	"golang.org/x/term"
	"nhooyr.io/websocket"
)

// Run launches agent (with args) under a relay-owned PTY and proxies it to the
// current terminal, while maintaining a control WS to the daemon for input
// delivery. Blocks until the agent exits or ctx is cancelled.
func Run(ctx context.Context, agent string, args []string, daemonWS, token, thread string, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	ptmx, err := xpty.New()
	if err != nil {
		return fmt.Errorf("pty: open: %w", err)
	}
	defer ptmx.Close()

	// Size the PTY from the real terminal when we have one; otherwise fall back
	// to a sane default. A 0x0 PTY makes full-screen TUIs (claude/codex/cursor
	// are Ink/React apps) render nothing and ignore input, so a default is
	// essential when launched from a non-console context.
	cols, rows := 120, 30
	if w, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil && w > 0 && h > 0 {
		cols, rows = w, h
	}
	_ = ptmx.Resize(cols, rows)

	c := ptmx.Command(agent, args...)
	if err := c.Start(); err != nil {
		return fmt.Errorf("pty: start %q (is it on PATH?): %w", agent, err)
	}

	// Put our own stdin in raw mode so keystrokes pass straight through to the
	// child. Restored on exit.
	if term.IsTerminal(int(os.Stdin.Fd())) {
		if old, err := term.MakeRaw(int(os.Stdin.Fd())); err == nil {
			defer func() { _ = term.Restore(int(os.Stdin.Fd()), old) }()
		}
	}

	// When there is no real controlling terminal (headless: launched from a
	// service, CI, or driven purely by phone/web), modern TUI agents stall on
	// startup waiting for the terminal to answer capability queries (DA1/DA2,
	// XTVERSION, DSR). A real terminal answers them; a pipe never does, so the
	// agent never reaches its prompt and ignores injected input. In that case we
	// answer the queries ourselves so the agent proceeds. With a real terminal
	// attached we stay out of the way and let it respond.
	headless := !term.IsTerminal(int(os.Stdin.Fd()))

	go controlLoop(ctx, daemonWS, token, thread, ptmx, logger)
	go func() { _, _ = io.Copy(ptmx, os.Stdin) }()
	go pumpOut(os.Stdout, ptmx, headless)

	logger.Info("pty: agent running under relay PTY", "agent", agent, "thread", thread, "headless", headless)
	return c.Wait()
}

// capabilityReplies maps a terminal capability query the agent may emit to the
// canned answer a real terminal would send back. Kept minimal: just the queries
// that block TUI startup.
var capabilityReplies = []struct{ query, reply string }{
	{"\x1b[>0q", "\x1bP>|ambient-link-relay\x1b\\"}, // XTVERSION
	{"\x1b[>0c", "\x1b[>0;276;0c"},                  // secondary DA
	{"\x1b[>c", "\x1b[>0;276;0c"},                   // secondary DA
	{"\x1b[0c", "\x1b[?1;2c"},                       // primary DA
	{"\x1b[c", "\x1b[?1;2c"},                        // primary DA
	{"\x1b[5n", "\x1b[0n"},                          // device status (OK)
	{"\x1b[6n", "\x1b[1;1R"},                        // cursor position report
}

// pumpOut copies the PTY output to dst and, when headless, watches the stream
// for capability queries and writes the canned answers back into the PTY so the
// agent's startup handshake completes.
func pumpOut(dst io.Writer, ptmx xpty.Pty, headless bool) {
	buf := make([]byte, 8192)
	var carry []byte
	for {
		n, err := ptmx.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			_, _ = dst.Write(chunk)
			if headless {
				carry = answerQueries(append(carry, chunk...), ptmx)
			}
		}
		if err != nil {
			return
		}
	}
}

// answerQueries scans data for known capability queries, writes their replies
// into ptmx, and returns a small trailing remainder that may hold a query split
// across reads.
func answerQueries(data []byte, ptmx xpty.Pty) []byte {
	s := string(data)
	for {
		idx, qlen, reply := -1, 0, ""
		for _, q := range capabilityReplies {
			if i := indexFrom(s, q.query); i >= 0 && (idx == -1 || i < idx) {
				idx, qlen, reply = i, len(q.query), q.reply
			}
		}
		if idx == -1 {
			break
		}
		_, _ = ptmx.Write([]byte(reply))
		s = s[idx+qlen:]
	}
	// Keep a short tail in case a query straddles the next read.
	const keep = 8
	if len(s) > keep {
		s = s[len(s)-keep:]
	}
	return []byte(s)
}

func indexFrom(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// controlLoop maintains the control WS to the daemon and writes any input it
// receives into the PTY master. Reconnects with capped backoff.
func controlLoop(ctx context.Context, base, token, thread string, ptmx xpty.Pty, logger *slog.Logger) {
	endpoint := base + "?thread=" + url.QueryEscape(thread)
	if token != "" {
		endpoint += "&token=" + url.QueryEscape(token)
	}
	backoff := time.Second
	for ctx.Err() == nil {
		dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		conn, _, err := websocket.Dial(dialCtx, endpoint, nil)
		cancel()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 15*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		logger.Info("pty: control channel up", "thread", thread)
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				break
			}
			var m struct {
				Text   string `json:"text"`
				Submit bool   `json:"submit"`
			}
			if json.Unmarshal(data, &m) != nil {
				continue
			}
			if m.Text != "" {
				_, _ = ptmx.Write([]byte(m.Text))
			}
			if m.Submit {
				// A CR that lands in the same read burst as the text is
				// treated by TUI composers (codex) as a pasted newline, not a
				// submit — the message sits in the composer forever. Let the
				// TUI consume the text as input first, then send Enter as a
				// distinct keypress.
				if m.Text != "" {
					time.Sleep(150 * time.Millisecond)
				}
				_, _ = ptmx.Write([]byte("\r"))
			}
		}
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}
}
