// Package cloud is the relay's optional outbound reverse channel (DECISIONS §6).
//
// When AMBIENT_LINK_CLOUD is set, the relay dials OUT to a cloud relay endpoint
// (e.g. wss://public.computer/ambient-link/relay) and:
//
//   - mirrors every local broadcast UP, so a phone/glasses talking to the cloud
//     sees the same session cards it would see on the LAN; and
//   - accepts input/special messages coming DOWN and routes them through the
//     same delivery path as a LAN client — this is the human→agent reverse
//     channel (G5) that was previously impossible across networks.
//
// The cloud is a BACKUP transport. The relay never requires it: with no env var
// the relay is pure LAN. The native app remains source of truth; anything the
// cloud delivers is reconciled there, never trusted blindly.
//
// Client contract (relay → cloud): the relay sends local broadcast frames as-is
// and a leading {"type":"relay_hello",...} snapshot on each (re)connect. It
// expects to receive client frames of the same shape the LAN WS hub accepts:
//
//	{"type":"input","thread":"...","text":"..."}
//	{"type":"special","thread":"...","key":"y"}
package cloud

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

// Config wires the bridge to the relay's delivery + state surfaces.
type Config struct {
	URL    string // wss:// or ws:// endpoint to dial
	Token  string // optional bearer; sent as ?token=
	Logger *slog.Logger

	// Deliver routes a human message into a live agent session and returns the
	// honest delivery status. Required.
	Deliver func(thread, text string) (status string, err error)
	// Special routes a single raw key (permission answer). Optional.
	Special func(thread, key string) error
	// Snapshot returns an initial state frame to send on each (re)connect.
	// Optional.
	Snapshot func() []byte
}

// Bridge maintains a reconnecting outbound WS to the cloud relay.
type Bridge struct {
	cfg Config
	log *slog.Logger

	mu   sync.Mutex
	conn *websocket.Conn
	out  chan []byte
}

// New constructs a Bridge. Call Run to start; Mirror to push broadcasts up.
func New(cfg Config) *Bridge {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Bridge{cfg: cfg, log: cfg.Logger, out: make(chan []byte, 256)}
}

// Mirror enqueues a local broadcast payload to send upstream. Non-blocking:
// if the buffer is full (cloud slow/down) the frame is dropped — the store +
// subscribe/replay protocol lets the remote catch up on reconnect.
func (b *Bridge) Mirror(payload []byte) {
	select {
	case b.out <- payload:
	default:
		b.log.Warn("cloud: mirror buffer full, dropping frame")
	}
}

// Run dials and maintains the connection until ctx is cancelled, reconnecting
// with capped backoff.
func (b *Bridge) Run(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if err := b.session(ctx); err != nil && ctx.Err() == nil {
			b.log.Warn("cloud: session ended", "err", err, "retry_in", backoff.String())
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
}

func (b *Bridge) session(ctx context.Context) error {
	url := b.cfg.URL
	if b.cfg.Token != "" {
		sep := "?"
		for _, c := range url {
			if c == '?' {
				sep = "&"
				break
			}
		}
		url += sep + "token=" + b.cfg.Token
	}
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	conn, _, err := websocket.Dial(dialCtx, url, &websocket.DialOptions{})
	cancel()
	if err != nil {
		return err
	}
	b.log.Info("cloud: connected", "url", b.cfg.URL)
	b.mu.Lock()
	b.conn = conn
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.conn = nil
		b.mu.Unlock()
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}()

	sessCtx, stop := context.WithCancel(ctx)
	defer stop()

	if b.cfg.Snapshot != nil {
		if snap := b.cfg.Snapshot(); len(snap) > 0 {
			wctx, wc := context.WithTimeout(sessCtx, 5*time.Second)
			_ = conn.Write(wctx, websocket.MessageText, snap)
			wc()
		}
	}

	// Reader: route input/special coming down.
	go func() {
		for {
			_, data, err := conn.Read(sessCtx)
			if err != nil {
				stop()
				return
			}
			b.handleDownstream(data)
		}
	}()

	// Writer: mirror local broadcasts up.
	for {
		select {
		case <-sessCtx.Done():
			return sessCtx.Err()
		case payload := <-b.out:
			wctx, wc := context.WithTimeout(sessCtx, 5*time.Second)
			err := conn.Write(wctx, websocket.MessageText, payload)
			wc()
			if err != nil {
				return err
			}
		}
	}
}

func (b *Bridge) handleDownstream(data []byte) {
	var msg struct {
		Type   string `json:"type"`
		Thread string `json:"thread"`
		Text   string `json:"text"`
		Key    string `json:"key"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}
	switch msg.Type {
	case "input":
		if msg.Thread == "" || msg.Text == "" || b.cfg.Deliver == nil {
			return
		}
		status, err := b.cfg.Deliver(msg.Thread, msg.Text)
		if err != nil {
			b.log.Warn("cloud: downstream input failed", "thread", msg.Thread, "err", err)
			return
		}
		b.log.Info("cloud: downstream input delivered", "thread", msg.Thread, "status", status)
	case "special":
		if msg.Thread == "" || msg.Key == "" || b.cfg.Special == nil {
			return
		}
		if err := b.cfg.Special(msg.Thread, msg.Key); err != nil {
			b.log.Warn("cloud: downstream special failed", "thread", msg.Thread, "err", err)
		}
	}
}
