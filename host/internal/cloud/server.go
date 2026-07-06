// Package cloud — server-side peer acceptor for laptop reverse bridges (DECISIONS §6).
//
// When the cloud relay runs on public.computer, a laptop dials /ambient-link/relay
// and mirrors broadcasts up. Web clients on the cloud relay send input here; this
// server forwards those frames down to the connected laptop peer.
package cloud

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

// Upstream applies broadcast frames and relay snapshots from a connected laptop peer.
type Upstream interface {
	ApplyUpstreamBroadcast(data []byte)
	ApplyRelayHello(data []byte)
}

// PeerServer accepts one active laptop bridge at /ambient-link/relay.
type PeerServer struct {
	log      *slog.Logger
	upstream Upstream

	// OnDisconnect, when set, runs after a laptop peer connection ends and no
	// replacement is attached. Proxy role uses it to drop the peer's sessions.
	OnDisconnect func()

	mu   sync.RWMutex
	conn *websocket.Conn
}

// NewPeerServer constructs a PeerServer. upstream receives mirrored broadcasts.
func NewPeerServer(upstream Upstream, logger *slog.Logger) *PeerServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &PeerServer{log: logger, upstream: upstream}
}

// Connected reports whether a laptop peer is currently attached.
func (p *PeerServer) Connected() bool {
	p.mu.RLock()
	ok := p.conn != nil
	p.mu.RUnlock()
	return ok
}

// SendInput forwards a web-client composer message to the laptop peer.
func (p *PeerServer) SendInput(thread, text, clientID string) error {
	payload, err := json.Marshal(map[string]any{
		"type":      "input",
		"thread":    thread,
		"text":      text,
		"client_id": clientID,
	})
	if err != nil {
		return err
	}
	return p.write(payload)
}

// SendSpecial forwards a permission/special key to the laptop peer.
func (p *PeerServer) SendSpecial(thread, key string) error {
	payload, err := json.Marshal(map[string]any{
		"type":   "special",
		"thread": thread,
		"key":    key,
	})
	if err != nil {
		return err
	}
	return p.write(payload)
}

func (p *PeerServer) write(payload []byte) error {
	p.mu.RLock()
	conn := p.conn
	p.mu.RUnlock()
	if conn == nil {
		return errNoPeer
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return conn.Write(ctx, websocket.MessageText, payload)
}

var errNoPeer = &peerError{"no laptop peer connected"}

type peerError struct{ msg string }

func (e *peerError) Error() string { return e.msg }

// ServeHTTP upgrades /ambient-link/relay to a laptop bridge WebSocket.
func (p *PeerServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		p.log.Warn("cloud peer: upgrade failed", "err", err)
		return
	}
	// relay_hello carries every live session with up-to-4KB snippets; the
	// websocket default read limit (32KB) killed any bridge whose snapshot
	// exceeded it — the B-105 "connects then dies in 300ms" flake. Size the
	// limit to the mux bound (256 sessions × ~9KB snippets ≈ 2.3MB).
	conn.SetReadLimit(8 << 20)
	p.mu.Lock()
	if old := p.conn; old != nil {
		_ = old.Close(websocket.StatusNormalClosure, "replaced")
	}
	p.conn = conn
	p.mu.Unlock()
	p.log.Info("cloud peer: laptop connected", "remote", r.RemoteAddr)
	defer func() {
		p.mu.Lock()
		last := p.conn == conn
		if last {
			p.conn = nil
		}
		p.mu.Unlock()
		_ = conn.Close(websocket.StatusNormalClosure, "")
		p.log.Info("cloud peer: laptop disconnected")
		// Only fire when this was the active peer (not one replaced by a
		// newer connection) so a reconnect race doesn't wipe fresh state.
		if last && p.OnDisconnect != nil {
			p.OnDisconnect()
		}
	}()

	ctx := r.Context()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			// Name the close reason: a silent return here hid the read-limit
			// kill for days (every log just said connected/disconnected).
			if ctx.Err() == nil {
				p.log.Warn("cloud peer: read ended", "err", err)
			}
			return
		}
		var peek struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &peek); err != nil {
			continue
		}
		if peek.Type == "relay_hello" {
			p.log.Info("cloud peer: relay_hello")
			if p.upstream != nil {
				p.upstream.ApplyRelayHello(data)
			}
			continue
		}
		if p.upstream != nil {
			p.upstream.ApplyUpstreamBroadcast(data)
		}
	}
}
