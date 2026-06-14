// Package pair builds pairing payloads and deep links for phone relay apps.
package pair

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
)

const Version = 1

// Payload is returned by GET /face-chat/pair and encoded in QR deep links.
type Payload struct {
	V        int    `json:"v"`
	WSURL    string `json:"wsUrl"`
	Token    string `json:"token,omitempty"`
	Hostname string `json:"hostname"`
	PairURL  string `json:"pairUrl"`
}

// Build constructs a pairing payload from a listen address and optional bearer token.
func Build(listenAddr, token, wsScheme string) (Payload, error) {
	if wsScheme == "" {
		wsScheme = "ws"
	}
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return Payload{}, fmt.Errorf("pair: parse listen %q: %w", listenAddr, err)
	}
	lanIP, err := PrimaryLANIP()
	if err != nil {
		return Payload{}, err
	}
	wsHost := lanIP
	if host != "" && host != "0.0.0.0" && host != "::" {
		wsHost = host
	}
	wsURL := fmt.Sprintf("%s://%s:%s/face-chat/ws", wsScheme, wsHost, port)
	hostname, _ := os.Hostname()
	p := Payload{
		V:        Version,
		WSURL:    wsURL,
		Token:    token,
		Hostname: hostname,
	}
	p.PairURL = EncodeDeepLink(p)
	return p, nil
}

// EncodeDeepLink returns ambientlink://pair?… for QR codes and universal links.
func EncodeDeepLink(p Payload) string {
	q := url.Values{}
	q.Set("v", fmt.Sprintf("%d", p.V))
	q.Set("ws", p.WSURL)
	if p.Token != "" {
		q.Set("token", p.Token)
	}
	if p.Hostname != "" {
		q.Set("host", p.Hostname)
	}
	return "ambientlink://pair?" + q.Encode()
}

// ParseDeepLink decodes ambientlink://pair?… or a bare query string from QR scans.
func ParseDeepLink(raw string) (Payload, error) {
	s := strings.TrimSpace(raw)
	if strings.HasPrefix(s, "ambientlink://") {
		u, err := url.Parse(s)
		if err != nil {
			return Payload{}, err
		}
		s = u.RawQuery
	}
	q, err := url.ParseQuery(s)
	if err != nil {
		return Payload{}, err
	}
	ws := q.Get("ws")
	if ws == "" {
		return Payload{}, fmt.Errorf("pair: missing ws")
	}
	return Payload{
		V:        atoiDefault(q.Get("v"), Version),
		WSURL:    ws,
		Token:    q.Get("token"),
		Hostname: q.Get("host"),
		PairURL:  "ambientlink://pair?" + s,
	}, nil
}

// WSURLWithToken appends ?token= when the relay requires auth on upgrade.
func WSURLWithToken(wsURL, token string) string {
	if token == "" {
		return wsURL
	}
	u, err := url.Parse(wsURL)
	if err != nil {
		return wsURL
	}
	q := u.Query()
	q.Set("token", token)
	u.RawQuery = q.Encode()
	return u.String()
}

// PrimaryLANIP returns the first private IPv4 on a non-loopback interface.
func PrimaryLANIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok || ipNet.IP.To4() == nil {
				continue
			}
			ip := ipNet.IP.To4()
			if isPrivateIPv4(ip) {
				return ip.String(), nil
			}
		}
	}
	return "", fmt.Errorf("pair: no private LAN IPv4 found")
}

func isPrivateIPv4(ip net.IP) bool {
	return ip[0] == 10 ||
		(ip[0] == 172 && ip[1] >= 16 && ip[1] <= 31) ||
		(ip[0] == 192 && ip[1] == 168)
}

// DefaultHookURL is http://<lan-ip>:5181 for agent hook POSTs during install.
func DefaultHookURL() string {
	ip, err := PrimaryLANIP()
	if err != nil {
		return "http://127.0.0.1:5181"
	}
	return fmt.Sprintf("http://%s:5181", ip)
}

// FetchRunning asks a local daemon for its pair payload.
func FetchRunning(statusBase string) (Payload, error) {
	resp, err := http.Get(strings.TrimRight(statusBase, "/") + "/face-chat/pair")
	if err != nil {
		return Payload{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Payload{}, fmt.Errorf("pair: GET /face-chat/pair: %s", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Payload{}, err
	}
	var p Payload
	if err := json.Unmarshal(body, &p); err != nil {
		return Payload{}, err
	}
	return p, nil
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil || n == 0 {
		return def
	}
	return n
}
