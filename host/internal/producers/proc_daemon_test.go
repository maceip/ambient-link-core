package producers

import "testing"

// Claude's daemon-hosted sessions don't hold the transcript open; the session
// uuid rides in argv and in the daemon socket path. Regression for the bug
// where such sessions had no delivery endpoint and were reaped while alive.
func TestSessionIDFromArgv(t *testing.T) {
	cases := map[string]string{
		"/Users/mac/.local/share/claude/versions/2.1.202 --session-id 62544dfb-ec6b-4b01-92dc-ecdc738d17f4 --other": "62544dfb-ec6b-4b01-92dc-ecdc738d17f4",
		"claude --session-id=aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee":                                                  "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
		"claude serve":   "",
		"codex --resume": "",
	}
	for cmd, want := range cases {
		if got := sessionIDFromArgv(cmd); got != want {
			t.Errorf("sessionIDFromArgv(%q) = %q, want %q", cmd, got, want)
		}
	}
}

func TestCCDaemonSockRegex(t *testing.T) {
	path := "/tmp/cc-daemon-501/d78168b1/rv/62544dfb-ec6b-4b01-92dc-ecdc738d17f4.sock"
	m := ccDaemonSock.FindStringSubmatch(path)
	if m == nil || m[1] != "62544dfb-ec6b-4b01-92dc-ecdc738d17f4" {
		t.Fatalf("ccDaemonSock failed to extract uuid from %q: %v", path, m)
	}
}
