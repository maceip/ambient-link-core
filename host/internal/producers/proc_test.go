package producers

import (
	"testing"

	"github.com/maceip/ambient-link-core/host/internal/proto"
)

type noopReaper struct{}

func (noopReaper) MarkDead(sessionID string) {}

func TestAgentFromCommandNormalizesWindowsExecutables(t *testing.T) {
	cases := map[string]string{
		`"C:\Users\mac\.local\bin\claude.exe" --dangerously-skip-permissions`: "claude",
		`C:\Users\mac\scoop\apps\codex\codex.exe --foo`:                       "codex",
		`/Users/mac/.local/bin/agent`:                                         "cursor",
		`cursor-agent.exe`:                                                    "cursor",
	}
	for cmd, want := range cases {
		if got := agentFromCommand(cmd); got != want {
			t.Fatalf("agentFromCommand(%q)=%q want %q", cmd, got, want)
		}
	}
}

func TestSessionsForUniqueLiveAgentRequiresSingleProcessAndSession(t *testing.T) {
	w, err := NewProcWatcher(noopReaper{}, ProcConfig{
		LiveSessions: func() []LiveSession {
			return []LiveSession{
				{SessionID: "s-claude", Agent: "claude", State: proto.StateIdle},
				{SessionID: "s-dead", Agent: "claude", State: proto.StateDead},
				{SessionID: "s-codex", Agent: "codex", State: proto.StateBusy},
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := w.sessionsForUniqueLiveAgent("claude.exe", 1)
	if _, ok := got["s-claude"]; !ok || len(got) != 1 {
		t.Fatalf("got %#v, want only s-claude", got)
	}
	if got := w.sessionsForUniqueLiveAgent("claude.exe", 2); got != nil {
		t.Fatalf("multiple process fallback should be nil, got %#v", got)
	}
	if got := w.sessionsForUniqueLiveAgent("missing.exe", 1); got != nil {
		t.Fatalf("unknown agent fallback should be nil, got %#v", got)
	}
}
