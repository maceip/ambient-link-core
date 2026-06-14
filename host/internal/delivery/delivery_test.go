package delivery

import (
	"testing"
)

func TestOutboxEnqueueDequeue(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	box := NewOutbox()
	msg := Message{SessionID: "sess-1", ThreadID: "claude-abc", Text: "continue", Enter: true, At: 1}
	if err := box.Enqueue(msg); err != nil {
		t.Fatal(err)
	}
	if !box.HasPending("sess-1") {
		t.Fatal("expected pending")
	}
	got, ok := box.Dequeue("sess-1")
	if !ok || got.Text != "continue" {
		t.Fatalf("dequeue: %+v ok=%v", got, ok)
	}
	if box.HasPending("sess-1") {
		t.Fatal("expected empty after dequeue")
	}
}

func TestHookResponseStop(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	box := NewOutbox()
	_ = box.Enqueue(Message{SessionID: "s1", Text: "please verify", Enter: true})
	resp := HookResponse("Stop", "s1", nil, box)
	if resp == nil || resp["decision"] != "block" {
		t.Fatalf("stop resp: %#v", resp)
	}
	if box.HasPending("s1") {
		t.Fatal("should dequeue")
	}
}

func TestHookResponsePermission(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	box := NewOutbox()
	_ = box.Enqueue(Message{SessionID: "s1", Text: "y", Enter: false})
	resp := HookResponse("PermissionRequest", "s1", nil, box)
	hs, ok := resp["hookSpecificOutput"].(map[string]any)
	if !ok {
		t.Fatalf("resp: %#v", resp)
	}
	dec, _ := hs["decision"].(map[string]any)
	if dec["behavior"] != "allow" {
		t.Fatalf("behavior: %#v", dec)
	}
}

func TestPermissionBehavior(t *testing.T) {
	if permissionBehavior("approve") != "allow" {
		t.Fatal("approve")
	}
	if permissionBehavior("deny") != "deny" {
		t.Fatal("deny")
	}
	if permissionBehavior("continue") != "" {
		t.Fatal("non-permission text")
	}
}
