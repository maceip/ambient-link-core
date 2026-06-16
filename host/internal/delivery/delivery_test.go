package delivery

import (
	"testing"
)

func setOutboxHome(t *testing.T) {
	t.Helper()
	t.Setenv("AMBIENT_LINK_HOME", t.TempDir())
}

func TestOutboxEnqueueDequeue(t *testing.T) {
	setOutboxHome(t)
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

func TestOutboxPreservesFIFO(t *testing.T) {
	setOutboxHome(t)
	box := NewOutbox()
	_ = box.Enqueue(Message{ID: "a", SessionID: "sess-1", ThreadID: "claude-abc", Text: "first", Enter: true})
	_ = box.Enqueue(Message{ID: "b", SessionID: "sess-1", ThreadID: "claude-abc", Text: "second", Enter: true})
	if got := box.Count("sess-1"); got != 2 {
		t.Fatalf("count=%d, want 2", got)
	}
	first, ok := box.Dequeue("sess-1")
	if !ok || first.Text != "first" {
		t.Fatalf("first dequeue: %+v ok=%v", first, ok)
	}
	second, ok := box.Dequeue("sess-1")
	if !ok || second.Text != "second" {
		t.Fatalf("second dequeue: %+v ok=%v", second, ok)
	}
}

func TestHookResponseStop(t *testing.T) {
	setOutboxHome(t)
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

func TestHookResponsePermissionDoesNotDropNonPermissionReply(t *testing.T) {
	setOutboxHome(t)
	box := NewOutbox()
	_ = box.Enqueue(Message{SessionID: "s1", Text: "please continue", Enter: true})
	resp := HookResponse("PermissionRequest", "s1", nil, box)
	if resp != nil {
		t.Fatalf("non-permission text should not satisfy permission request: %#v", resp)
	}
	if !box.HasPending("s1") {
		t.Fatal("message should remain queued")
	}
}

func TestHookResponsePermission(t *testing.T) {
	setOutboxHome(t)
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

func TestDeliverWithResultQueuesWithoutLiveEndpoint(t *testing.T) {
	setOutboxHome(t)
	box := NewOutbox()
	res, err := DeliverWithResult("s1", "codex-abc", "codex", "hello", true, NewRegistry(), box, "client-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusQueued || res.ID != "client-1" || res.PendingCount != 1 {
		t.Fatalf("result: %+v", res)
	}
	msg, ok := box.Peek("s1")
	if !ok || msg.ID != "client-1" || msg.Text != "hello" {
		t.Fatalf("queued msg: %+v ok=%v", msg, ok)
	}
}

func TestDeliverWithResultPreservesPendingOrder(t *testing.T) {
	setOutboxHome(t)
	box := NewOutbox()
	_ = box.Enqueue(Message{ID: "first", SessionID: "s1", ThreadID: "codex-abc", Text: "first", Enter: true})
	res, err := DeliverWithResult("s1", "codex-abc", "codex", "second", true, NewRegistry(), box, "second")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusQueued || res.PendingCount != 2 {
		t.Fatalf("result: %+v", res)
	}
	msg, ok := box.Dequeue("s1")
	if !ok || msg.ID != "first" {
		t.Fatalf("first message was not preserved: %+v ok=%v", msg, ok)
	}
	msg, ok = box.Dequeue("s1")
	if !ok || msg.ID != "second" {
		t.Fatalf("second message missing: %+v ok=%v", msg, ok)
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
