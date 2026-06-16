package delivery

import "strings"

// HookResponse builds Claude/Codex hook JSON responses that drain the outbox.
func HookResponse(hookEvent, sessionID string, raw map[string]any, box *Outbox) map[string]any {
	if box == nil || sessionID == "" {
		return nil
	}
	msg, ok := box.Peek(sessionID)
	if !ok {
		return nil
	}
	var resp map[string]any
	switch hookEvent {
	case "Stop", "SubagentStop":
		resp = map[string]any{
			"decision": "block",
			"reason":   msg.Text,
		}
	case "PermissionRequest":
		behavior := permissionBehavior(msg.Text)
		if behavior == "" {
			return nil
		}
		resp = map[string]any{
			"hookSpecificOutput": map[string]any{
				"hookEventName": "PermissionRequest",
				"decision": map[string]any{
					"behavior": behavior,
				},
			},
		}
	case "UserPromptSubmit":
		resp = map[string]any{
			"hookSpecificOutput": map[string]any{
				"hookEventName":     "UserPromptSubmit",
				"additionalContext": "Ambient Link HUD reply: " + msg.Text,
			},
		}
	default:
		return nil
	}
	if resp != nil {
		_, _ = box.Dequeue(sessionID)
	}
	return resp
}

func permissionBehavior(text string) string {
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "y", "yes", "approve", "allow":
		return "allow"
	case "n", "no", "deny", "reject":
		return "deny"
	default:
		return ""
	}
}
