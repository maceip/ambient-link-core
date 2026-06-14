package delivery

import "strings"

// HookResponse builds Claude/Codex hook JSON responses that drain the outbox.
func HookResponse(hookEvent, sessionID string, raw map[string]any, box *Outbox) map[string]any {
	if box == nil || sessionID == "" {
		return nil
	}
	msg, ok := box.Dequeue(sessionID)
	if !ok {
		return nil
	}
	switch hookEvent {
	case "Stop", "SubagentStop":
		return map[string]any{
			"decision": "block",
			"reason":   msg.Text,
		}
	case "PermissionRequest":
		behavior := permissionBehavior(msg.Text)
		if behavior == "" {
			return nil
		}
		return map[string]any{
			"hookSpecificOutput": map[string]any{
				"hookEventName": "PermissionRequest",
				"decision": map[string]any{
					"behavior": behavior,
				},
			},
		}
	case "UserPromptSubmit":
		return map[string]any{
			"hookSpecificOutput": map[string]any{
				"hookEventName":     "UserPromptSubmit",
				"additionalContext": "Ambient Link HUD reply: " + msg.Text,
			},
		}
	default:
		return nil
	}
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
