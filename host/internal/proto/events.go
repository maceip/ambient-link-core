// Package proto defines the wire types shared between producers, the
// SessionMux, and downstream WS clients (notably the iOS / Android relay
// apps that fan events onto the glasses). Keep this in sync with
// protocol/PROTOCOL.md. The Node prototype at relay-node-prototype/
// is being phased out in favor of this Go host.
package proto

// EventType is the closed enum of normalized session events that producers
// emit and the SessionMux ingests. Values are stable on the wire.
type EventType string

const (
	EventSessionStart     EventType = "session_start"
	EventUserPrompt       EventType = "user_prompt"
	EventAssistantMessage EventType = "assistant_message"
	EventToolUse          EventType = "tool_use"
	EventPermissionPrompt EventType = "permission_prompt"
	EventStop             EventType = "stop"
	EventSessionEnd       EventType = "session_end"
)

// AllEventTypes is the closed set; use ValidEventType to gate inputs.
var AllEventTypes = map[EventType]bool{
	EventSessionStart:     true,
	EventUserPrompt:       true,
	EventAssistantMessage: true,
	EventToolUse:          true,
	EventPermissionPrompt: true,
	EventStop:             true,
	EventSessionEnd:       true,
}

// ValidEventType returns whether t is a known wire-stable event type.
func ValidEventType(t EventType) bool { return AllEventTypes[t] }

// SessionState is the canonical state held by the SessionMux per session.
// Only state transitions get broadcast as thread_* events.
type SessionState string

const (
	StateStarting           SessionState = "STARTING"
	StateBusy               SessionState = "BUSY"
	StateIdle               SessionState = "IDLE"
	StateAwaitingPermission SessionState = "AWAITING_PERMISSION"
	StateDead               SessionState = "DEAD"
)

// ProducerName identifies which subsystem observed an event. Used only for
// diagnostics; the mux treats all producers symmetrically.
type ProducerName string

const (
	ProducerHooks ProducerName = "hooks"
	ProducerJSONL ProducerName = "jsonl"
	ProducerProc  ProducerName = "proc"
)

// Event is the single shape every producer must emit. The mux dedupes by
// (SessionID, Type) inside a short window and never trusts any single
// producer as authoritative.
type Event struct {
	SessionID  string       `json:"session_id"`
	Agent      string       `json:"agent"`
	CWD        string       `json:"cwd"`
	Type       EventType    `json:"event_type"`
	Payload    any          `json:"payload,omitempty"`
	Source     ProducerName `json:"source"`
	ObservedAt int64        `json:"observed_at"` // unix milliseconds
}

// Wire-side broadcast events emitted by the mux. These match the
// server→client message shapes documented in protocol/PROTOCOL.md.

type BroadcastType string

const (
	BroadcastThreadStarted BroadcastType = "thread_started"
	BroadcastThreadBusy    BroadcastType = "thread_busy"
	BroadcastThreadIdle    BroadcastType = "thread_idle"
	BroadcastThreadEnded   BroadcastType = "thread_ended"
)

// Broadcast is the envelope written to every connected WS client. Optional
// fields are omitted when absent so clients don't have to gate on nulls.
type Broadcast struct {
	Type             BroadcastType `json:"type"`
	Thread           string        `json:"thread"`
	SessionID        string        `json:"session_id,omitempty"`
	Label            string        `json:"label,omitempty"`
	Agent            string        `json:"agent,omitempty"`
	CWD              string        `json:"cwd,omitempty"`
	LastAssistant    string        `json:"lastAssistant,omitempty"`
	Awaiting         string        `json:"awaiting,omitempty"` // "permission" | "reply"
	PermissionPrompt string        `json:"permissionPrompt,omitempty"`
	Inferred         bool          `json:"inferred,omitempty"`
	At               int64         `json:"at"`
}
