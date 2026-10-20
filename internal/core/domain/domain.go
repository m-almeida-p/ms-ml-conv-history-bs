// Package domain holds the types of the conversation history. It imports no
// infrastructure.
package domain

import (
	"encoding/json"
	"errors"
	"time"
)

// Role is who wrote a message, in Smooch terms.
type Role string

const (
	// RoleUser is a message from the customer.
	RoleUser Role = "appUser"
	// RoleMaker is a message from the assistant, guardrail replies included.
	RoleMaker Role = "appMaker"
)

const (
	// MakerAuthorID is the author of every appMaker message.
	MakerAuthorID = "ms-ml-genai-agent-bs"
	// MessageTypeText is the only message type for now.
	MessageTypeText = "text"
)

// Event names that become chat messages.
const (
	EventUserMessageSent       = "UserMessageSent"
	EventAgentMessageSent      = "AgentMessageSent"
	EventGuardrailResponseSent = "GuardrailResponseSent"
)

// ErrInvalidEvent marks an event that should project but lacks what the
// projection needs, such as its text. Wrap it with the reason.
var ErrInvalidEvent = errors.New("invalid event")

// ErrRejected marks an event the store refuses for good, such as a violated
// constraint. Trying again cannot help, so a consumer skips it. The error
// carries the constraint name and code, never the row.
var ErrRejected = errors.New("event rejected by the store")

// InboxEvent is one outbox row as it arrived from Kafka. Payload and Metadata
// hold JSON. An absent CorrelationID, IDYapeAccount or Actor is the empty string.
type InboxEvent struct {
	OutboxID       int64
	ConversationID string
	Sequence       int
	EventName      string
	EventVersion   int
	Payload        json.RawMessage
	Metadata       json.RawMessage
	CorrelationID  string
	IDYapeAccount  string
	Actor          string
	OccurredAt     time.Time

	// Where the record came from, kept for audit only.
	KafkaTopic     string
	KafkaPartition int32
	KafkaOffset    int64
}

// Message is one line of the chat between the user and the assistant.
type Message struct {
	ConversationID string
	Sequence       int
	OutboxID       int64
	Role           Role
	AuthorID       string
	Type           string
	Text           string
	Truncated      bool
	SourceEvent    string
	IDYapeAccount  string
	ReceivedAt     time.Time
}

// ApplyResult tells what Apply did with an event.
type ApplyResult struct {
	// Applied is false when the event was received before and nothing was written.
	Applied bool
	// Projected is true when the event also added a chat message.
	Projected bool
	// Invalid is true when the event should have projected but could not. It is
	// stored in the inbox without a message.
	Invalid bool
}

// RebuildResult counts what a rebuild did.
type RebuildResult struct {
	// Events is how many inbox events were replayed.
	Events int
	// Messages is how many chat messages were written.
	Messages int
	// Invalid is how many events should have projected but could not. They stay
	// in the inbox and add no message.
	Invalid int
}

// PageSize is how many messages one page holds.
const PageSize = 100

// Direction says which way a page of messages is read, relative to a cursor.
type Direction string

const (
	// DirectionNewest reads the most recent page. It takes no cursor.
	DirectionNewest Direction = "newest"
	// DirectionBefore reads the page of messages older than the cursor.
	DirectionBefore Direction = "before"
	// DirectionAfter reads the page of messages newer than the cursor.
	DirectionAfter Direction = "after"
)

// The errors a read can end with. A caller maps them to its own answer.
var (
	// ErrNotFound means the conversation has no chat messages.
	ErrNotFound = errors.New("not found")
	// ErrInvalidCursor means the cursor is not the _id of a message of that conversation.
	ErrInvalidCursor = errors.New("invalid cursor")
	// ErrInvalidRequest means the request is malformed, such as two cursors at once.
	ErrInvalidRequest = errors.New("invalid request")
)

// PageQuery asks for one page of a conversation. CursorID is the outbox id of a
// message of that conversation, and is ignored for DirectionNewest.
type PageQuery struct {
	ConversationID string
	Direction      Direction
	CursorID       int64
}

// Page is what the store returns: up to PageSize messages in ascending order of
// sequence, and whether more exist in the direction that was read.
type Page struct {
	Messages []Message
	HasMore  bool
}

// MessagePage is a page ready to be served, with the cursors that continue it.
type MessagePage struct {
	ConversationID string
	Direction      Direction
	Messages       []Message
	// HasMore is true when older messages exist (newest and before) or newer ones
	// exist (after): the direction the caller is walking.
	HasMore bool
	// BeforeCursor is the _id of the first message of the page, AfterCursor of the
	// last. Both are empty for an empty page.
	BeforeCursor string
	AfterCursor  string
}

// ConversationSummary describes one conversation of an account.
type ConversationSummary struct {
	ConversationID  string
	MessageCount    int
	FirstReceivedAt time.Time
	LastReceivedAt  time.Time
}

// ConversationList is the most recent conversations of an account.
type ConversationList struct {
	Conversations []ConversationSummary
	HasMore       bool
}

// ConversationListSize is how many conversations one list holds.
const ConversationListSize = 100
