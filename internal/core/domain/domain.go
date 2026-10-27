// Package domain holds the types of the conversation history. It imports no
// infrastructure.
package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
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
// constraint or a stream position that already holds another event. Trying again
// cannot help, so a consumer skips it. The error carries the constraint name and
// code, never the row.
var ErrRejected = errors.New("event rejected by the store")

// flowName is what a flow is called in the event store: lower-case words joined by
// underscores, such as p2p or bill_payment.
var flowName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// ValidFlow reports whether name can be a flow.
func ValidFlow(name string) bool {
	return flowName.MatchString(name)
}

// InboxEvent is one conversation_events row as it arrived from Kafka. Its stream
// position, ConversationID and Sequence, is its identity. Payload and Metadata
// hold JSON. An absent CorrelationID, IDYapeAccount or Actor is the empty string.
type InboxEvent struct {
	ConversationID string
	Sequence       int
	EventName      string
	EventVersion   int
	Payload        json.RawMessage
	Metadata       json.RawMessage
	CorrelationID  string
	IDYapeAccount  string
	Actor          string
	// Flow is the flow the fact took place in, such as p2p, or empty outside any flow.
	Flow       string
	OccurredAt time.Time

	// Where the record came from, kept for audit only.
	KafkaTopic     string
	KafkaPartition int32
	KafkaOffset    int64
}

// Ref names the event by where its record sits in Kafka, for logs and errors.
// The coordinates find the record; no conversation id, account or content is in
// them, so they are safe to log.
func (e InboxEvent) Ref() string {
	return fmt.Sprintf("%s/%d/%d", e.KafkaTopic, e.KafkaPartition, e.KafkaOffset)
}

// identifier is what a conversation id or an account id may look like. The
// decoder and the API share it, so everything stored can be asked for.
var identifier = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// ValidIdentifier reports whether id can be a conversation id or an account id.
func ValidIdentifier(id string) bool {
	return identifier.MatchString(id)
}

// Message is one line of the chat between the user and the assistant.
type Message struct {
	ConversationID string
	Sequence       int
	Role           Role
	AuthorID       string
	Type           string
	Text           string
	Truncated      bool
	EventName      string
	IDYapeAccount  string
	// Flow is the flow the message took place in, or empty outside any flow.
	Flow       string
	OccurredAt time.Time
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

// PageSize is how many messages a page holds when the client does not ask for
// fewer. It is also the most a client can ask for.
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

// PageQuery asks for one page of a conversation. CursorSequence is the sequence
// of a message of that conversation, and is ignored for DirectionNewest. Size is
// how many messages to return, from 1 to PageSize.
type PageQuery struct {
	ConversationID string
	Direction      Direction
	CursorSequence int
	Size           int
}

// Page is what the store returns: up to Size messages in ascending order of
// sequence, and whether more exist in the direction that was read.
type Page struct {
	Messages []Message
	HasMore  bool
}

// MessagePage is a page ready to be served, with the cursors that continue it.
type MessagePage struct {
	ConversationID string
	Direction      Direction
	// Size is the page size the read used: the one the client asked for, or PageSize.
	Size     int
	Messages []Message
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
	FirstOccurredAt time.Time
	LastOccurredAt  time.Time
}

// ConversationQuery asks for one page of an account's conversations. CursorID is
// the id of a conversation of that account and is ignored for DirectionNewest.
// Before reads the conversations older than the cursor (less recently active),
// After the ones more recently active. Size is how many to return, from 1 to
// ConversationListSize.
type ConversationQuery struct {
	IDYapeAccount string
	Direction     Direction
	CursorID      string
	Size          int
}

// ConversationList is one page of an account's conversations, the most recent
// first, with the cursors that continue it.
type ConversationList struct {
	Direction Direction
	// Size is the page size the read used: the one the client asked for, or ConversationListSize.
	Size          int
	Conversations []ConversationSummary
	// HasMore is true when older conversations exist (newest and before pages) or more
	// recent ones exist (after pages): the direction the caller is walking.
	HasMore bool
	// BeforeCursor is the id of the oldest conversation of the page, the one that
	// continues backwards; AfterCursor is the id of the most recent one, the one that
	// continues forwards. Both are empty for an empty page.
	BeforeCursor string
	AfterCursor  string
}

// ConversationListSize is how many conversations a page holds when the client does
// not ask for fewer. It is also the most a client can ask for.
const ConversationListSize = 100
