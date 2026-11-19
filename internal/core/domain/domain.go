// Package domain holds the types of the conversation history. It imports no
// infrastructure.
package domain

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
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

// flowName is what a flow is called in the event store: lower-case words joined by
// underscores, such as p2p or bill_payment.
var flowName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// ValidFlow reports whether name can be a flow.
func ValidFlow(name string) bool {
	return flowName.MatchString(name)
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
	// ErrInvalidRequest means the request is malformed, such as two cursors at once. A reader
	// that wants the words for the client takes them from an *InvalidRequestError.
	ErrInvalidRequest = errors.New("invalid request")
)

// InvalidRequestError is an ErrInvalidRequest that carries what is wrong, worded for the
// client and without the value that was wrong, so it is safe to show. errors.Is(err,
// ErrInvalidRequest) holds for it, and errors.As finds it through any context wrapped around it.
type InvalidRequestError struct {
	// Detail says what is wrong with the request, never with which value.
	Detail string
}

// InvalidRequest returns an InvalidRequestError whose detail is the formatted text.
func InvalidRequest(format string, args ...any) error {
	return &InvalidRequestError{Detail: fmt.Sprintf(format, args...)}
}

// Error implements error.
func (e *InvalidRequestError) Error() string {
	return ErrInvalidRequest.Error() + ": " + e.Detail
}

// Is makes an InvalidRequestError match ErrInvalidRequest.
func (e *InvalidRequestError) Is(target error) bool {
	return target == ErrInvalidRequest
}

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

// ConversationCursor is a position in an account's list of conversations: the time of a
// conversation's last message and its id as the tie-break. It is the cursor the API issues, and
// it carries the position itself, so it stays right even when the conversation it came from
// receives a message and moves to the top. A client treats it as opaque and never builds one.
type ConversationCursor struct {
	LastOccurredAt time.Time
	ConversationID string
}

// cursorSeparator splits the instant from the id. It is not a character an id may hold, and it
// needs no escaping in a URL.
const cursorSeparator = "~"

// maxCursorMicros is the last instant of the year 9999, in Unix microseconds.
const maxCursorMicros = 253402300799999999

// NewConversationCursor returns the cursor of a conversation whose last message was at the given time.
func NewConversationCursor(lastOccurredAt time.Time, conversationID string) ConversationCursor {
	return ConversationCursor{LastOccurredAt: lastOccurredAt.UTC(), ConversationID: conversationID}
}

// String renders the cursor: the instant in Unix microseconds, the separator and the id.
func (c ConversationCursor) String() string {
	return strconv.FormatInt(c.LastOccurredAt.UnixMicro(), 10) + cursorSeparator + c.ConversationID
}

// ParseConversationCursor reads a cursor back. Only the form String writes is accepted, so "+5",
// "05" or an id with characters outside ValidIdentifier are not cursors. It returns ErrInvalidCursor otherwise.
func ParseConversationCursor(cursor string) (ConversationCursor, error) {
	instant, id, found := strings.Cut(cursor, cursorSeparator)
	micros, err := strconv.ParseInt(instant, 10, 64)
	if !found || err != nil || micros < 1 || micros > maxCursorMicros || strconv.FormatInt(micros, 10) != instant || !ValidIdentifier(id) {
		return ConversationCursor{}, fmt.Errorf("%w: the cursor is not a conversation list cursor", ErrInvalidCursor)
	}

	return ConversationCursor{LastOccurredAt: time.UnixMicro(micros).UTC(), ConversationID: id}, nil
}

// ConversationQuery asks for one page of an account's conversations. Cursor is the position to
// read from and is ignored for DirectionNewest. Before reads the conversations older than the
// cursor (less recently active), After the ones more recently active. Size is how many to return,
// from 1 to ConversationListSize.
type ConversationQuery struct {
	IDYapeAccount string
	Direction     Direction
	Cursor        ConversationCursor
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
	// BeforeCursor is the cursor of the oldest conversation of the page, the one that
	// continues backwards; AfterCursor is the cursor of the most recent one, the one that
	// continues forwards. Both are in ConversationCursor form and empty for an empty page.
	BeforeCursor string
	AfterCursor  string
}

// ConversationListSize is how many conversations a page holds when the client does
// not ask for fewer. It is also the most a client can ask for.
const ConversationListSize = 100
