// Package handler holds the HTTP handlers of the history API. Request and
// response types live here, in camelCase as the Smooch API writes them; no
// domain type is exposed.
package handler

import "time"

// messagesResponse is the reply for one page of a conversation. It holds the data
// only: whether more pages exist, and where, is the Link header.
type messagesResponse struct {
	Conversation conversationRef   `json:"conversation"`
	Messages     []messageResponse `json:"messages"`
}

type conversationRef struct {
	// ID is the conversation id (the A2A contextId).
	ID string `json:"_id" example:"conv_v1_8ab59029-bc3c-4e77-b49f-9fb053d90c52"`
}

// messageResponse is a Smooch-shaped message. Fields the history does not hold
// (name, avatarUrl, actions, items, mediaUrl, quotedMessage, subroles) are left out.
type messageResponse struct {
	// ID is the message id and the paging cursor: the sequence of its event in the conversation, as a string. It is unique within the conversation.
	ID string `json:"_id" example:"1042"`
	// Type is the message type. Always text for now.
	Type string `json:"type" example:"text"`
	// Role is who wrote it: appUser for the customer, appMaker for the assistant and its guardrail replies.
	Role string `json:"role" enums:"appUser,appMaker" example:"appUser"`
	// AuthorID is the customer account for appUser and ms-ml-genai-agent-bs for appMaker.
	AuthorID string `json:"authorId" example:"c59f8a80-6e65-4d1b-9c0e-0a1b2c3d4e5f"`
	Text     string `json:"text" example:"quiero yapear"`
	// Received is when it happened, in Unix seconds with milliseconds as decimals.
	Received float64         `json:"received" example:"1792505243.792"`
	Source   sourceResponse  `json:"source"`
	Metadata messageMetadata `json:"metadata"`
}

type sourceResponse struct {
	Type string `json:"type" example:"api"`
}

type messageMetadata struct {
	// Sequence is the position of the message's event in the conversation. Gaps are normal.
	Sequence int `json:"sequence" example:"2"`
	// EventName is the event that produced the message.
	EventName string `json:"eventName" enums:"UserMessageSent,AgentMessageSent,GuardrailResponseSent" example:"UserMessageSent"`
	// Truncated is true when the recorded text was cut at the agent's limit.
	Truncated bool `json:"truncated" example:"false"`
	// Flow is the flow the message took place in, such as p2p. It is absent outside any flow. A message the user writes carries the flow they were in when they wrote it, so the message that opens a flow has none and the one that leaves it carries the flow it was in.
	Flow string `json:"flow,omitempty" example:"p2p"`
}

// conversationsResponse is the reply for one page of an account's conversations. It holds the
// data only: whether more pages exist, and where, is the Link header.
type conversationsResponse struct {
	Conversations []conversationResponse `json:"conversations"`
}

type conversationResponse struct {
	ID string `json:"_id" example:"conv_v1_8ab59029-bc3c-4e77-b49f-9fb053d90c52"`
	// Cursor is the position of this conversation in the list when it was read. It is opaque: pass it
	// as page[before] for the conversations older than this position or as page[after] for the more
	// recent ones. It stays right even if the conversation receives a message later.
	Cursor       string `json:"cursor" example:"1792771602218000~conv_v1_8ab59029-bc3c-4e77-b49f-9fb053d90c52"`
	MessageCount int    `json:"messageCount" example:"5"`
	// FirstReceived and LastReceived are the times of the first and last message, in Unix seconds with milliseconds.
	FirstReceived float64 `json:"firstReceived" example:"1792505243.5"`
	LastReceived  float64 `json:"lastReceived" example:"1792505543.5"`
}

// problemResponse is the body of every error: RFC 9457 problem details, served as
// application/problem+json. Clients tell problems apart by Type.
type problemResponse struct {
	// Type identifies the kind of problem. One of the service's tag URIs (invalid-request,
	// invalid-cursor, conversation-not-found, under tag:yape.com.pe,2026:conversation-history:), or
	// about:blank when the problem means nothing beyond its HTTP status.
	Type string `json:"type" example:"tag:yape.com.pe,2026:conversation-history:invalid-cursor"`
	// Title is a short summary of the type; for about:blank it is the HTTP status phrase.
	Title string `json:"title" example:"Invalid cursor"`
	// Status repeats the HTTP status code of the response.
	Status int `json:"status" example:"400"`
	// Detail explains this occurrence without repeating what the client sent. Do not parse it.
	Detail string `json:"detail" example:"page[before] or page[after] is not the _id of a message in this conversation."`
}

// unixSeconds renders a time as Smooch writes `received`: Unix seconds, with
// milliseconds as decimals.
func unixSeconds(t time.Time) float64 {
	return float64(t.UnixMilli()) / 1000
}
