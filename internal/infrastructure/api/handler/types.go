// Package handler holds the HTTP handlers of the history API. Request and
// response types live here, in camelCase as the Smooch API writes them; no
// domain type is exposed.
package handler

import "time"

// messagesResponse is the reply for one page of a conversation.
type messagesResponse struct {
	Conversation conversationRef   `json:"conversation"`
	Messages     []messageResponse `json:"messages"`
	Meta         messagesMeta      `json:"meta"`
	Links        pageLinks         `json:"links"`
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
	// SourceEvent is the event that produced the message.
	SourceEvent string `json:"sourceEvent" enums:"UserMessageSent,AgentMessageSent,GuardrailResponseSent" example:"UserMessageSent"`
	// Truncated is true when the recorded text was cut at the agent's limit.
	Truncated bool `json:"truncated" example:"false"`
}

// messagesMeta says whether the page can be continued in the direction it was
// read, and gives the cursors of its first and last message.
type messagesMeta struct {
	// HasMore is true when older messages exist (newest and before pages) or newer ones exist (after pages).
	HasMore bool `json:"hasMore" example:"true"`
	// BeforeCursor is the _id of the first message of the page. Absent for an empty page.
	BeforeCursor string `json:"beforeCursor,omitempty" example:"1042"`
	// AfterCursor is the _id of the last message of the page. Absent for an empty page.
	AfterCursor string `json:"afterCursor,omitempty" example:"1051"`
}

// pageLinks continue the page: prev reads older messages, next reads newer ones.
// Only the link in the direction of travel is present, and only when there is more.
type pageLinks struct {
	Prev string `json:"prev,omitempty" example:"/v1/conversations/conv_v1_8ab59029-bc3c-4e77-b49f-9fb053d90c52/messages?page[before]=1042"`
	Next string `json:"next,omitempty" example:"/v1/conversations/conv_v1_8ab59029-bc3c-4e77-b49f-9fb053d90c52/messages?page[after]=1051"`
}

// conversationsResponse is the reply for an account's conversations.
type conversationsResponse struct {
	Conversations []conversationResponse `json:"conversations"`
	Meta          conversationsMeta      `json:"meta"`
}

type conversationResponse struct {
	ID           string `json:"_id" example:"conv_v1_8ab59029-bc3c-4e77-b49f-9fb053d90c52"`
	MessageCount int    `json:"messageCount" example:"5"`
	// FirstReceived and LastReceived are the times of the first and last message, in Unix seconds with milliseconds.
	FirstReceived float64 `json:"firstReceived" example:"1792505243.5"`
	LastReceived  float64 `json:"lastReceived" example:"1792505543.5"`
}

type conversationsMeta struct {
	// HasMore is true when the account has more conversations than the 100 listed.
	HasMore bool `json:"hasMore" example:"false"`
}

// errorResponse is the body of every error.
type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	// Code is one of invalid_request, invalid_cursor, not_found, method_not_allowed, internal_error.
	Code        string `json:"code" enums:"invalid_request,invalid_cursor,not_found,method_not_allowed,internal_error" example:"invalid_cursor"`
	Description string `json:"description" example:"page[before] or page[after] is not the _id of a message in this conversation."`
}

// unixSeconds renders a time as Smooch writes `received`: Unix seconds, with
// milliseconds as decimals.
func unixSeconds(t time.Time) float64 {
	return float64(t.UnixMilli()) / 1000
}
