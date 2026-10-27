package service

import (
	"context"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
)

//go:generate mockery --name History --filename history.go

// MessagesRequest asks for a page of a conversation. Before and After are message
// _ids as a client sends them; nil means the parameter was not sent, and an empty
// string means it was sent empty, which is an invalid cursor. At most one may be
// set, and neither means the newest page. Size is page[size] as a client sends it:
// nil means the parameter was not sent and the page holds domain.PageSize messages.
type MessagesRequest struct {
	ConversationID string
	Before         *string
	After          *string
	Size           *string
}

// ConversationsRequest asks for a page of an account's conversations. Before and After
// are conversation ids as a client sends them, and Size is page[size]; nil means the
// parameter was not sent, and an empty string for a cursor means it was sent empty, which
// is an invalid cursor. At most one cursor may be set, and neither means the most recent page.
type ConversationsRequest struct {
	IDYapeAccount string
	Before        *string
	After         *string
	Size          *string
}

// History is what a client can read.
type History interface {
	// Messages returns one page of a conversation. Errors wrap domain.ErrInvalidRequest
	// (malformed id, both cursors, a page size outside 1 to domain.PageSize), domain.ErrInvalidCursor (not a message _id of that
	// conversation) or domain.ErrNotFound (no chat messages).
	Messages(ctx context.Context, req MessagesRequest) (domain.MessagePage, error)

	// Conversations returns one page of the account's conversations, the most recent
	// first. Errors wrap domain.ErrInvalidRequest (missing or malformed account, both
	// cursors, a page size outside 1 to domain.ConversationListSize) or
	// domain.ErrInvalidCursor (not a conversation of that account). An account with
	// none returns an empty page, not an error.
	Conversations(ctx context.Context, req ConversationsRequest) (domain.ConversationList, error)
}
