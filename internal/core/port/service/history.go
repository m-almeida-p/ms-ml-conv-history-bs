package service

import (
	"context"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
)

//go:generate mockery --name History --filename history.go

// MessagesRequest asks for a page of a conversation. Before and After are message
// _ids as a client sends them; nil means the parameter was not sent, and an empty
// string means it was sent empty, which is an invalid cursor. At most one may be
// set, and neither means the newest page.
type MessagesRequest struct {
	ConversationID string
	Before         *string
	After          *string
}

// History is what a client can read.
type History interface {
	// Messages returns one page of a conversation. Errors wrap domain.ErrInvalidRequest
	// (malformed id, both cursors), domain.ErrInvalidCursor (not a message _id of that
	// conversation) or domain.ErrNotFound (no chat messages).
	Messages(ctx context.Context, req MessagesRequest) (domain.MessagePage, error)

	// Conversations lists the account's conversations, the most recent first.
	// An account with none returns an empty list, not an error.
	Conversations(ctx context.Context, idYapeAccount string) (domain.ConversationList, error)
}
