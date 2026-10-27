package repository

import (
	"context"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
)

//go:generate mockery --name MessageRepository --filename messages.go

// MessageRepository reads the chat history.
type MessageRepository interface {
	// ListMessages returns one page of a conversation, ascending by sequence.
	// It returns domain.ErrNotFound when the conversation has no messages and
	// domain.ErrInvalidCursor when the cursor is not a message of that conversation.
	ListMessages(ctx context.Context, query domain.PageQuery) (domain.Page, error)

	// ListConversations returns one page of the account's conversations with chat
	// messages, the most recent first. It returns domain.ErrInvalidCursor when the
	// cursor is not a conversation of that account. An account with none gets an
	// empty page, not an error.
	ListConversations(ctx context.Context, query domain.ConversationQuery) (domain.ConversationList, error)
}
