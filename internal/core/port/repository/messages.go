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

	// ListConversations returns the account's conversations with chat messages,
	// the most recent first, up to domain.ConversationListSize.
	ListConversations(ctx context.Context, idYapeAccount string) (domain.ConversationList, error)
}
