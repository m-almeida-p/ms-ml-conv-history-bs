// Package history serves the chat history: it checks a read request, turns the
// cursors a client sends into store queries, and builds the cursors that continue
// the page.
package history

import (
	"context"
	"fmt"
	"strconv"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
)

var _ service.History = (*Service)(nil)

// Service implements service.History over a message repository.
type Service struct {
	messages repository.MessageRepository
}

// NewService returns the history service.
func NewService(messages repository.MessageRepository) *Service {
	return &Service{messages: messages}
}

// Messages implements service.History.
func (s *Service) Messages(ctx context.Context, req service.MessagesRequest) (domain.MessagePage, error) {
	if !domain.ValidIdentifier(req.ConversationID) {
		return domain.MessagePage{}, fmt.Errorf("%w: the conversation id is malformed", domain.ErrInvalidRequest)
	}

	query := domain.PageQuery{ConversationID: req.ConversationID, Direction: domain.DirectionNewest}

	switch {
	case req.Before != nil && req.After != nil:
		return domain.MessagePage{}, fmt.Errorf("%w: page[before] and page[after] cannot be used together", domain.ErrInvalidRequest)
	case req.Before != nil:
		query.Direction = domain.DirectionBefore
		id, err := parseCursor(*req.Before)
		if err != nil {
			return domain.MessagePage{}, err
		}
		query.CursorSequence = id
	case req.After != nil:
		query.Direction = domain.DirectionAfter
		id, err := parseCursor(*req.After)
		if err != nil {
			return domain.MessagePage{}, err
		}
		query.CursorSequence = id
	}

	page, err := s.messages.ListMessages(ctx, query)
	if err != nil {
		return domain.MessagePage{}, err
	}

	result := domain.MessagePage{
		ConversationID: req.ConversationID,
		Direction:      query.Direction,
		Messages:       page.Messages,
		HasMore:        page.HasMore,
	}
	if n := len(page.Messages); n > 0 {
		result.BeforeCursor = strconv.Itoa(page.Messages[0].Sequence)
		result.AfterCursor = strconv.Itoa(page.Messages[n-1].Sequence)
	}

	return result, nil
}

// Conversations implements service.History.
func (s *Service) Conversations(ctx context.Context, idYapeAccount string) (domain.ConversationList, error) {
	if !domain.ValidIdentifier(idYapeAccount) {
		return domain.ConversationList{}, fmt.Errorf("%w: idYapeAccount is missing or malformed", domain.ErrInvalidRequest)
	}

	return s.messages.ListConversations(ctx, idYapeAccount)
}

// parseCursor reads a message _id, which is the sequence of its event. Only the
// plain decimal form that the service itself hands out is accepted, so "+5" or
// "05" are not cursors.
func parseCursor(cursor string) (int, error) {
	id, err := strconv.ParseInt(cursor, 10, 32)
	if err != nil || id < 1 || strconv.FormatInt(id, 10) != cursor {
		return 0, fmt.Errorf("%w: the cursor is not a message _id", domain.ErrInvalidCursor)
	}

	return int(id), nil
}
