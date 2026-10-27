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
		return domain.MessagePage{}, domain.InvalidRequest("the conversation id is malformed")
	}

	size, err := parsePageSize(req.Size, domain.PageSize)
	if err != nil {
		return domain.MessagePage{}, err
	}

	query := domain.PageQuery{ConversationID: req.ConversationID, Direction: domain.DirectionNewest, Size: size}

	switch {
	case req.Before != nil && req.After != nil:
		return domain.MessagePage{}, domain.InvalidRequest("page[before] and page[after] cannot be used together")
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
		Size:           size,
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
func (s *Service) Conversations(ctx context.Context, req service.ConversationsRequest) (domain.ConversationList, error) {
	if !domain.ValidIdentifier(req.IDYapeAccount) {
		return domain.ConversationList{}, domain.InvalidRequest("idYapeAccount is missing or malformed")
	}

	size, err := parsePageSize(req.Size, domain.ConversationListSize)
	if err != nil {
		return domain.ConversationList{}, err
	}

	query := domain.ConversationQuery{IDYapeAccount: req.IDYapeAccount, Direction: domain.DirectionNewest, Size: size}

	var rawCursor string
	switch {
	case req.Before != nil && req.After != nil:
		return domain.ConversationList{}, domain.InvalidRequest("page[before] and page[after] cannot be used together")
	case req.Before != nil:
		query.Direction, rawCursor = domain.DirectionBefore, *req.Before
	case req.After != nil:
		query.Direction, rawCursor = domain.DirectionAfter, *req.After
	}
	if query.Direction != domain.DirectionNewest {
		cursor, err := domain.ParseConversationCursor(rawCursor)
		if err != nil {
			return domain.ConversationList{}, err
		}
		query.Cursor = cursor
	}

	list, err := s.messages.ListConversations(ctx, query)
	if err != nil {
		return domain.ConversationList{}, err
	}

	list.Direction, list.Size = query.Direction, size
	if n := len(list.Conversations); n > 0 {
		// The page is most recent first: the first conversation continues forwards, the last backwards.
		list.AfterCursor = conversationCursor(list.Conversations[0]).String()
		list.BeforeCursor = conversationCursor(list.Conversations[n-1]).String()
	}

	return list, nil
}

// conversationCursor is the position of a conversation in the list.
func conversationCursor(c domain.ConversationSummary) domain.ConversationCursor {
	return domain.NewConversationCursor(c.LastOccurredAt, c.ConversationID)
}

// parsePageSize reads page[size]. Absent means a full page of limit. Present, it must be
// a plain decimal from 1 to limit, in the form the service documents.
func parsePageSize(raw *string, limit int) (int, error) {
	if raw == nil {
		return limit, nil
	}
	size, err := strconv.ParseInt(*raw, 10, 32)
	if err != nil || size < 1 || size > int64(limit) || strconv.FormatInt(size, 10) != *raw {
		return 0, domain.InvalidRequest("page[size] must be a whole number from 1 to %d", limit)
	}

	return int(size), nil
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
