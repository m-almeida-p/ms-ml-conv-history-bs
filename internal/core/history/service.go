// Package history serves the chat history: it checks a read request, turns the
// cursors a client sends into store queries, and builds the cursors that continue
// the page.
package history

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
)

var _ service.History = (*Service)(nil)

// identifier is what a conversation id or an account id may look like. It keeps
// out anything that could not be one, without tying the service to a format.
var identifier = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

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
	if !identifier.MatchString(req.ConversationID) {
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
		query.CursorID = id
	case req.After != nil:
		query.Direction = domain.DirectionAfter
		id, err := parseCursor(*req.After)
		if err != nil {
			return domain.MessagePage{}, err
		}
		query.CursorID = id
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
		result.BeforeCursor = strconv.FormatInt(page.Messages[0].OutboxID, 10)
		result.AfterCursor = strconv.FormatInt(page.Messages[n-1].OutboxID, 10)
	}

	return result, nil
}

// Conversations implements service.History.
func (s *Service) Conversations(ctx context.Context, idYapeAccount string) (domain.ConversationList, error) {
	if !identifier.MatchString(idYapeAccount) {
		return domain.ConversationList{}, fmt.Errorf("%w: idYapeAccount is missing or malformed", domain.ErrInvalidRequest)
	}

	return s.messages.ListConversations(ctx, idYapeAccount)
}

// parseCursor reads a message _id. Only the plain decimal form that the service
// itself hands out is accepted, so "+5" or "05" are not cursors.
func parseCursor(cursor string) (int64, error) {
	id, err := strconv.ParseInt(cursor, 10, 64)
	if err != nil || id < 1 || strconv.FormatInt(id, 10) != cursor {
		return 0, fmt.Errorf("%w: the cursor is not a message _id", domain.ErrInvalidCursor)
	}

	return id, nil
}
