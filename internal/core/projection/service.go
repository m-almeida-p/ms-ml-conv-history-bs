// Package projection turns inbox events into chat messages.
package projection

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository"
)

// Service maps events to messages and rebuilds the messages from the inbox.
type Service struct {
	inbox repository.InboxRepository
}

// NewService returns the projection service over inbox.
func NewService(inbox repository.InboxRepository) *Service {
	return &Service{inbox: inbox}
}

// Handle projects an event and stores it, with its message, in one transaction.
// An event that should project but cannot (domain.ErrInvalidEvent) is still
// stored, without a message, and reported through ApplyResult.Invalid, so the
// inbox keeps everything that arrived and a later fix can rebuild from it.
func (s *Service) Handle(ctx context.Context, event domain.InboxEvent) (domain.ApplyResult, error) {
	// Project's only error is ErrInvalidEvent, and it returns no message with it.
	message, err := s.Project(event)
	invalid := err != nil

	result, err := s.inbox.Apply(ctx, event, message)
	if err != nil {
		return domain.ApplyResult{}, err
	}
	result.Invalid = invalid && result.Applied

	return result, nil
}

// messagePayload is the part of a chat event's payload the history reads. Text
// fields are pointers so a missing key differs from an empty string.
type messagePayload struct {
	Text         *string `json:"text"`
	ResponseText *string `json:"response_text"`
	Truncated    bool    `json:"truncated"`
}

// Project returns the chat message of an event, or nil when the event is not
// part of the chat (tool summaries, notifications, blocked input and so on).
// Skipping is not an error. An event that should project but cannot returns an
// error wrapping domain.ErrInvalidEvent.
func (s *Service) Project(event domain.InboxEvent) (*domain.Message, error) {
	var (
		role      domain.Role
		textField func(messagePayload) *string
	)

	switch event.EventName {
	case domain.EventUserMessageSent:
		role = domain.RoleUser
		textField = func(p messagePayload) *string { return p.Text }
	case domain.EventAgentMessageSent:
		role = domain.RoleMaker
		textField = func(p messagePayload) *string { return p.Text }
	case domain.EventGuardrailResponseSent:
		role = domain.RoleMaker
		textField = func(p messagePayload) *string { return p.ResponseText }
	default:
		return nil, nil
	}

	var payload messagePayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return nil, fmt.Errorf("%w: %s payload is not a JSON object: %v", domain.ErrInvalidEvent, event.EventName, err)
	}

	text := textField(payload)
	if text == nil {
		return nil, fmt.Errorf("%w: %s has no text", domain.ErrInvalidEvent, event.EventName)
	}

	authorID := domain.MakerAuthorID
	if role == domain.RoleUser {
		if event.IDYapeAccount == "" {
			return nil, fmt.Errorf("%w: %s has no account for its author", domain.ErrInvalidEvent, event.EventName)
		}
		authorID = event.IDYapeAccount
	}

	return &domain.Message{
		ConversationID: event.ConversationID,
		Sequence:       event.Sequence,
		Role:           role,
		AuthorID:       authorID,
		Type:           domain.MessageTypeText,
		Text:           *text,
		Truncated:      payload.Truncated,
		EventName:      event.EventName,
		Flow:           event.Flow,
		IDYapeAccount:  event.IDYapeAccount,
		OccurredAt:     event.OccurredAt,
	}, nil
}
