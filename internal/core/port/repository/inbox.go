// Package repository holds the storage ports.
package repository

import (
	"context"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
)

//go:generate mockery --name InboxRepository --filename inbox.go

// InboxRepository stores received events and the chat messages they project to.
type InboxRepository interface {
	// Apply stores the event and, when message is not nil, its chat message, in
	// one transaction. An event seen before (same conversation and
	// sequence) writes nothing and returns Applied=false; a different event at a
	// position already taken is domain.ErrRejected. Storing a skipped event (nil message) still marks it projected.
	Apply(ctx context.Context, event domain.InboxEvent, message *domain.Message) (domain.ApplyResult, error)

	// Rebuild empties the chat messages and replays every inbox event through
	// project, in (conversation_id, sequence) order, in one transaction. The
	// inbox is not modified. An event for which project returns
	// domain.ErrInvalidEvent is counted and skipped; any other error, or a
	// cancelled context, rolls everything back.
	Rebuild(ctx context.Context, project func(domain.InboxEvent) (*domain.Message, error)) (domain.RebuildResult, error)
}
