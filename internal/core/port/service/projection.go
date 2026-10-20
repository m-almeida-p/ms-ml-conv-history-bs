// Package service holds the use cases other layers call.
package service

import (
	"context"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
)

//go:generate mockery --name Projection --filename projection.go

// Projection decides what an inbox event adds to the history.
type Projection interface {
	// Project returns the chat message of an event, or nil when the event is not
	// part of the chat. An event that should project but cannot returns an
	// error wrapping domain.ErrInvalidEvent.
	Project(event domain.InboxEvent) (*domain.Message, error)

	// Handle projects an event and stores it in the inbox, with its message, in
	// one transaction. A repeated event writes nothing (Applied=false). An event
	// that should project but cannot is stored without a message (Invalid=true).
	// A store error wrapping domain.ErrRejected will never succeed; any other
	// store error is worth retrying.
	Handle(ctx context.Context, event domain.InboxEvent) (domain.ApplyResult, error)

	// Rebuild recreates the chat messages from the inbox, for example after a
	// fix to the projection. The inbox is not modified.
	Rebuild(ctx context.Context) (domain.RebuildResult, error)
}
