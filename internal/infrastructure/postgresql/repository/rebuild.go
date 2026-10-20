package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
)

// Rebuild implements repository.InboxRepository. It empties the messages and
// replays the inbox in keyset batches ordered by (conversation_id, sequence),
// all in one transaction, so readers see the old messages until the commit.
func (r *Inbox) Rebuild(ctx context.Context, project func(domain.InboxEvent) (*domain.Message, error)) (domain.RebuildResult, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return domain.RebuildResult{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, sqlDeleteMessages); err != nil {
		return domain.RebuildResult{}, fmt.Errorf("empty messages: %w", err)
	}

	var (
		result   domain.RebuildResult
		lastConv string
		lastSeq  = -1
	)

	for {
		batch, err := r.readBatch(ctx, tx, lastConv, lastSeq)
		if err != nil {
			return domain.RebuildResult{}, err
		}

		for _, event := range batch {
			// A cancelled context stops the replay at the next event.
			if err := ctx.Err(); err != nil {
				return domain.RebuildResult{}, fmt.Errorf("replay stopped at event %d: %w", event.OutboxID, err)
			}

			result.Events++

			message, err := project(event)
			switch {
			case errors.Is(err, domain.ErrInvalidEvent):
				result.Invalid++
				continue
			case err != nil:
				return domain.RebuildResult{}, fmt.Errorf("project event %d: %w", event.OutboxID, err)
			case message == nil:
				continue
			}

			if err := upsertMessage(ctx, tx, *message); err != nil {
				return domain.RebuildResult{}, fmt.Errorf("upsert message of event %d: %w", event.OutboxID, err)
			}
			result.Messages++
		}

		if len(batch) < r.batchSize {
			break
		}
		last := batch[len(batch)-1]
		lastConv, lastSeq = last.ConversationID, last.Sequence
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.RebuildResult{}, fmt.Errorf("commit rebuild: %w", err)
	}

	return result, nil
}

// readBatch returns the next inbox events after (conversationID, sequence). The
// rows are read to the end and closed before it returns, because a connection
// inside a transaction cannot run another statement while rows are open.
func (r *Inbox) readBatch(ctx context.Context, tx pgx.Tx, conversationID string, sequence int) ([]domain.InboxEvent, error) {
	rows, err := tx.Query(ctx, sqlSelectInboxBatch, conversationID, sequence, r.batchSize)
	if err != nil {
		return nil, fmt.Errorf("read inbox batch: %w", err)
	}
	defer rows.Close()

	events := make([]domain.InboxEvent, 0, r.batchSize)
	for rows.Next() {
		var (
			e                                domain.InboxEvent
			correlationID, account, actorRaw *string
		)
		if err := rows.Scan(&e.OutboxID, &e.ConversationID, &e.Sequence, &e.EventName, &e.EventVersion,
			&e.Payload, &e.Metadata, &correlationID, &account, &actorRaw, &e.OccurredAt,
			&e.KafkaTopic, &e.KafkaPartition, &e.KafkaOffset); err != nil {
			return nil, fmt.Errorf("scan inbox event: %w", err)
		}
		e.CorrelationID, e.IDYapeAccount, e.Actor = value(correlationID), value(account), value(actorRaw)
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read inbox batch: %w", err)
	}

	return events, nil
}

// value reads a nullable text column; NULL is the empty string.
func value(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}
