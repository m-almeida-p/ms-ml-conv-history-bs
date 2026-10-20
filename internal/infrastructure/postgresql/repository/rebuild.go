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
		return domain.RebuildResult{}, dbError("begin transaction", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, sqlDeleteMessages); err != nil {
		return domain.RebuildResult{}, dbError("empty messages", err)
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
			if err := replayEvent(ctx, tx, project, event, &result); err != nil {
				return domain.RebuildResult{}, err
			}
		}

		if len(batch) < r.batchSize {
			break
		}
		last := batch[len(batch)-1]
		lastConv, lastSeq = last.ConversationID, last.Sequence
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.RebuildResult{}, dbError("commit rebuild", err)
	}

	return result, nil
}

// replayEvent projects one inbox event and writes its message, counting what it
// did. A cancelled context stops the replay at the next event.
func replayEvent(ctx context.Context, tx pgx.Tx, project func(domain.InboxEvent) (*domain.Message, error), event domain.InboxEvent, result *domain.RebuildResult) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("replay stopped at event %s: %w", event.Ref(), err)
	}

	result.Events++

	message, err := project(event)
	switch {
	case errors.Is(err, domain.ErrInvalidEvent):
		result.Invalid++

		return nil
	case err != nil:
		return fmt.Errorf("project event %s: %w", event.Ref(), err)
	case message == nil:
		return nil
	}

	if err := upsertMessage(ctx, tx, *message); err != nil {
		return dbError(fmt.Sprintf("upsert message of event %s", event.Ref()), err)
	}
	result.Messages++

	return nil
}

// readBatch returns the next inbox events after (conversationID, sequence). The
// rows are read to the end and closed before it returns, because a connection
// inside a transaction cannot run another statement while rows are open.
func (r *Inbox) readBatch(ctx context.Context, tx pgx.Tx, conversationID string, sequence int) ([]domain.InboxEvent, error) {
	rows, err := tx.Query(ctx, sqlSelectInboxBatch, conversationID, sequence, r.batchSize)
	if err != nil {
		return nil, dbError("read inbox batch", err)
	}
	defer rows.Close()

	events := make([]domain.InboxEvent, 0, r.batchSize)
	for rows.Next() {
		var (
			e                                domain.InboxEvent
			correlationID, account, actorRaw *string
		)
		if err := rows.Scan(&e.ConversationID, &e.Sequence, &e.EventName, &e.EventVersion,
			&e.Payload, &e.Metadata, &correlationID, &account, &actorRaw, &e.OccurredAt,
			&e.KafkaTopic, &e.KafkaPartition, &e.KafkaOffset); err != nil {
			return nil, dbError("scan inbox event", err)
		}
		e.CorrelationID, e.IDYapeAccount, e.Actor = value(correlationID), value(account), value(actorRaw)
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, dbError("read inbox batch", err)
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
