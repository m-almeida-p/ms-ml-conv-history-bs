// Package repository implements the storage ports on Postgres.
package repository

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql"
)

var (
	//go:embed SQL/insert_inbox.sql
	sqlInsertInbox string
	//go:embed SQL/upsert_message.sql
	sqlUpsertMessage string
	//go:embed SQL/delete_messages.sql
	sqlDeleteMessages string
	//go:embed SQL/select_inbox_batch.sql
	sqlSelectInboxBatch string
	//go:embed SQL/select_inbox_position.sql
	sqlSelectInboxPosition string
)

// defaultBatchSize is how many inbox rows a rebuild reads at a time, so memory
// stays flat however large the inbox grows.
const defaultBatchSize = 500

// Inbox is the Postgres InboxRepository.
type Inbox struct {
	db        postgresql.DB
	batchSize int
}

// Option tunes an Inbox.
type Option func(*Inbox)

// WithBatchSize sets how many inbox rows a rebuild reads at a time. A value
// below 1 keeps the default.
func WithBatchSize(size int) Option {
	return func(i *Inbox) {
		if size > 0 {
			i.batchSize = size
		}
	}
}

// NewInbox returns an inbox over db.
func NewInbox(db postgresql.DB, opts ...Option) *Inbox {
	inbox := &Inbox{db: db, batchSize: defaultBatchSize}
	for _, opt := range opts {
		opt(inbox)
	}

	return inbox
}

// Apply implements repository.InboxRepository. The dedupe is the primary key:
// ON CONFLICT DO NOTHING plus RowsAffected tells a new event from a repeat.
func (r *Inbox) Apply(ctx context.Context, event domain.InboxEvent, message *domain.Message) (domain.ApplyResult, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return domain.ApplyResult{}, fmt.Errorf("begin transaction: %w", err)
	}
	// A rollback after a commit returns an error that means nothing here.
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, sqlInsertInbox,
		event.OutboxID, event.ConversationID, event.Sequence, event.EventName, event.EventVersion,
		event.Payload, event.Metadata,
		nullable(event.CorrelationID), nullable(event.IDYapeAccount), nullable(event.Actor), event.OccurredAt,
		event.KafkaTopic, event.KafkaPartition, event.KafkaOffset)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// The aborted transaction holds nothing; look from outside it.
			_ = tx.Rollback(ctx)
			if handled, checkErr := r.racedRepeat(ctx, event); handled {
				return domain.ApplyResult{}, checkErr
			}
		}

		return domain.ApplyResult{}, writeError(fmt.Sprintf("insert inbox event %d", event.OutboxID), err)
	}
	if tag.RowsAffected() == 0 {
		// Seen before. The key is the outbox id alone, so a different event that
		// reuses an id (the agent's outbox recreated, its ids restarting) would pass
		// for a repeat and be lost without a trace. Compare the position it holds.
		return domain.ApplyResult{}, sameEvent(ctx, tx, event)
	}

	if message != nil {
		if err := upsertMessage(ctx, tx, *message); err != nil {
			return domain.ApplyResult{}, writeError(fmt.Sprintf("upsert message of event %d", event.OutboxID), err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.ApplyResult{}, fmt.Errorf("commit event %d: %w", event.OutboxID, err)
	}

	return domain.ApplyResult{Applied: true, Projected: message != nil}, nil
}

// rowQuerier is the part of a pool or a transaction that sameEvent needs.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// storedPosition reads the position of the inbox row that holds outboxID. found is
// false when there is none.
func storedPosition(ctx context.Context, q rowQuerier, outboxID int64) (conversationID string, sequence int, found bool, err error) {
	err = q.QueryRow(ctx, sqlSelectInboxPosition, outboxID).Scan(&conversationID, &sequence)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, fmt.Errorf("check event %d: %w", outboxID, err)
	}

	return conversationID, sequence, true, nil
}

// sameEvent confirms that the inbox row holding event's outbox id is this event.
// It returns nil for a genuine repeat and domain.ErrRejected when another event
// holds the id.
func sameEvent(ctx context.Context, q rowQuerier, event domain.InboxEvent) error {
	conversationID, sequence, found, err := storedPosition(ctx, q, event.OutboxID)
	if err != nil {
		return err
	}
	if !found {
		// The row was there a moment ago (ON CONFLICT said so) and is gone: not a
		// repeat we can vouch for, and worth another attempt.
		return fmt.Errorf("check repeated event %d: the row it conflicted with is gone", event.OutboxID)
	}
	if conversationID != event.ConversationID || sequence != event.Sequence {
		return fmt.Errorf("event %d: %w: its outbox id already belongs to another event", event.OutboxID, domain.ErrRejected)
	}

	return nil
}

// racedRepeat decides what a unique violation on the inbox insert means. Two
// writers storing the same event at the same moment can make Postgres report a
// violation of the stream key before ON CONFLICT (outbox_id) settles the race. The
// other writer's row is committed by the time the error comes back, so if the row
// holding this outbox id is this very event, it is a repeat. handled is false when
// it is not, and the caller reports the violation as it is.
func (r *Inbox) racedRepeat(ctx context.Context, event domain.InboxEvent) (handled bool, err error) {
	conversationID, sequence, found, err := storedPosition(ctx, r.db, event.OutboxID)
	switch {
	case err != nil:
		return true, err
	case !found:
		return false, nil // a genuine clash with a different outbox id
	case conversationID != event.ConversationID || sequence != event.Sequence:
		return true, fmt.Errorf("event %d: %w: its outbox id already belongs to another event", event.OutboxID, domain.ErrRejected)
	}

	return true, nil
}

// writeError wraps a failed write. Two kinds of error can never succeed on a
// retry and become domain.ErrRejected: a violated constraint (SQLSTATE class 23)
// and data the store cannot hold (class 22: a value too long, out of range, or a
// NUL in a text). Postgres puts the failing row in the error detail, and that row
// holds the message text, so only the class, the constraint and the code are kept.
// Any other error keeps its cause, so a retry can be told apart.
func writeError(what string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "23"):
			return fmt.Errorf("%s: %w: constraint %q (%s)", what, domain.ErrRejected, pgErr.ConstraintName, pgErr.Code)
		case strings.HasPrefix(pgErr.Code, "22"):
			return fmt.Errorf("%s: %w: data the store cannot hold (%s)", what, domain.ErrRejected, pgErr.Code)
		}
	}

	return fmt.Errorf("%s: %w", what, err)
}

// upsertMessage writes one chat message inside tx.
func upsertMessage(ctx context.Context, tx pgx.Tx, message domain.Message) error {
	_, err := tx.Exec(ctx, sqlUpsertMessage,
		message.ConversationID, message.Sequence, message.OutboxID, string(message.Role), message.AuthorID,
		message.Type, message.Text, message.Truncated, message.SourceEvent,
		nullable(message.IDYapeAccount), message.ReceivedAt)

	return err
}

// nullable stores an empty string as NULL, which is what an absent value means.
func nullable(value string) *string {
	if value == "" {
		return nil
	}

	return &value
}
