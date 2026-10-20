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
	//go:embed SQL/select_inbox_same.sql
	sqlSelectInboxSame string
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

// Apply implements repository.InboxRepository. The dedupe is the stream key:
// ON CONFLICT DO NOTHING plus RowsAffected tells a new event from a repeat, and
// settles two writers racing on the same event without raising an error.
func (r *Inbox) Apply(ctx context.Context, event domain.InboxEvent, message *domain.Message) (domain.ApplyResult, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return domain.ApplyResult{}, dbError("begin transaction", err)
	}
	// A rollback after a commit returns an error that means nothing here.
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, sqlInsertInbox,
		event.ConversationID, event.Sequence, event.EventName, event.EventVersion,
		event.Payload, event.Metadata,
		nullable(event.CorrelationID), nullable(event.IDYapeAccount), nullable(event.Actor), event.OccurredAt,
		event.KafkaTopic, event.KafkaPartition, event.KafkaOffset)
	if err != nil {
		return domain.ApplyResult{}, writeError(fmt.Sprintf("insert inbox event %s", event.Ref()), err)
	}
	if tag.RowsAffected() == 0 {
		// Seen before. The key is the stream position alone, so a different event at
		// a position already taken (events deleted from the tail of a stream and
		// written again) would pass for a repeat and be lost without a trace.
		return domain.ApplyResult{}, sameEvent(ctx, tx, event)
	}

	if message != nil {
		if err := upsertMessage(ctx, tx, *message); err != nil {
			return domain.ApplyResult{}, writeError(fmt.Sprintf("upsert message of event %s", event.Ref()), err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.ApplyResult{}, dbError(fmt.Sprintf("commit event %s", event.Ref()), err)
	}

	return domain.ApplyResult{Applied: true, Projected: message != nil}, nil
}

// rowQuerier is the part of a pool or a transaction that sameEvent needs.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// sameEvent confirms that the inbox row at event's position holds this event. It
// returns nil for a genuine repeat and domain.ErrRejected when the position holds
// another event. Name, version, payload, metadata, correlation id, account and actor
// are compared (an absent value equals only an absent one); JSON by value, so the
// order of keys does not matter. The time is left out on purpose: its precision
// differs between the database and the record.
func sameEvent(ctx context.Context, q rowQuerier, event domain.InboxEvent) error {
	var same bool
	err := q.QueryRow(ctx, sqlSelectInboxSame,
		event.ConversationID, event.Sequence, event.EventName, event.EventVersion, event.Payload, event.Metadata,
		nullable(event.CorrelationID), nullable(event.IDYapeAccount), nullable(event.Actor)).Scan(&same)
	if errors.Is(err, pgx.ErrNoRows) {
		// The row was there a moment ago (ON CONFLICT said so) and is gone: not a
		// repeat we can vouch for, and worth another attempt.
		return fmt.Errorf("check repeated event %s: the row it conflicted with is gone", event.Ref())
	}
	if err != nil {
		return dbError(fmt.Sprintf("check repeated event %s", event.Ref()), err)
	}
	if !same {
		return fmt.Errorf("event %s: %w: its stream position already holds another event", event.Ref(), domain.ErrRejected)
	}

	return nil
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

	return dbError(what, err)
}

// upsertMessage writes one chat message inside tx.
func upsertMessage(ctx context.Context, tx pgx.Tx, message domain.Message) error {
	_, err := tx.Exec(ctx, sqlUpsertMessage,
		message.ConversationID, message.Sequence, string(message.Role), message.AuthorID,
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
