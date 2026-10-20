package repository

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository"
)

var _ repository.InboxRepository = (*Inbox)(nil)

func TestApply(t *testing.T) {
	occurredAt := time.Date(2026, 10, 20, 14, 13, 41, 0, time.UTC)
	account, actor, correlation := "acc-1", "user", "c-1"

	event := domain.InboxEvent{
		OutboxID: 1042, ConversationID: "conv_v1_a", Sequence: 2, EventName: "UserMessageSent", EventVersion: 1,
		Payload: json.RawMessage(`{"text": "hola"}`), Metadata: json.RawMessage(`{"actor": "user"}`),
		CorrelationID: "c-1", IDYapeAccount: "acc-1", Actor: "user", OccurredAt: occurredAt,
		KafkaTopic: "yape.public.conversation_outbox", KafkaPartition: 1, KafkaOffset: 7,
	}
	message := &domain.Message{
		ConversationID: "conv_v1_a", Sequence: 2, OutboxID: 1042, Role: domain.RoleUser, AuthorID: "acc-1",
		Type: "text", Text: "hola", SourceEvent: "UserMessageSent", IDYapeAccount: "acc-1", ReceivedAt: occurredAt,
	}

	insertArgs := []any{
		int64(1042), "conv_v1_a", 2, "UserMessageSent", 1, json.RawMessage(`{"text": "hola"}`), json.RawMessage(`{"actor": "user"}`),
		&correlation, &account, &actor, occurredAt, "yape.public.conversation_outbox", int32(1), int64(7),
	}
	upsertArgs := []any{"conv_v1_a", 2, int64(1042), "appUser", "acc-1", "text", "hola", false, "UserMessageSent", &account, occurredAt}

	dbDown := errors.New("connection reset")

	tests := []struct {
		name    string
		event   domain.InboxEvent
		message *domain.Message
		expect  func(mock pgxmock.PgxPoolIface)
		want    domain.ApplyResult
		wantErr string
	}{
		{
			name:    "newEventWithMessage",
			event:   event,
			message: message,
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectExec("INSERT INTO conversation_inbox").WithArgs(insertArgs...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectExec("INSERT INTO conversation_messages").WithArgs(upsertArgs...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectCommit()
			},
			want: domain.ApplyResult{Applied: true, Projected: true},
		},
		{
			name:  "skippedEventStillLandsInTheInbox",
			event: event,
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectExec("INSERT INTO conversation_inbox").WithArgs(insertArgs...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectCommit()
			},
			want: domain.ApplyResult{Applied: true},
		},
		{
			name: "absentMetadataCopiesAreStoredAsNull",
			event: func() domain.InboxEvent {
				e := event
				e.CorrelationID, e.IDYapeAccount, e.Actor = "", "", ""
				return e
			}(),
			expect: func(m pgxmock.PgxPoolIface) {
				var none *string
				m.ExpectBegin()
				m.ExpectExec("INSERT INTO conversation_inbox").
					WithArgs(int64(1042), "conv_v1_a", 2, "UserMessageSent", 1, json.RawMessage(`{"text": "hola"}`), json.RawMessage(`{"actor": "user"}`),
						none, none, none, occurredAt, "yape.public.conversation_outbox", int32(1), int64(7)).
					WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectCommit()
			},
			want: domain.ApplyResult{Applied: true},
		},
		{
			name:    "repeatedEventWritesNothingElse",
			event:   event,
			message: message,
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectExec("INSERT INTO conversation_inbox").WithArgs(insertArgs...).WillReturnResult(pgxmock.NewResult("INSERT", 0))
				m.ExpectQuery("FROM conversation_inbox").WithArgs(int64(1042)).WillReturnRows(pgxmock.NewRows([]string{"conversation_id", "sequence"}).AddRow("conv_v1_a", 2))
				m.ExpectRollback()
			},
			want: domain.ApplyResult{},
		},
		{
			name:    "beginFails",
			event:   event,
			message: message,
			expect:  func(m pgxmock.PgxPoolIface) { m.ExpectBegin().WillReturnError(dbDown) },
			wantErr: "begin transaction",
		},
		{
			name:    "inboxInsertFailsAndRollsBack",
			event:   event,
			message: message,
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectExec("INSERT INTO conversation_inbox").WithArgs(insertArgs...).WillReturnError(dbDown)
				m.ExpectRollback()
			},
			wantErr: "insert inbox event 1042",
		},
		{
			name:    "messageUpsertFailsAndRollsBack",
			event:   event,
			message: message,
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectExec("INSERT INTO conversation_inbox").WithArgs(insertArgs...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectExec("INSERT INTO conversation_messages").WithArgs(upsertArgs...).WillReturnError(dbDown)
				m.ExpectRollback()
			},
			wantErr: "upsert message of event 1042",
		},
		{
			name:    "commitFails",
			event:   event,
			message: message,
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectExec("INSERT INTO conversation_inbox").WithArgs(insertArgs...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectExec("INSERT INTO conversation_messages").WithArgs(upsertArgs...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectCommit().WillReturnError(dbDown)
				m.ExpectRollback()
			},
			wantErr: "commit event 1042",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			tt.expect(mock)

			got, err := NewInbox(mock).Apply(context.Background(), tt.event, tt.message)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.ErrorIs(t, err, dbDown, "the root cause is kept")
				assert.Equal(t, domain.ApplyResult{}, got)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestApplyErrorNeverCarriesTheEventContent(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO conversation_inbox").WillReturnError(errors.New("boom"))
	mock.ExpectRollback()

	event := domain.InboxEvent{OutboxID: 9, Payload: json.RawMessage(`{"text": "card 4111"}`)}
	_, err = NewInbox(mock).Apply(context.Background(), event, nil)

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "4111")
}

func TestWithBatchSize(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want int
	}{
		{name: "defaultWithoutOptions", want: defaultBatchSize},
		{name: "positiveValueIsUsed", opts: []Option{WithBatchSize(2)}, want: 2},
		{name: "zeroKeepsTheDefault", opts: []Option{WithBatchSize(0)}, want: defaultBatchSize},
		{name: "negativeKeepsTheDefault", opts: []Option{WithBatchSize(-3)}, want: defaultBatchSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, NewInbox(nil, tt.opts...).batchSize)
		})
	}
}

func TestApplyWriteErrors(t *testing.T) {
	const secret = "card 4111 1111"

	event := domain.InboxEvent{OutboxID: 9, Payload: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`)}
	message := &domain.Message{Role: domain.RoleUser, Text: secret}

	failingRow := func(code, constraint string) error {
		return &pgconn.PgError{
			Code: code, ConstraintName: constraint,
			Message: "new row violates a constraint", Detail: "Failing row contains (" + secret + ")",
		}
	}

	tests := []struct {
		name         string
		failOn       string // "inbox" or "message"
		err          error
		wantRejected bool
		wantContains string
		recheck      bool // a unique violation on the inbox is looked at again from outside the transaction
	}{
		{name: "uniqueViolationOnTheInbox", failOn: "inbox", err: failingRow("23505", "conversation_inbox_stream_key"), wantRejected: true, wantContains: `constraint "conversation_inbox_stream_key" (23505)`, recheck: true},
		{name: "checkViolationOnTheMessage", failOn: "message", err: failingRow("23514", "conversation_messages_role_check"), wantRejected: true, wantContains: `constraint "conversation_messages_role_check" (23514)`},
		{name: "notNullViolationOnTheMessage", failOn: "message", err: failingRow("23502", ""), wantRejected: true, wantContains: "(23502)"},
		{name: "valueTooLongOnTheInbox", failOn: "inbox", err: failingRow("22001", ""), wantRejected: true, wantContains: "data the store cannot hold (22001)"},
		{name: "unsupportedUnicodeEscapeOnTheInbox", failOn: "inbox", err: failingRow("22P05", ""), wantRejected: true, wantContains: "data the store cannot hold (22P05)"},
		{name: "invalidByteSequenceOnTheMessage", failOn: "message", err: failingRow("22021", ""), wantRejected: true, wantContains: "data the store cannot hold (22021)"},
		{name: "numberOutOfRangeOnTheMessage", failOn: "message", err: failingRow("22003", ""), wantRejected: true, wantContains: "data the store cannot hold (22003)"},
		{name: "serializationFailureIsWorthRetrying", failOn: "inbox", err: &pgconn.PgError{Code: "40001", Message: "could not serialize"}, wantContains: "could not serialize"},
		{name: "connectionErrorIsWorthRetrying", failOn: "message", err: errors.New("connection reset"), wantContains: "connection reset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()

			mock.ExpectBegin()
			if tt.failOn == "inbox" {
				mock.ExpectExec("INSERT INTO conversation_inbox").WithArgs(anyArgs(14)...).WillReturnError(tt.err)
			} else {
				mock.ExpectExec("INSERT INTO conversation_inbox").WithArgs(anyArgs(14)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				mock.ExpectExec("INSERT INTO conversation_messages").WithArgs(anyArgs(11)...).WillReturnError(tt.err)
			}
			mock.ExpectRollback()
			if tt.recheck {
				mock.ExpectQuery("FROM conversation_inbox").WithArgs(int64(9)).WillReturnRows(pgxmock.NewRows([]string{"conversation_id", "sequence"}))
			}

			_, err = NewInbox(mock).Apply(context.Background(), event, message)

			require.Error(t, err)
			assert.Equal(t, tt.wantRejected, errors.Is(err, domain.ErrRejected))
			assert.Contains(t, err.Error(), tt.wantContains)
			assert.NotContains(t, err.Error(), secret, "the failing row never reaches the error")
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestApplyOfARepeatedIDChecksItIsTheSameEvent(t *testing.T) {
	dbDown := errors.New("connection reset")
	event := domain.InboxEvent{OutboxID: 7, ConversationID: "conv", Sequence: 3, Payload: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`)}
	positionRows := func(conv string, seq int) *pgxmock.Rows {
		return pgxmock.NewRows([]string{"conversation_id", "sequence"}).AddRow(conv, seq)
	}

	tests := []struct {
		name         string
		rows         func() *pgxmock.Rows
		queryErr     error
		wantRejected bool
		wantErr      string
	}{
		{name: "sameEventIsAnOrdinaryRepeat", rows: func() *pgxmock.Rows { return positionRows("conv", 3) }},
		{name: "anotherConversationUsesTheID", rows: func() *pgxmock.Rows { return positionRows("other", 3) }, wantRejected: true, wantErr: "already belongs to another event"},
		{name: "anotherPositionUsesTheID", rows: func() *pgxmock.Rows { return positionRows("conv", 4) }, wantRejected: true, wantErr: "already belongs to another event"},
		{name: "theCheckFails", queryErr: dbDown, wantErr: "check event 7"},
		{name: "theRowIsGoneBeforeTheCheck", rows: func() *pgxmock.Rows { return pgxmock.NewRows([]string{"conversation_id", "sequence"}) }, wantErr: "check repeated event 7"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO conversation_inbox").WithArgs(anyArgs(14)...).WillReturnResult(pgxmock.NewResult("INSERT", 0))
			query := mock.ExpectQuery("FROM conversation_inbox").WithArgs(int64(7))
			if tt.queryErr != nil {
				query.WillReturnError(tt.queryErr)
			} else {
				query.WillReturnRows(tt.rows())
			}
			mock.ExpectRollback()

			result, err := NewInbox(mock).Apply(context.Background(), event, nil)

			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Equal(t, tt.wantRejected, errors.Is(err, domain.ErrRejected))
			}
			assert.Equal(t, domain.ApplyResult{}, result)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// Two writers storing the same event at once can make Postgres report the stream
// key before ON CONFLICT (outbox_id) settles the race. The other writer's row is
// committed by then, so Apply looks from outside the aborted transaction.
func TestApplyResolvesAUniqueViolationRacedByAnotherWriter(t *testing.T) {
	dbDown := errors.New("connection reset")
	event := domain.InboxEvent{OutboxID: 7, ConversationID: "conv", Sequence: 3, Payload: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`)}
	violation := &pgconn.PgError{Code: "23505", ConstraintName: "conversation_inbox_stream_key", Detail: "Key (conversation_id, sequence)=(conv, 3) already exists."}
	position := func(conv string, seq int) *pgxmock.Rows {
		return pgxmock.NewRows([]string{"conversation_id", "sequence"}).AddRow(conv, seq)
	}
	none := func() *pgxmock.Rows { return pgxmock.NewRows([]string{"conversation_id", "sequence"}) }

	tests := []struct {
		name         string
		lookup       func(m pgxmock.PgxPoolIface)
		wantRejected bool
		wantErr      string
	}{
		{
			name: "theSameEventWasStoredByTheOtherWriter",
			lookup: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_inbox").WithArgs(int64(7)).WillReturnRows(position("conv", 3))
			},
		},
		{
			name: "anotherEventHoldsTheOutboxID",
			lookup: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_inbox").WithArgs(int64(7)).WillReturnRows(position("conv", 9))
			},
			wantRejected: true, wantErr: "already belongs to another event",
		},
		{
			name: "noRowHoldsTheOutboxIDSoItIsAGenuineStreamClash",
			lookup: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_inbox").WithArgs(int64(7)).WillReturnRows(none())
			},
			wantRejected: true, wantErr: `constraint "conversation_inbox_stream_key" (23505)`,
		},
		{
			name: "theLookupFailsSoItIsWorthARetry",
			lookup: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_inbox").WithArgs(int64(7)).WillReturnError(dbDown)
			},
			wantErr: "check event 7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO conversation_inbox").WithArgs(anyArgs(14)...).WillReturnError(violation)
			mock.ExpectRollback()
			tt.lookup(mock)

			result, err := NewInbox(mock).Apply(context.Background(), event, nil)

			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Equal(t, tt.wantRejected, errors.Is(err, domain.ErrRejected))
				assert.NotContains(t, err.Error(), "Key (conversation_id", "the row detail never reaches the error")
			}
			assert.Equal(t, domain.ApplyResult{}, result, "a repeat writes nothing")
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
