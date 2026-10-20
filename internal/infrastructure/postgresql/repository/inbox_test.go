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
		ConversationID: "conv_v1_a", Sequence: 2, EventName: "UserMessageSent", EventVersion: 1,
		Payload: json.RawMessage(`{"text": "hola"}`), Metadata: json.RawMessage(`{"actor": "user"}`),
		CorrelationID: "c-1", IDYapeAccount: "acc-1", Actor: "user", OccurredAt: occurredAt,
		KafkaTopic: "yape.public.conversation_events", KafkaPartition: 1, KafkaOffset: 7,
	}
	message := &domain.Message{
		ConversationID: "conv_v1_a", Sequence: 2, Role: domain.RoleUser, AuthorID: "acc-1",
		Type: "text", Text: "hola", SourceEvent: "UserMessageSent", IDYapeAccount: "acc-1", ReceivedAt: occurredAt,
	}

	insertArgs := []any{
		"conv_v1_a", 2, "UserMessageSent", 1, json.RawMessage(`{"text": "hola"}`), json.RawMessage(`{"actor": "user"}`),
		&correlation, &account, &actor, occurredAt, "yape.public.conversation_events", int32(1), int64(7),
	}
	upsertArgs := []any{"conv_v1_a", 2, "appUser", "acc-1", "text", "hola", false, "UserMessageSent", &account, occurredAt}

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
					WithArgs("conv_v1_a", 2, "UserMessageSent", 1, json.RawMessage(`{"text": "hola"}`), json.RawMessage(`{"actor": "user"}`),
						none, none, none, occurredAt, "yape.public.conversation_events", int32(1), int64(7)).
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
				m.ExpectQuery("FROM conversation_inbox").WithArgs("conv_v1_a", 2, "UserMessageSent", 1, json.RawMessage(`{"text": "hola"}`), json.RawMessage(`{"actor": "user"}`), &correlation, &account, &actor).WillReturnRows(pgxmock.NewRows([]string{"same"}).AddRow(true))
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
			wantErr: "insert inbox event yape.public.conversation_events/1/7",
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
			wantErr: "upsert message of event yape.public.conversation_events/1/7",
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
			wantErr: "commit event yape.public.conversation_events/1/7",
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

	event := domain.InboxEvent{ConversationID: "conv", Sequence: 9, Payload: json.RawMessage(`{"text": "card 4111"}`)}
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

	event := domain.InboxEvent{ConversationID: "conv", Sequence: 9, Payload: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`)}
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
	}{
		{name: "uniqueViolationOnTheInbox", failOn: "inbox", err: failingRow("23505", "conversation_inbox_stream_key"), wantRejected: true, wantContains: `constraint "conversation_inbox_stream_key" (23505)`},
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
				mock.ExpectExec("INSERT INTO conversation_inbox").WithArgs(anyArgs(13)...).WillReturnError(tt.err)
			} else {
				mock.ExpectExec("INSERT INTO conversation_inbox").WithArgs(anyArgs(13)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				mock.ExpectExec("INSERT INTO conversation_messages").WithArgs(anyArgs(10)...).WillReturnError(tt.err)
			}
			mock.ExpectRollback()

			_, err = NewInbox(mock).Apply(context.Background(), event, message)

			require.Error(t, err)
			assert.Equal(t, tt.wantRejected, errors.Is(err, domain.ErrRejected))
			assert.Contains(t, err.Error(), tt.wantContains)
			assert.NotContains(t, err.Error(), secret, "the failing row never reaches the error")
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestApplyOfARepeatedEventChecksItIsTheSameEvent(t *testing.T) {
	dbDown := errors.New("connection reset")
	event := domain.InboxEvent{
		ConversationID: "conv", Sequence: 3, EventName: "UserMessageSent", EventVersion: 1, Payload: json.RawMessage(`{}`), Metadata: json.RawMessage(`{}`),
		CorrelationID: "c-1", IDYapeAccount: "acc-1", Actor: "user", KafkaTopic: "t", KafkaPartition: 2, KafkaOffset: 9,
	}
	correlation, account, actor := "c-1", "acc-1", "user"
	sameRows := func(same bool) func() *pgxmock.Rows {
		return func() *pgxmock.Rows { return pgxmock.NewRows([]string{"same"}).AddRow(same) }
	}

	tests := []struct {
		name         string
		rows         func() *pgxmock.Rows
		queryErr     error
		wantRejected bool
		wantErr      string
	}{
		{name: "sameEventIsAnOrdinaryRepeat", rows: sameRows(true)},
		{name: "anotherEventAtThePositionIsRefused", rows: sameRows(false), wantRejected: true, wantErr: "stream position already holds another event"},
		{name: "theCheckFails", queryErr: dbDown, wantErr: "check repeated event t/2/9"},
		{name: "theRowIsGoneBeforeTheCheck", rows: func() *pgxmock.Rows { return pgxmock.NewRows([]string{"same"}) }, wantErr: "the row it conflicted with is gone"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO conversation_inbox").WithArgs(anyArgs(13)...).WillReturnResult(pgxmock.NewResult("INSERT", 0))
			query := mock.ExpectQuery("FROM conversation_inbox").WithArgs("conv", 3, "UserMessageSent", 1, json.RawMessage(`{}`), json.RawMessage(`{}`), &correlation, &account, &actor)
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
