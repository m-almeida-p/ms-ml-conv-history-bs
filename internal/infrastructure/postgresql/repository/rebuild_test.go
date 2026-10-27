package repository

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
)

var inboxColumns = []string{
	"conversation_id", "sequence", "event_name", "event_version", "payload", "metadata",
	"correlation_id", "id_yape_account", "actor", "flow", "occurred_at", "kafka_topic", "kafka_partition", "kafka_offset",
}

// anyArgs matches n arguments of any value.
func anyArgs(n int) []any {
	args := make([]any, n)
	for i := range args {
		args[i] = pgxmock.AnyArg()
	}

	return args
}

func TestRebuild(t *testing.T) {
	at := time.Date(2026, 10, 20, 14, 0, 0, 0, time.UTC)
	account, actor, correlation, flow := "acc-1", "user", "c-1", "p2p"

	row := func(rows *pgxmock.Rows, id int64, seq int, name string) *pgxmock.Rows {
		return rows.AddRow("conv", seq, name, 1, json.RawMessage(`{}`), json.RawMessage(`{}`), &correlation, &account, &actor, &flow, at, "t", int32(0), id)
	}
	// project keeps user messages, skips tool summaries and flags anything else.
	project := func(e domain.InboxEvent) (*domain.Message, error) {
		switch e.EventName {
		case "UserMessageSent":
			return &domain.Message{ConversationID: e.ConversationID, Sequence: e.Sequence, Role: domain.RoleUser, AuthorID: e.IDYapeAccount, Type: "text", Text: "t", EventName: e.EventName, IDYapeAccount: e.IDYapeAccount, OccurredAt: e.OccurredAt}, nil
		case "ToolActionSummarized":
			return nil, nil
		default:
			return nil, domain.ErrInvalidEvent
		}
	}
	dbDown := errors.New("connection reset")

	tests := []struct {
		name      string
		batchSize int
		project   func(domain.InboxEvent) (*domain.Message, error)
		expect    func(m pgxmock.PgxPoolIface)
		want      domain.RebuildResult
		wantErr   string
		wantIs    error
	}{
		{
			name:      "emptyInboxLeavesNoMessages",
			batchSize: 2,
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectExec("DELETE FROM conversation_messages").WillReturnResult(pgxmock.NewResult("DELETE", 5))
				m.ExpectExec("DELETE FROM conversation_summaries").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectQuery("FROM conversation_inbox").WithArgs("", -1, 2).WillReturnRows(pgxmock.NewRows(inboxColumns))
				m.ExpectCommit()
			},
		},
		{
			name:      "messagesSkippedAndInvalidAreCounted",
			batchSize: 5,
			expect: func(m pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows(inboxColumns)
				row(rows, 1, 1, "UserMessageSent")
				row(rows, 2, 2, "ToolActionSummarized")
				row(rows, 3, 3, "Mystery")
				m.ExpectBegin()
				m.ExpectExec("DELETE FROM conversation_messages").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectExec("DELETE FROM conversation_summaries").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectQuery("FROM conversation_inbox").WithArgs("", -1, 5).WillReturnRows(rows)
				m.ExpectExec("INSERT INTO conversation_messages").WithArgs(anyArgs(11)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectExec("INSERT INTO conversation_summaries").WithArgs(anyArgs(3)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectCommit()
			},
			want: domain.RebuildResult{Events: 3, Messages: 1, Invalid: 1},
		},
		{
			name:      "fullBatchAsksForTheNextOneFromTheLastKey",
			batchSize: 2,
			expect: func(m pgxmock.PgxPoolIface) {
				first := pgxmock.NewRows(inboxColumns)
				row(first, 1, 1, "UserMessageSent")
				row(first, 2, 2, "UserMessageSent")
				second := pgxmock.NewRows(inboxColumns)
				row(second, 3, 3, "UserMessageSent")
				m.ExpectBegin()
				m.ExpectExec("DELETE FROM conversation_messages").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectExec("DELETE FROM conversation_summaries").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectQuery("FROM conversation_inbox").WithArgs("", -1, 2).WillReturnRows(first)
				m.ExpectExec("INSERT INTO conversation_messages").WithArgs(anyArgs(11)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectExec("INSERT INTO conversation_summaries").WithArgs(anyArgs(3)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectExec("INSERT INTO conversation_messages").WithArgs(anyArgs(11)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectExec("INSERT INTO conversation_summaries").WithArgs(anyArgs(3)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectQuery("FROM conversation_inbox").WithArgs("conv", 2, 2).WillReturnRows(second)
				m.ExpectExec("INSERT INTO conversation_messages").WithArgs(anyArgs(11)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectExec("INSERT INTO conversation_summaries").WithArgs(anyArgs(3)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
				m.ExpectCommit()
			},
			want: domain.RebuildResult{Events: 3, Messages: 3},
		},
		{
			name:      "nullCopiesReadAsEmpty",
			batchSize: 5,
			expect: func(m pgxmock.PgxPoolIface) {
				var none *string
				rows := pgxmock.NewRows(inboxColumns).
					AddRow("conv", 1, "ToolActionSummarized", 1, json.RawMessage(`{}`), json.RawMessage(`{}`), none, none, none, none, at, "t", int32(0), int64(1))
				m.ExpectBegin()
				m.ExpectExec("DELETE FROM conversation_messages").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectExec("DELETE FROM conversation_summaries").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectQuery("FROM conversation_inbox").WithArgs(anyArgs(3)...).WillReturnRows(rows)
				m.ExpectCommit()
			},
			project: func(e domain.InboxEvent) (*domain.Message, error) {
				if e.CorrelationID != "" || e.IDYapeAccount != "" || e.Actor != "" {
					return nil, errors.New("null columns must read as empty strings")
				}
				return nil, nil
			},
			want: domain.RebuildResult{Events: 1},
		},
		{
			name:      "beginFails",
			batchSize: 2,
			expect:    func(m pgxmock.PgxPoolIface) { m.ExpectBegin().WillReturnError(dbDown) },
			wantErr:   "begin transaction", wantIs: dbDown,
		},
		{
			name:      "emptyingFails",
			batchSize: 2,
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectExec("DELETE FROM conversation_messages").WillReturnError(dbDown)
				m.ExpectRollback()
			},
			wantErr: "empty messages", wantIs: dbDown,
		},
		{
			name:      "readFails",
			batchSize: 2,
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectExec("DELETE FROM conversation_messages").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectExec("DELETE FROM conversation_summaries").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectQuery("FROM conversation_inbox").WithArgs(anyArgs(3)...).WillReturnError(dbDown)
				m.ExpectRollback()
			},
			wantErr: "read inbox batch", wantIs: dbDown,
		},
		{
			name:      "scanFails",
			batchSize: 2,
			expect: func(m pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows(inboxColumns).
					AddRow("conv", "not a number", "E", 1, json.RawMessage(`{}`), json.RawMessage(`{}`), nil, nil, nil, nil, at, "t", int32(0), int64(1))
				m.ExpectBegin()
				m.ExpectExec("DELETE FROM conversation_messages").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectExec("DELETE FROM conversation_summaries").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectQuery("FROM conversation_inbox").WithArgs(anyArgs(3)...).WillReturnRows(rows)
				m.ExpectRollback()
			},
			wantErr: "scan inbox event",
		},
		{
			name:      "rowErrorStopsTheRead",
			batchSize: 2,
			expect: func(m pgxmock.PgxPoolIface) {
				rows := row(pgxmock.NewRows(inboxColumns), 1, 1, "UserMessageSent").RowError(1, dbDown)
				m.ExpectBegin()
				m.ExpectExec("DELETE FROM conversation_messages").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectExec("DELETE FROM conversation_summaries").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectQuery("FROM conversation_inbox").WithArgs(anyArgs(3)...).WillReturnRows(rows)
				m.ExpectRollback()
			},
			wantErr: "read inbox batch", wantIs: dbDown,
		},
		{
			name:      "emptyingTheSummariesFailsAndRollsBack",
			batchSize: 2,
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectExec("DELETE FROM conversation_messages").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectExec("DELETE FROM conversation_summaries").WillReturnError(dbDown)
				m.ExpectRollback()
			},
			wantErr: "empty conversation summaries",
		},
		{
			name:      "projectionFailureRollsBack",
			batchSize: 2,
			project:   func(domain.InboxEvent) (*domain.Message, error) { return nil, dbDown },
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectExec("DELETE FROM conversation_messages").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectExec("DELETE FROM conversation_summaries").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectQuery("FROM conversation_inbox").WithArgs(anyArgs(3)...).WillReturnRows(row(pgxmock.NewRows(inboxColumns), 1, 1, "UserMessageSent"))
				m.ExpectRollback()
			},
			wantErr: "project event t/0/1", wantIs: dbDown,
		},
		{
			name:      "upsertFailureRollsBack",
			batchSize: 2,
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectExec("DELETE FROM conversation_messages").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectExec("DELETE FROM conversation_summaries").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectQuery("FROM conversation_inbox").WithArgs(anyArgs(3)...).WillReturnRows(row(pgxmock.NewRows(inboxColumns), 1, 1, "UserMessageSent"))
				m.ExpectExec("INSERT INTO conversation_messages").WithArgs(anyArgs(11)...).WillReturnError(dbDown)
				m.ExpectRollback()
			},
			wantErr: "upsert message of event t/0/1", wantIs: dbDown,
		},
		{
			name:      "commitFails",
			batchSize: 2,
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectBegin()
				m.ExpectExec("DELETE FROM conversation_messages").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectExec("DELETE FROM conversation_summaries").WillReturnResult(pgxmock.NewResult("DELETE", 0))
				m.ExpectQuery("FROM conversation_inbox").WithArgs(anyArgs(3)...).WillReturnRows(pgxmock.NewRows(inboxColumns))
				m.ExpectCommit().WillReturnError(dbDown)
				m.ExpectRollback()
			},
			wantErr: "commit rebuild", wantIs: dbDown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			tt.expect(mock)

			repo := NewPostgresInboxRepository(mock)
			repo.batchSize = tt.batchSize
			ctx := context.Background()
			fn := project
			if tt.project != nil {
				fn = tt.project
			}

			got, err := repo.Rebuild(ctx, fn)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				if tt.wantIs != nil {
					assert.ErrorIs(t, err, tt.wantIs)
				}
				assert.Equal(t, domain.RebuildResult{}, got)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRebuildStopsWhenTheContextIsCancelled(t *testing.T) {
	at := time.Date(2026, 10, 20, 14, 0, 0, 0, time.UTC)
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows(inboxColumns).
		AddRow("conv", 1, "ToolActionSummarized", 1, json.RawMessage(`{}`), json.RawMessage(`{}`), nil, nil, nil, nil, at, "t", int32(0), int64(1)).
		AddRow("conv", 2, "UserMessageSent", 1, json.RawMessage(`{}`), json.RawMessage(`{}`), nil, nil, nil, nil, at, "t", int32(0), int64(2))
	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM conversation_messages").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectExec("DELETE FROM conversation_summaries").WillReturnResult(pgxmock.NewResult("DELETE", 0))
	mock.ExpectQuery("FROM conversation_inbox").WithArgs(anyArgs(3)...).WillReturnRows(rows)
	mock.ExpectRollback()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := 0
	project := func(domain.InboxEvent) (*domain.Message, error) {
		seen++
		cancel() // the first event is processed, then the caller gives up
		return nil, nil
	}

	_, err = NewPostgresInboxRepository(mock).Rebuild(ctx, project)

	require.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "replay stopped at event t/0/2")
	assert.Equal(t, 1, seen, "no event is projected after the cancel")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestValue(t *testing.T) {
	text := "x"
	tests := []struct {
		name string
		in   *string
		want string
	}{
		{name: "nullIsEmpty", in: nil, want: ""},
		{name: "textIsKept", in: &text, want: "x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, value(tt.in))
		})
	}
}
