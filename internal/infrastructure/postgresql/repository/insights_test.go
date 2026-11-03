package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
)

var insightQuery = domain.InsightQuery{IDYapeAccount: "acc-1", Flow: "p2p", Since: at.AddDate(0, 0, -30)}

// readsOnly checks that a statement only reads: the repository is a window onto the history, not a writer.
func TestInsightStatementsOnlyRead(t *testing.T) {
	for name, sql := range map[string]string{
		"list_accounts":         sqlListAccounts,
		"insight_outcome_count": sqlInsightOutcomeCounts,
		"insight_latency":       sqlInsightLatency,
		"insight_tool_counts":   sqlInsightToolCounts,
	} {
		t.Run(name, func(t *testing.T) {
			assert.NotRegexp(t, `(?i)\b(insert|update|delete|truncate|drop|alter|create)\b`, sql)
			assert.Regexp(t, `(?is)^(--[^\n]*\n)*\s*(select|with)\b`, sql)
		})
	}
}

// theStatementsScopeToTheAccount: every statement that is not the account listing takes the account
// as its first argument and finds the account's conversations in the summaries.
func TestInsightStatementsAreScopedToTheAccount(t *testing.T) {
	for name, sql := range map[string]string{
		"insight_outcome_count": sqlInsightOutcomeCounts,
		"insight_latency":       sqlInsightLatency,
		"insight_tool_counts":   sqlInsightToolCounts,
	} {
		t.Run(name, func(t *testing.T) {
			assert.Regexp(t, `conversation_id = ANY \(ARRAY\(SELECT conversation_id FROM conversation_summaries WHERE id_yape_account = \$1\)\)`, sql)
		})
	}
}

func TestListAccounts(t *testing.T) {
	dbDown := errors.New("connection reset")
	cols := []string{"id_yape_account", "conversations", "messages", "first_occurred_at", "last_occurred_at"}
	const sql = `FROM conversation_summaries\s+GROUP BY id_yape_account`

	tests := []struct {
		name        string
		expect      func(m pgxmock.PgxPoolIface)
		want        []domain.AccountActivity
		wantErr     error
		wantErrText string
	}{
		{
			name: "returnsTheAccountsInTheOrderTheStoreGaveThem",
			expect: func(m pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows(cols).
					AddRow("acc-2", 2, int64(10), at, at.Add(time.Hour)).
					AddRow("acc-1", 1, int64(4), at, at)
				m.ExpectQuery(sql).WithArgs(21, 40).WillReturnRows(rows)
			},
			want: []domain.AccountActivity{
				{IDYapeAccount: "acc-2", Conversations: 2, Messages: 10, FirstOccurredAt: at, LastOccurredAt: at.Add(time.Hour)},
				{IDYapeAccount: "acc-1", Conversations: 1, Messages: 4, FirstOccurredAt: at, LastOccurredAt: at},
			},
		},
		{
			name: "noAccountsIsAnEmptyResult",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs(21, 40).WillReturnRows(pgxmock.NewRows(cols))
			},
		},
		{
			name: "queryErrorIsWrappedAndKeepsItsCause",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs(21, 40).WillReturnError(dbDown)
			},
			wantErr:     dbDown,
			wantErrText: "list accounts",
		},
		{
			name: "aRowThatCannotBeReadIsAScanError",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs(21, 40).WillReturnRows(pgxmock.NewRows(cols).AddRow("acc-1", "many", int64(1), at, at))
			},
			wantErrText: "scan account",
		},
		{
			name: "rowErrorEndsTheRead",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs(21, 40).WillReturnRows(pgxmock.NewRows(cols).AddRow("acc-1", 1, int64(1), at, at).RowError(1, dbDown))
			},
			wantErrText: "list accounts",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			tt.expect(mock)

			got, err := NewPostgresInsightsRepository(mock).ListAccounts(context.Background(), domain.AccountQuery{Size: 21, Offset: 40})

			assertRead(t, err, tt.wantErr, tt.wantErrText)
			if tt.wantErr == nil && tt.wantErrText == "" {
				assert.Equal(t, tt.want, got)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestOutcomeCounts(t *testing.T) {
	dbDown := errors.New("connection reset")
	cols := []string{"flow", "outcome", "segments"}
	const sql = `FROM conversation_flow_segments`

	tests := []struct {
		name        string
		expect      func(m pgxmock.PgxPoolIface)
		want        []domain.OutcomeCount
		wantErr     error
		wantErrText string
	}{
		{
			name: "returnsTheCountsOfTheAccountFlowAndWindow",
			expect: func(m pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows(cols).AddRow("p2p", "approved", 3).AddRow("p2p", "rejected", 1)
				m.ExpectQuery(sql).WithArgs("acc-1", "p2p", insightQuery.Since).WillReturnRows(rows)
			},
			want: []domain.OutcomeCount{
				{Flow: "p2p", Outcome: "approved", Segments: 3},
				{Flow: "p2p", Outcome: "rejected", Segments: 1},
			},
		},
		{
			name: "noSegmentsIsAnEmptyResult",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs("acc-1", "p2p", insightQuery.Since).WillReturnRows(pgxmock.NewRows(cols))
			},
		},
		{
			name: "queryErrorIsWrappedAndKeepsItsCause",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs("acc-1", "p2p", insightQuery.Since).WillReturnError(dbDown)
			},
			wantErr:     dbDown,
			wantErrText: "count outcomes",
		},
		{
			name: "aRowThatCannotBeReadIsAScanError",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs("acc-1", "p2p", insightQuery.Since).WillReturnRows(pgxmock.NewRows(cols).AddRow("p2p", "approved", "many"))
			},
			wantErrText: "scan outcome count",
		},
		{
			name: "rowErrorEndsTheRead",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs("acc-1", "p2p", insightQuery.Since).WillReturnRows(pgxmock.NewRows(cols).AddRow("p2p", "approved", 1).RowError(1, dbDown))
			},
			wantErrText: "count outcomes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			tt.expect(mock)

			got, err := NewPostgresInsightsRepository(mock).OutcomeCounts(context.Background(), insightQuery)

			assertRead(t, err, tt.wantErr, tt.wantErrText)
			if tt.wantErr == nil && tt.wantErrText == "" {
				assert.Equal(t, tt.want, got)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestLatencyGroups(t *testing.T) {
	dbDown := errors.New("connection reset")
	cols := []string{"had_otp", "cards", "resolved", "ask_to_card_avg", "ask_to_card_median", "card_to_resolution_avg", "card_to_resolution_median"}
	const sql = `WITH seg AS`
	f := func(v float64) *float64 { return &v }

	tests := []struct {
		name        string
		expect      func(m pgxmock.PgxPoolIface)
		want        []domain.LatencyGroup
		wantErr     error
		wantErrText string
	}{
		{
			name: "readsEveryMeasureIncludingNullsAsNil",
			expect: func(m pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows(cols).
					AddRow(false, 2, 1, f(22), f(21.5), f(4), f(4)).
					AddRow(true, 1, 0, f(23), f(23), (*float64)(nil), (*float64)(nil))
				m.ExpectQuery(sql).WithArgs("acc-1", "p2p", insightQuery.Since).WillReturnRows(rows)
			},
			want: []domain.LatencyGroup{
				{
					OTP: false, Cards: 2, Resolved: 1,
					AskToCard:        domain.Stat{Average: f(22), Median: f(21.5)},
					CardToResolution: domain.Stat{Average: f(4), Median: f(4)},
				},
				{OTP: true, Cards: 1, Resolved: 0, AskToCard: domain.Stat{Average: f(23), Median: f(23)}},
			},
		},
		{
			name: "noCardsIsAnEmptyResult",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs("acc-1", "p2p", insightQuery.Since).WillReturnRows(pgxmock.NewRows(cols))
			},
		},
		{
			name: "queryErrorIsWrappedAndKeepsItsCause",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs("acc-1", "p2p", insightQuery.Since).WillReturnError(dbDown)
			},
			wantErr:     dbDown,
			wantErrText: "measure latency",
		},
		{
			name: "aRowThatCannotBeReadIsAScanError",
			expect: func(m pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows(cols).AddRow("maybe", 1, 1, f(1), f(1), f(1), f(1))
				m.ExpectQuery(sql).WithArgs("acc-1", "p2p", insightQuery.Since).WillReturnRows(rows)
			},
			wantErrText: "scan latency",
		},
		{
			name: "rowErrorEndsTheRead",
			expect: func(m pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows(cols).AddRow(false, 1, 1, f(1), f(1), f(1), f(1)).RowError(1, dbDown)
				m.ExpectQuery(sql).WithArgs("acc-1", "p2p", insightQuery.Since).WillReturnRows(rows)
			},
			wantErrText: "measure latency",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			tt.expect(mock)

			got, err := NewPostgresInsightsRepository(mock).LatencyGroups(context.Background(), insightQuery)

			assertRead(t, err, tt.wantErr, tt.wantErrText)
			if tt.wantErr == nil && tt.wantErrText == "" {
				assert.Equal(t, tt.want, got)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestToolCounts(t *testing.T) {
	dbDown := errors.New("connection reset")
	cols := []string{"tool", "outcome", "calls"}
	const sql = `event_name = 'ToolActionSummarized'`

	tests := []struct {
		name        string
		expect      func(m pgxmock.PgxPoolIface)
		want        []domain.ToolCount
		wantErr     error
		wantErrText string
	}{
		{
			name: "returnsTheCallsOfTheAccountSinceTheWindowOpened",
			expect: func(m pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows(cols).AddRow("draft_transfer", "ok", 6).AddRow("validate_p2p_amount", "failed", 1)
				// The flow is not an argument: the health of a tool does not depend on it.
				m.ExpectQuery(sql).WithArgs("acc-1", insightQuery.Since).WillReturnRows(rows)
			},
			want: []domain.ToolCount{
				{Tool: "draft_transfer", Outcome: "ok", Calls: 6},
				{Tool: "validate_p2p_amount", Outcome: "failed", Calls: 1},
			},
		},
		{
			name: "noCallsIsAnEmptyResult",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs("acc-1", insightQuery.Since).WillReturnRows(pgxmock.NewRows(cols))
			},
		},
		{
			name: "queryErrorIsWrappedAndKeepsItsCause",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs("acc-1", insightQuery.Since).WillReturnError(dbDown)
			},
			wantErr:     dbDown,
			wantErrText: "count tool calls",
		},
		{
			name: "aRowThatCannotBeReadIsAScanError",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs("acc-1", insightQuery.Since).WillReturnRows(pgxmock.NewRows(cols).AddRow("t", "ok", "many"))
			},
			wantErrText: "scan tool count",
		},
		{
			name: "rowErrorEndsTheRead",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(sql).WithArgs("acc-1", insightQuery.Since).WillReturnRows(pgxmock.NewRows(cols).AddRow("t", "ok", 1).RowError(1, dbDown))
			},
			wantErrText: "count tool calls",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			tt.expect(mock)

			got, err := NewPostgresInsightsRepository(mock).ToolCounts(context.Background(), insightQuery)

			assertRead(t, err, tt.wantErr, tt.wantErrText)
			if tt.wantErr == nil && tt.wantErrText == "" {
				assert.Equal(t, tt.want, got)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// assertRead checks the error side of a read: no error, or an error with the text and cause given.
func assertRead(t *testing.T, err, wantErr error, wantErrText string) {
	t.Helper()

	if wantErr == nil && wantErrText == "" {
		require.NoError(t, err)

		return
	}
	require.Error(t, err)
	if wantErr != nil {
		assert.ErrorIs(t, err, wantErr)
	}
	if wantErrText != "" {
		assert.Contains(t, err.Error(), wantErrText)
	}
}
