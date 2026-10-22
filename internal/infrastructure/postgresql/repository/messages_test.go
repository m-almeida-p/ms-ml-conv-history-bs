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

var messageColumns = []string{
	"conversation_id", "sequence", "role", "author_id", "type", "text", "truncated", "event_name", "id_yape_account", "flow", "occurred_at",
}

var at = time.Date(2026, 10, 20, 14, 0, 0, 0, time.UTC)

// msgRows builds the rows a query returns, in the order given.
func msgRows(sequences ...int) *pgxmock.Rows {
	rows := pgxmock.NewRows(messageColumns)
	account, flow := "acc-1", "p2p"
	for _, seq := range sequences {
		rows.AddRow("conv", seq, "appUser", "acc-1", "text", "m", false, "UserMessageSent", &account, &flow, at.Add(time.Duration(seq)*time.Second))
	}

	return rows
}

func seqs(from, to, step int) []int {
	var out []int
	for s := from; (step > 0 && s <= to) || (step < 0 && s >= to); s += step {
		out = append(out, s)
	}

	return out
}

func sequencesOf(messages []domain.Message) []int {
	out := make([]int, 0, len(messages))
	for _, m := range messages {
		out = append(out, m.Sequence)
	}

	return out
}

func TestListMessages(t *testing.T) {
	dbDown := errors.New("connection reset")
	cursorRows := func(seq int) *pgxmock.Rows { return pgxmock.NewRows([]string{"sequence"}).AddRow(seq) }
	noRows := func() *pgxmock.Rows { return pgxmock.NewRows([]string{"sequence"}) }
	existsRows := func(v bool) *pgxmock.Rows { return pgxmock.NewRows([]string{"exists"}).AddRow(v) }

	tests := []struct {
		name         string
		query        domain.PageQuery
		expect       func(m pgxmock.PgxPoolIface)
		wantSeqs     []int
		wantHasMore  bool
		wantEmptyOK  bool
		wantErr      error
		wantErrText  string
		wantRootCaus error
	}{
		{
			name:  "newestPageIsReturnedOldestFirst",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnRows(msgRows(9, 8, 7))
			},
			wantSeqs: []int{7, 8, 9},
		},
		{
			name:  "newestPageWithAnExtraRowHasMoreAndDropsTheOldestOne",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnRows(msgRows(seqs(101, 1, -1)...))
			},
			wantSeqs:    seqs(2, 101, 1),
			wantHasMore: true,
		},
		{
			name:  "exactlyOnePageHasNoMore",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnRows(msgRows(seqs(100, 1, -1)...))
			},
			wantSeqs: seqs(1, 100, 1),
		},
		{
			name:  "newestOfUnknownConversation",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnRows(msgRows())
			},
			wantErr: domain.ErrNotFound,
		},
		{
			name:  "beforeACursorReadsOlderMessagesOldestFirst",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 10},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 10).WillReturnRows(cursorRows(10))
				m.ExpectQuery("sequence < ").WithArgs("conv", 10, 101).WillReturnRows(msgRows(9, 8, 7))
			},
			wantSeqs: []int{7, 8, 9},
		},
		{
			name:  "beforeWithMoreOlderMessages",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 500},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 500).WillReturnRows(cursorRows(500))
				m.ExpectQuery("sequence < ").WithArgs("conv", 500, 101).WillReturnRows(msgRows(seqs(499, 399, -1)...))
			},
			wantSeqs:    seqs(400, 499, 1),
			wantHasMore: true,
		},
		{
			name:  "afterACursorReadsNewerMessagesAsTheyCome",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionAfter, CursorSequence: 10},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 10).WillReturnRows(cursorRows(10))
				m.ExpectQuery("sequence > ").WithArgs("conv", 10, 101).WillReturnRows(msgRows(11, 12))
			},
			wantSeqs: []int{11, 12},
		},
		{
			name:  "afterWithMoreNewerMessagesDropsTheNewestExtraOne",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionAfter, CursorSequence: 10},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 10).WillReturnRows(cursorRows(10))
				m.ExpectQuery("sequence > ").WithArgs("conv", 10, 101).WillReturnRows(msgRows(seqs(11, 111, 1)...))
			},
			wantSeqs:    seqs(11, 110, 1),
			wantHasMore: true,
		},
		{
			name:  "afterTheLastMessageIsAnEmptyPageNotAnError",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionAfter, CursorSequence: 10},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 10).WillReturnRows(cursorRows(10))
				m.ExpectQuery("sequence > ").WithArgs("conv", 10, 101).WillReturnRows(msgRows())
			},
			wantEmptyOK: true,
		},
		{
			name:  "cursorOfAnotherConversation",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 7},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 7).WillReturnRows(noRows())
				m.ExpectQuery("SELECT EXISTS").WithArgs("conv").WillReturnRows(existsRows(true))
			},
			wantErr: domain.ErrInvalidCursor,
		},
		{
			name:  "cursorInAnUnknownConversation",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionAfter, CursorSequence: 7},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 7).WillReturnRows(noRows())
				m.ExpectQuery("SELECT EXISTS").WithArgs("conv").WillReturnRows(existsRows(false))
			},
			wantErr: domain.ErrNotFound,
		},
		{
			name:  "cursorLookupFails",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 7},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 7).WillReturnError(dbDown)
			},
			wantErrText:  "find cursor",
			wantRootCaus: dbDown,
		},
		{
			name:  "existenceCheckFails",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 7},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 7).WillReturnRows(noRows())
				m.ExpectQuery("SELECT EXISTS").WithArgs("conv").WillReturnError(dbDown)
			},
			wantErrText:  "check conversation",
			wantRootCaus: dbDown,
		},
		{
			name:  "newestQueryFails",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnError(dbDown)
			},
			wantErrText:  "list messages",
			wantRootCaus: dbDown,
		},
		{
			name:  "beforeQueryFails",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 10},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 10).WillReturnRows(cursorRows(10))
				m.ExpectQuery("sequence < ").WithArgs("conv", 10, 101).WillReturnError(dbDown)
			},
			wantErrText:  "list messages",
			wantRootCaus: dbDown,
		},
		{
			name:  "scanFails",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows(messageColumns).AddRow("conv", "not a number", "appUser", "a", "text", "m", false, "E", nil, nil, at)
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnRows(rows)
			},
			wantErrText: "scan message",
		},
		{
			name:  "rowErrorEndsTheRead",
			query: domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnRows(msgRows(3).RowError(1, dbDown))
			},
			wantErrText:  "list messages",
			wantRootCaus: dbDown,
		},
		{
			name:        "unknownDirection",
			query:       domain.PageQuery{ConversationID: "conv", Direction: "sideways"},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErr:     domain.ErrInvalidRequest,
			wantErrText: "sideways",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			tt.expect(mock)

			page, err := NewPostgresMessageRepository(mock).ListMessages(context.Background(), tt.query)

			switch {
			case tt.wantErr != nil || tt.wantErrText != "":
				require.Error(t, err)
				if tt.wantErr != nil {
					assert.ErrorIs(t, err, tt.wantErr)
				}
				if tt.wantErrText != "" {
					assert.Contains(t, err.Error(), tt.wantErrText)
				}
				if tt.wantRootCaus != nil {
					assert.ErrorIs(t, err, tt.wantRootCaus)
				}
				assert.Empty(t, page.Messages)
			case tt.wantEmptyOK:
				require.NoError(t, err)
				assert.Empty(t, page.Messages)
				assert.False(t, page.HasMore)
			default:
				require.NoError(t, err)
				assert.Equal(t, tt.wantSeqs, sequencesOf(page.Messages))
				assert.Equal(t, tt.wantHasMore, page.HasMore)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestListMessagesFillsEveryField(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	var none *string
	rows := pgxmock.NewRows(messageColumns).
		AddRow("conv", 4, "appMaker", "ms-ml-genai-agent-bs", "text", "hola", true, "AgentMessageSent", none, none, at)
	mock.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnRows(rows)

	page, err := NewPostgresMessageRepository(mock).ListMessages(context.Background(), domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest})

	require.NoError(t, err)
	require.Len(t, page.Messages, 1)
	assert.Equal(t, domain.Message{
		ConversationID: "conv", Sequence: 4, Role: domain.RoleMaker, AuthorID: "ms-ml-genai-agent-bs",
		Type: "text", Text: "hola", Truncated: true, EventName: "AgentMessageSent", IDYapeAccount: "", OccurredAt: at,
	}, page.Messages[0])
}

func TestListConversations(t *testing.T) {
	dbDown := errors.New("connection reset")
	cols := []string{"conversation_id", "message_count", "first_occurred_at", "last_occurred_at"}
	summaries := func(n int) *pgxmock.Rows {
		rows := pgxmock.NewRows(cols)
		for i := range n {
			rows.AddRow("conv-"+string(rune('a'+i%26)), i+1, at, at.Add(time.Hour))
		}

		return rows
	}

	tests := []struct {
		name        string
		expect      func(m pgxmock.PgxPoolIface)
		wantCount   int
		wantHasMore bool
		wantErrText string
	}{
		{
			name: "conversationsOfTheAccount",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("GROUP BY conversation_id").WithArgs("acc-1", 101).WillReturnRows(summaries(3))
			},
			wantCount: 3,
		},
		{
			name: "noConversationsIsAnEmptyList",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("GROUP BY conversation_id").WithArgs("acc-1", 101).WillReturnRows(summaries(0))
			},
		},
		{
			name: "moreThanAPageHasMore",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("GROUP BY conversation_id").WithArgs("acc-1", 101).WillReturnRows(summaries(101))
			},
			wantCount:   100,
			wantHasMore: true,
		},
		{
			name: "queryFails",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("GROUP BY conversation_id").WithArgs("acc-1", 101).WillReturnError(dbDown)
			},
			wantErrText: "list conversations",
		},
		{
			name: "scanFails",
			expect: func(m pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows(cols).AddRow("c", "many", at, at)
				m.ExpectQuery("GROUP BY conversation_id").WithArgs("acc-1", 101).WillReturnRows(rows)
			},
			wantErrText: "scan conversation",
		},
		{
			name: "rowErrorEndsTheRead",
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("GROUP BY conversation_id").WithArgs("acc-1", 101).WillReturnRows(summaries(1).RowError(1, dbDown))
			},
			wantErrText: "list conversations",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			tt.expect(mock)

			list, err := NewPostgresMessageRepository(mock).ListConversations(context.Background(), "acc-1")

			if tt.wantErrText != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrText)
				assert.Empty(t, list.Conversations)
			} else {
				require.NoError(t, err)
				assert.Len(t, list.Conversations, tt.wantCount)
				assert.Equal(t, tt.wantHasMore, list.HasMore)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
