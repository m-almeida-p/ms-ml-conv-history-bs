package repository

import (
	"context"
	"errors"
	"fmt"
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
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnRows(msgRows(9, 8, 7))
			},
			wantSeqs: []int{7, 8, 9},
		},
		{
			name:  "newestPageWithAnExtraRowHasMoreAndDropsTheOldestOne",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnRows(msgRows(seqs(101, 1, -1)...))
			},
			wantSeqs:    seqs(2, 101, 1),
			wantHasMore: true,
		},
		{
			name:  "exactlyOnePageHasNoMore",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnRows(msgRows(seqs(100, 1, -1)...))
			},
			wantSeqs: seqs(1, 100, 1),
		},
		{
			name:  "newestOfUnknownConversation",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnRows(msgRows())
			},
			wantErr: domain.ErrNotFound,
		},
		{
			name:  "beforeACursorReadsOlderMessagesOldestFirst",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 10},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 10).WillReturnRows(cursorRows(10))
				m.ExpectQuery("sequence < ").WithArgs("conv", 10, 101).WillReturnRows(msgRows(9, 8, 7))
			},
			wantSeqs: []int{7, 8, 9},
		},
		{
			name:  "beforeWithMoreOlderMessages",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 500},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 500).WillReturnRows(cursorRows(500))
				m.ExpectQuery("sequence < ").WithArgs("conv", 500, 101).WillReturnRows(msgRows(seqs(499, 399, -1)...))
			},
			wantSeqs:    seqs(400, 499, 1),
			wantHasMore: true,
		},
		{
			name:  "afterACursorReadsNewerMessagesAsTheyCome",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionAfter, CursorSequence: 10},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 10).WillReturnRows(cursorRows(10))
				m.ExpectQuery("sequence > ").WithArgs("conv", 10, 101).WillReturnRows(msgRows(11, 12))
			},
			wantSeqs: []int{11, 12},
		},
		{
			name:  "afterWithMoreNewerMessagesDropsTheNewestExtraOne",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionAfter, CursorSequence: 10},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 10).WillReturnRows(cursorRows(10))
				m.ExpectQuery("sequence > ").WithArgs("conv", 10, 101).WillReturnRows(msgRows(seqs(11, 111, 1)...))
			},
			wantSeqs:    seqs(11, 110, 1),
			wantHasMore: true,
		},
		{
			name:  "afterTheLastMessageIsAnEmptyPageNotAnError",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionAfter, CursorSequence: 10},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 10).WillReturnRows(cursorRows(10))
				m.ExpectQuery("sequence > ").WithArgs("conv", 10, 101).WillReturnRows(msgRows())
			},
			wantEmptyOK: true,
		},
		{
			name:  "cursorOfAnotherConversation",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 7},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 7).WillReturnRows(noRows())
				m.ExpectQuery("SELECT EXISTS").WithArgs("conv").WillReturnRows(existsRows(true))
			},
			wantErr: domain.ErrInvalidCursor,
		},
		{
			name:  "cursorInAnUnknownConversation",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionAfter, CursorSequence: 7},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 7).WillReturnRows(noRows())
				m.ExpectQuery("SELECT EXISTS").WithArgs("conv").WillReturnRows(existsRows(false))
			},
			wantErr: domain.ErrNotFound,
		},
		{
			name:  "cursorLookupFails",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 7},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 7).WillReturnError(dbDown)
			},
			wantErrText:  "find cursor",
			wantRootCaus: dbDown,
		},
		{
			name:  "existenceCheckFails",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 7},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 7).WillReturnRows(noRows())
				m.ExpectQuery("SELECT EXISTS").WithArgs("conv").WillReturnError(dbDown)
			},
			wantErrText:  "check conversation",
			wantRootCaus: dbDown,
		},
		{
			name:  "newestQueryFails",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnError(dbDown)
			},
			wantErrText:  "list messages",
			wantRootCaus: dbDown,
		},
		{
			name:  "beforeQueryFails",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 10},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("FROM conversation_messages").WithArgs("conv", 10).WillReturnRows(cursorRows(10))
				m.ExpectQuery("sequence < ").WithArgs("conv", 10, 101).WillReturnError(dbDown)
			},
			wantErrText:  "list messages",
			wantRootCaus: dbDown,
		},
		{
			name:  "scanFails",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows(messageColumns).AddRow("conv", "not a number", "appUser", "a", "text", "m", false, "E", nil, nil, at)
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnRows(rows)
			},
			wantErrText: "scan message",
		},
		{
			name:  "rowErrorEndsTheRead",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 101).WillReturnRows(msgRows(3).RowError(1, dbDown))
			},
			wantErrText:  "list messages",
			wantRootCaus: dbDown,
		},
		{
			name:  "aSmallerPageReadsOneRowMoreThanItsSize",
			query: domain.PageQuery{Size: 3, ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 4).WillReturnRows(msgRows(9, 8, 7, 6))
			},
			wantSeqs:    []int{7, 8, 9},
			wantHasMore: true,
		},
		{
			name:  "aSmallerPageBeforeACursorKeepsTheNewestRowsAndTheLimit",
			query: domain.PageQuery{Size: 2, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 10},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("SELECT 1").WithArgs("conv", 10).WillReturnRows(pgxmock.NewRows([]string{"x"}).AddRow(1))
				m.ExpectQuery("sequence < ").WithArgs("conv", 10, 3).WillReturnRows(msgRows(9, 8, 7))
			},
			wantSeqs:    []int{8, 9},
			wantHasMore: true,
		},
		{
			name:  "aSmallerPageAfterACursorStopsAtItsSize",
			query: domain.PageQuery{Size: 2, ConversationID: "conv", Direction: domain.DirectionAfter, CursorSequence: 10},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("SELECT 1").WithArgs("conv", 10).WillReturnRows(pgxmock.NewRows([]string{"x"}).AddRow(1))
				m.ExpectQuery("sequence > ").WithArgs("conv", 10, 3).WillReturnRows(msgRows(11, 12))
			},
			wantSeqs: []int{11, 12},
		},
		{
			name:  "rowsEqualToTheSizeMeanNoMore",
			query: domain.PageQuery{Size: 3, ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 4).WillReturnRows(msgRows(9, 8, 7))
			},
			wantSeqs: []int{7, 8, 9},
		},
		{
			name:  "fewerRowsThanTheSizeMeanNoMore",
			query: domain.PageQuery{Size: 50, ConversationID: "conv", Direction: domain.DirectionNewest},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("ORDER BY sequence DESC").WithArgs("conv", 51).WillReturnRows(msgRows(9, 8))
			},
			wantSeqs: []int{8, 9},
		},
		{
			name:  "theLargestSizeReadsOneRowMore",
			query: domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 500},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery("SELECT 1").WithArgs("conv", 500).WillReturnRows(pgxmock.NewRows([]string{"x"}).AddRow(1))
				m.ExpectQuery("sequence < ").WithArgs("conv", 500, 101).WillReturnRows(msgRows(seqs(499, 399, -1)...))
			},
			wantSeqs:    seqs(400, 499, 1),
			wantHasMore: true,
		},
		{
			name:        "aNegativeSizeIsRefusedBeforeTheStoreIsAsked",
			query:       domain.PageQuery{Size: -1, ConversationID: "conv", Direction: domain.DirectionNewest},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErr:     domain.ErrInvalidRequest,
			wantErrText: "page size",
		},
		{
			name:        "aBadSizeIsRefusedBeforeTheCursorIsChecked",
			query:       domain.PageQuery{Size: 0, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 10},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErr:     domain.ErrInvalidRequest,
			wantErrText: "page size",
		},
		{
			name:        "aSizeOfZeroIsRefusedBeforeTheStoreIsAsked",
			query:       domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErr:     domain.ErrInvalidRequest,
			wantErrText: "page size",
		},
		{
			name:        "aSizeOverTheMaximumIsRefusedBeforeTheStoreIsAsked",
			query:       domain.PageQuery{Size: domain.PageSize + 1, ConversationID: "conv", Direction: domain.DirectionNewest},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErr:     domain.ErrInvalidRequest,
			wantErrText: "page size",
		},
		{
			name:        "unknownDirection",
			query:       domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: "sideways"},
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

	page, err := NewPostgresMessageRepository(mock).ListMessages(context.Background(), domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionNewest})

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
	// rowsOf returns the named conversations in the order given, each one an hour older than the one before.
	rowsOf := func(ids ...string) *pgxmock.Rows {
		rows := pgxmock.NewRows(cols)
		for i, id := range ids {
			rows.AddRow(id, i+1, at, at.Add(-time.Duration(i)*time.Hour))
		}

		return rows
	}
	named := func(prefix string, n int) []string {
		ids := make([]string, 0, n)
		for i := range n {
			ids = append(ids, fmt.Sprintf("%s%03d", prefix, i))
		}

		return ids
	}
	cursorRow := func() *pgxmock.Rows { return pgxmock.NewRows([]string{"last_occurred_at"}).AddRow(at) }
	// A read of the newest page, and of a page beside a cursor, as the statements are told apart.
	const (
		newestSQL = `GROUP BY conversation_id\s+ORDER BY`
		beforeSQL = `HAVING max\(occurred_at\) <`
		afterSQL  = `HAVING max\(occurred_at\) >`
		cursorSQL = `SELECT max\(occurred_at\) AS last_occurred_at`
	)
	size := domain.ConversationListSize

	tests := []struct {
		name        string
		query       domain.ConversationQuery
		expect      func(m pgxmock.PgxPoolIface)
		wantIDs     []string
		wantHasMore bool
		wantErr     error
		wantErrText string
	}{
		{
			name:  "newestPageOfTheAccount",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: size},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(newestSQL).WithArgs("acc-1", 101).WillReturnRows(rowsOf("c", "b", "a"))
			},
			wantIDs: []string{"c", "b", "a"},
		},
		{
			name:  "noConversationsIsAnEmptyPage",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: size},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(newestSQL).WithArgs("acc-1", 101).WillReturnRows(rowsOf())
			},
			wantIDs: []string{},
		},
		{
			name:  "moreThanAPageHasMoreAndDropsTheOldest",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: size},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(newestSQL).WithArgs("acc-1", 101).WillReturnRows(rowsOf(named("c", 101)...))
			},
			wantIDs:     named("c", 100),
			wantHasMore: true,
		},
		{
			name:  "exactlyOnePageHasNoMore",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: size},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(newestSQL).WithArgs("acc-1", 101).WillReturnRows(rowsOf(named("c", 100)...))
			},
			wantIDs: named("c", 100),
		},
		{
			name:  "aSmallerPageReadsOneRowMoreThanItsSize",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: 3},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(newestSQL).WithArgs("acc-1", 4).WillReturnRows(rowsOf("d", "c", "b", "a"))
			},
			wantIDs:     []string{"d", "c", "b"},
			wantHasMore: true,
		},
		{
			name:  "rowsEqualToTheSizeMeanNoMore",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: 3},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(newestSQL).WithArgs("acc-1", 4).WillReturnRows(rowsOf("c", "b", "a"))
			},
			wantIDs: []string{"c", "b", "a"},
		},
		{
			name:  "beforeACursorChecksItThenReadsOlderConversations",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionBefore, CursorID: "c", Size: 2},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(cursorSQL).WithArgs("acc-1", "c").WillReturnRows(cursorRow())
				m.ExpectQuery(beforeSQL).WithArgs("acc-1", at, "c", 3).WillReturnRows(rowsOf("b", "a", "z"))
			},
			wantIDs:     []string{"b", "a"},
			wantHasMore: true,
		},
		{
			name:  "beforeTheOldestHasNothing",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionBefore, CursorID: "a", Size: size},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(cursorSQL).WithArgs("acc-1", "a").WillReturnRows(cursorRow())
				m.ExpectQuery(beforeSQL).WithArgs("acc-1", at, "a", 101).WillReturnRows(rowsOf())
			},
			wantIDs: []string{},
		},
		{
			name:  "afterACursorReadsMoreRecentOnesAndReturnsThemMostRecentFirst",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionAfter, CursorID: "c", Size: 2},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(cursorSQL).WithArgs("acc-1", "c").WillReturnRows(cursorRow())
				// The statement reads upwards from the cursor, nearest first: d, e, f.
				m.ExpectQuery(afterSQL).WithArgs("acc-1", at, "c", 3).WillReturnRows(rowsOf("d", "e", "f"))
			},
			wantIDs:     []string{"e", "d"},
			wantHasMore: true,
		},
		{
			name:  "afterWithFewerRowsThanTheSizeHasNoMore",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionAfter, CursorID: "c", Size: 5},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(cursorSQL).WithArgs("acc-1", "c").WillReturnRows(cursorRow())
				m.ExpectQuery(afterSQL).WithArgs("acc-1", at, "c", 6).WillReturnRows(rowsOf("d"))
			},
			wantIDs: []string{"d"},
		},
		{
			name:  "afterTheMostRecentHasNothing",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionAfter, CursorID: "z", Size: size},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(cursorSQL).WithArgs("acc-1", "z").WillReturnRows(cursorRow())
				m.ExpectQuery(afterSQL).WithArgs("acc-1", at, "z", 101).WillReturnRows(rowsOf())
			},
			wantIDs: []string{},
		},
		{
			name:  "aCursorThatIsNotAConversationOfTheAccountIsInvalid",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionBefore, CursorID: "theirs", Size: size},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(cursorSQL).WithArgs("acc-1", "theirs").WillReturnRows(pgxmock.NewRows([]string{"last_occurred_at"}))
			},
			wantErr: domain.ErrInvalidCursor,
		},
		{
			name:  "theCursorLookupFails",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionAfter, CursorID: "c", Size: size},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(cursorSQL).WithArgs("acc-1", "c").WillReturnError(dbDown)
			},
			wantErrText: "find conversation cursor",
		},
		{
			name:        "aSizeOfZeroIsRefusedBeforeTheStoreIsAsked",
			query:       domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErr:     domain.ErrInvalidRequest,
			wantErrText: "page size",
		},
		{
			name:        "aNegativeSizeIsRefused",
			query:       domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: -1},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErr:     domain.ErrInvalidRequest,
			wantErrText: "page size",
		},
		{
			name:        "aSizeOverTheMaximumIsRefused",
			query:       domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: size + 1},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErr:     domain.ErrInvalidRequest,
			wantErrText: "page size",
		},
		{
			name:        "aBadSizeIsRefusedBeforeTheCursorIsChecked",
			query:       domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionBefore, CursorID: "c"},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErr:     domain.ErrInvalidRequest,
			wantErrText: "page size",
		},
		{
			name:        "unknownDirection",
			query:       domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: "sideways", Size: size},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErr:     domain.ErrInvalidRequest,
			wantErrText: "sideways",
		},
		{
			name:  "queryFails",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: size},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(newestSQL).WithArgs("acc-1", 101).WillReturnError(dbDown)
			},
			wantErrText: "list conversations",
		},
		{
			name:  "scanFails",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: size},
			expect: func(m pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows(cols).AddRow("c", "many", at, at)
				m.ExpectQuery(newestSQL).WithArgs("acc-1", 101).WillReturnRows(rows)
			},
			wantErrText: "scan conversation",
		},
		{
			name:  "rowErrorEndsTheRead",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: size},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(newestSQL).WithArgs("acc-1", 101).WillReturnRows(rowsOf("a").RowError(1, dbDown))
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

			list, err := NewPostgresMessageRepository(mock).ListConversations(context.Background(), tt.query)

			if tt.wantErr != nil || tt.wantErrText != "" {
				require.Error(t, err)
				if tt.wantErr != nil {
					assert.ErrorIs(t, err, tt.wantErr)
				}
				if tt.wantErrText != "" {
					assert.Contains(t, err.Error(), tt.wantErrText)
				}
				assert.Empty(t, list.Conversations)
			} else {
				require.NoError(t, err)
				ids := make([]string, 0, len(list.Conversations))
				for _, c := range list.Conversations {
					ids = append(ids, c.ConversationID)
				}
				assert.Equal(t, tt.wantIDs, ids)
				assert.Equal(t, tt.wantHasMore, list.HasMore)
			}
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
