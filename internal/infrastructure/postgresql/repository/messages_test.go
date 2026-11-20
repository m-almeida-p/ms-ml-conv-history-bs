package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
			wantErrText: "page size",
		},
		{
			name:        "aBadSizeIsRefusedBeforeTheCursorIsChecked",
			query:       domain.PageQuery{Size: 0, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 10},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErrText: "page size",
		},
		{
			name:        "aSizeOfZeroIsRefusedBeforeTheStoreIsAsked",
			query:       domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErrText: "page size",
		},
		{
			name:        "aSizeOverTheMaximumIsRefusedBeforeTheStoreIsAsked",
			query:       domain.PageQuery{Size: domain.PageSize + 1, ConversationID: "conv", Direction: domain.DirectionNewest},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErrText: "page size",
		},
		{
			name:        "unknownDirection",
			query:       domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: "sideways"},
			expect:      func(pgxmock.PgxPoolIface) {},
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
	cols := []string{"conversation_id", "message_count", "first_occurred_at", "last_occurred_at", "first_message", "first_message_truncated"}
	// rowsOf returns the named conversations in the order given, each one an hour older than the one before.
	rowsOf := func(ids ...string) *pgxmock.Rows {
		rows := pgxmock.NewRows(cols)
		for i, id := range ids {
			rows.AddRow(id, i+1, at, at.Add(-time.Duration(i)*time.Hour), (*string)(nil), (*bool)(nil))
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
	// A cursor is a position: the time of a conversation's last message and its id.
	cursorAt := func(id string) domain.ConversationCursor {
		return domain.ConversationCursor{LastOccurredAt: at, ConversationID: id}
	}
	// A read of the newest page, and of a page beside a cursor, as the statements are told apart.
	const (
		newestSQL = `FROM conversation_summaries\s+WHERE 1=1\s+AND id_yape_account = \$1\s+ORDER BY`
		beforeSQL = `\(last_occurred_at, conversation_id\) < \(\$2, \$3\)`
		afterSQL  = `\(last_occurred_at, conversation_id\) > \(\$2, \$3\)`
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
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionBefore, Cursor: cursorAt("c"), Size: 2},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(beforeSQL).WithArgs("acc-1", at, "c", 3).WillReturnRows(rowsOf("b", "a", "z"))
			},
			wantIDs:     []string{"b", "a"},
			wantHasMore: true,
		},
		{
			name:  "beforeTheOldestHasNothing",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionBefore, Cursor: cursorAt("a"), Size: size},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(beforeSQL).WithArgs("acc-1", at, "a", 101).WillReturnRows(rowsOf())
			},
			wantIDs: []string{},
		},
		{
			name:  "afterACursorReadsMoreRecentOnesAndReturnsThemMostRecentFirst",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionAfter, Cursor: cursorAt("c"), Size: 2},
			expect: func(m pgxmock.PgxPoolIface) {
				// The statement reads upwards from the cursor, nearest first: d, e, f.
				m.ExpectQuery(afterSQL).WithArgs("acc-1", at, "c", 3).WillReturnRows(rowsOf("d", "e", "f"))
			},
			wantIDs:     []string{"e", "d"},
			wantHasMore: true,
		},
		{
			name:  "afterWithFewerRowsThanTheSizeHasNoMore",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionAfter, Cursor: cursorAt("c"), Size: 5},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(afterSQL).WithArgs("acc-1", at, "c", 6).WillReturnRows(rowsOf("d"))
			},
			wantIDs: []string{"d"},
		},
		{
			name:  "afterTheMostRecentHasNothing",
			query: domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionAfter, Cursor: cursorAt("z"), Size: size},
			expect: func(m pgxmock.PgxPoolIface) {
				m.ExpectQuery(afterSQL).WithArgs("acc-1", at, "z", 101).WillReturnRows(rowsOf())
			},
			wantIDs: []string{},
		},
		{
			name:        "aSizeOfZeroIsRefusedBeforeTheStoreIsAsked",
			query:       domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErrText: "page size",
		},
		{
			name:        "aNegativeSizeIsRefused",
			query:       domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: -1},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErrText: "page size",
		},
		{
			name:        "aSizeOverTheMaximumIsRefused",
			query:       domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: size + 1},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErrText: "page size",
		},
		{
			name:        "aBadSizeIsRefusedBeforeTheCursorIsChecked",
			query:       domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionBefore, Cursor: cursorAt("c")},
			expect:      func(pgxmock.PgxPoolIface) {},
			wantErrText: "page size",
		},
		{
			name:        "unknownDirection",
			query:       domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: "sideways", Size: size},
			expect:      func(pgxmock.PgxPoolIface) {},
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
				rows := pgxmock.NewRows(cols).AddRow("c", "many", at, at, (*string)(nil), (*bool)(nil))
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

func TestBrokenInvariantsAreNotTheClientsFault(t *testing.T) {
	// A bad size or direction can only come from a caller inside the service, so it is a server
	// error. A request the client got wrong is rejected by the service before it gets here.
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	repo := NewPostgresMessageRepository(mock)

	_, err = repo.ListMessages(context.Background(), domain.PageQuery{ConversationID: "c", Direction: domain.DirectionNewest})
	assert.Error(t, err)
	assert.NotErrorIs(t, err, domain.ErrInvalidRequest)

	_, err = repo.ListMessages(context.Background(), domain.PageQuery{ConversationID: "c", Direction: "sideways", Size: 5})
	assert.Error(t, err)
	assert.NotErrorIs(t, err, domain.ErrInvalidRequest)

	_, err = repo.ListConversations(context.Background(), domain.ConversationQuery{IDYapeAccount: "a", Direction: domain.DirectionNewest})
	assert.Error(t, err)
	assert.NotErrorIs(t, err, domain.ErrInvalidRequest)

	_, err = repo.ListConversations(context.Background(), domain.ConversationQuery{IDYapeAccount: "a", Direction: "sideways", Size: 5})
	assert.Error(t, err)
	assert.NotErrorIs(t, err, domain.ErrInvalidRequest)

	assert.NoError(t, mock.ExpectationsWereMet(), "none of them reached the database")
}

// The first message of each conversation comes with the list, in the same statement: the conversation's first message from the user,
// cut at domain.FirstMessageMaxChars characters, and whether it was cut. A conversation with no message from the user has none.
func TestListConversations_TheFirstMessageOfEachConversation(t *testing.T) {
	cols := []string{"conversation_id", "message_count", "first_occurred_at", "last_occurred_at", "first_message", "first_message_truncated"}
	text, cut, yes := "quiero yapear", false, true
	long := strings.Repeat("ñ", domain.FirstMessageMaxChars)

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	mock.ExpectQuery(`FROM conversation_summaries\s+WHERE 1=1\s+AND id_yape_account = \$1\s+ORDER BY`).WithArgs("acc-1", 4).WillReturnRows(
		pgxmock.NewRows(cols).
			AddRow("c", 3, at, at, &text, &cut).
			AddRow("b", 2, at, at, &long, &yes).
			AddRow("a", 1, at, at, (*string)(nil), (*bool)(nil)))

	list, err := NewPostgresMessageRepository(mock).ListConversations(t.Context(),
		domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: 3})

	require.NoError(t, err)
	require.Len(t, list.Conversations, 3)
	assert.Equal(t, "quiero yapear", list.Conversations[0].FirstMessage)
	assert.False(t, list.Conversations[0].FirstMessageTruncated)
	assert.Equal(t, long, list.Conversations[1].FirstMessage)
	assert.True(t, list.Conversations[1].FirstMessageTruncated)
	assert.Empty(t, list.Conversations[2].FirstMessage, "no message from the user: none")
	assert.False(t, list.Conversations[2].FirstMessageTruncated)
	require.NoError(t, mock.ExpectationsWereMet())
}

// One statement, and the same limit in all three: the limit is a constant of the domain written into the SQL, so the two cannot differ.
func TestConversationStatements_CutTheFirstMessageAtTheLimitOfTheDomain(t *testing.T) {
	for name, sql := range map[string]string{
		"newest": sqlListConversations, "before": sqlListConversationsBefore, "after": sqlListConversationsAfter,
	} {
		t.Run(name, func(t *testing.T) {
			assert.Contains(t, sql, fmt.Sprintf("left(m.text, %d)", domain.FirstMessageMaxChars))
			assert.Contains(t, sql, fmt.Sprintf("char_length(m.text) > %d", domain.FirstMessageMaxChars))
			assert.NotContains(t, sql, "{{", "no placeholder is left in the statement")
		})
	}
}
