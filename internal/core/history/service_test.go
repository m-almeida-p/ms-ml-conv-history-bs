package history

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository/mocks"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
)

func messagesAt(sequences ...int) []domain.Message {
	out := make([]domain.Message, 0, len(sequences))
	for _, sequence := range sequences {
		out = append(out, domain.Message{ConversationID: "conv", Sequence: sequence})
	}

	return out
}

func TestMessages(t *testing.T) {
	storeDown := errors.New("connection reset")

	tests := []struct {
		name        string
		req         service.MessagesRequest
		wantQuery   *domain.PageQuery // nil when the store must not be called
		page        domain.Page
		repoErr     error
		want        domain.MessagePage
		wantErr     error
		wantErrText string
	}{
		{
			name:      "noCursorAsksForTheNewestPage",
			req:       service.MessagesRequest{ConversationID: "conv_v1_a"},
			wantQuery: &domain.PageQuery{Size: domain.PageSize, ConversationID: "conv_v1_a", Direction: domain.DirectionNewest},
			page:      domain.Page{Messages: messagesAt(42, 43, 44), HasMore: true},
			want: domain.MessagePage{
				Size: domain.PageSize, ConversationID: "conv_v1_a", Direction: domain.DirectionNewest, Messages: messagesAt(42, 43, 44),
				HasMore: true, BeforeCursor: "42", AfterCursor: "44",
			},
		},
		{
			name:      "beforeAsksForOlderMessages",
			req:       service.MessagesRequest{ConversationID: "conv", Before: ptr("42")},
			wantQuery: &domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 42},
			page:      domain.Page{Messages: messagesAt(30, 31)},
			want: domain.MessagePage{
				Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionBefore, Messages: messagesAt(30, 31),
				BeforeCursor: "30", AfterCursor: "31",
			},
		},
		{
			name:      "afterAsksForNewerMessages",
			req:       service.MessagesRequest{ConversationID: "conv", After: ptr("7")},
			wantQuery: &domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionAfter, CursorSequence: 7},
			page:      domain.Page{Messages: messagesAt(8), HasMore: true},
			want: domain.MessagePage{
				Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionAfter, Messages: messagesAt(8),
				HasMore: true, BeforeCursor: "8", AfterCursor: "8",
			},
		},
		{
			name:      "emptyPageHasNoCursors",
			req:       service.MessagesRequest{ConversationID: "conv", After: ptr("9")},
			wantQuery: &domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionAfter, CursorSequence: 9},
			page:      domain.Page{},
			want:      domain.MessagePage{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionAfter},
		},
		{
			name:      "cursorsAreTheLargestSequenceToo",
			req:       service.MessagesRequest{ConversationID: "conv", Before: ptr("2147483647")},
			wantQuery: &domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 2147483647},
			page:      domain.Page{Messages: messagesAt(1)},
			want:      domain.MessagePage{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionBefore, Messages: messagesAt(1), BeforeCursor: "1", AfterCursor: "1"},
		},
		{
			name:      "pageSizeAsksForFewerMessages",
			req:       service.MessagesRequest{ConversationID: "conv", Size: ptr("5")},
			wantQuery: &domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest, Size: 5},
			page:      domain.Page{Messages: messagesAt(8, 9), HasMore: true},
			want: domain.MessagePage{
				ConversationID: "conv", Direction: domain.DirectionNewest, Size: 5, Messages: messagesAt(8, 9),
				HasMore: true, BeforeCursor: "8", AfterCursor: "9",
			},
		},
		{
			name:      "pageSizeTravelsWithACursor",
			req:       service.MessagesRequest{ConversationID: "conv", Before: ptr("42"), Size: ptr("1")},
			wantQuery: &domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 42, Size: 1},
			page:      domain.Page{Messages: messagesAt(41), HasMore: true},
			want: domain.MessagePage{
				ConversationID: "conv", Direction: domain.DirectionBefore, Size: 1, Messages: messagesAt(41),
				HasMore: true, BeforeCursor: "41", AfterCursor: "41",
			},
		},
		{
			name:      "pageSizeAtTheMaximumIsAccepted",
			req:       service.MessagesRequest{ConversationID: "conv", Size: ptr("100")},
			wantQuery: &domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest, Size: 100},
			page:      domain.Page{Messages: messagesAt(1)},
			want:      domain.MessagePage{ConversationID: "conv", Direction: domain.DirectionNewest, Size: 100, Messages: messagesAt(1), BeforeCursor: "1", AfterCursor: "1"},
		},
		{name: "pageSizeEmpty", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeZero", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("0")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeNegative", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("-1")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeOverTheMaximum", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("101")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeNotANumber", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("ten")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeWithAPlusSign", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("+5")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeWithLeadingZeros", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("05")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeBeyondAnInt64", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("9223372036854775808")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeWithASpaceBefore", req: service.MessagesRequest{ConversationID: "conv", Size: ptr(" 5")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeWithASpaceAfter", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("5 ")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeInExponentForm", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("1e2")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeWithADecimalPoint", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("5.0")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeInHexadecimal", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("0x10")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeWithUnderscores", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("1_0")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeInArabicIndicDigits", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("٥")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeWithANulCharacter", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("5\x00")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeJustBeyondAnInt32", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("2147483648")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeTheLargestInt32", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("2147483647")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeVeryLong", req: service.MessagesRequest{ConversationID: "conv", Size: ptr(strings.Repeat("9", 5000))}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeJustBelowTheMaximumIsAccepted", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("99")},
			wantQuery: &domain.PageQuery{Size: 99, ConversationID: "conv", Direction: domain.DirectionNewest},
			page:      domain.Page{Messages: messagesAt(1)},
			want:      domain.MessagePage{ConversationID: "conv", Direction: domain.DirectionNewest, Size: 99, Messages: messagesAt(1), BeforeCursor: "1", AfterCursor: "1"}},
		// The checks run in a fixed order: conversation id, page size, cursors.
		{name: "aMalformedIDIsReportedBeforeABadPageSize", req: service.MessagesRequest{ConversationID: "bad id", Size: ptr("0")}, wantErr: domain.ErrInvalidRequest, wantErrText: "conversation id"},
		{name: "aBadPageSizeIsReportedBeforeABadCursor", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("0"), Before: ptr("abc")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "aBadPageSizeIsReportedBeforeTwoCursors", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("0"), Before: ptr("1"), After: ptr("2")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "aGoodPageSizeStillLetsABadCursorThrough", req: service.MessagesRequest{ConversationID: "conv", Size: ptr("5"), Before: ptr("abc")}, wantErr: domain.ErrInvalidCursor},
		{name: "bothCursorsAreRefused", req: service.MessagesRequest{ConversationID: "conv", Before: ptr("1"), After: ptr("2")}, wantErr: domain.ErrInvalidRequest, wantErrText: "cannot be used together"},
		{name: "emptyConversationID", req: service.MessagesRequest{}, wantErr: domain.ErrInvalidRequest, wantErrText: "conversation id"},
		{name: "conversationIDWithASpace", req: service.MessagesRequest{ConversationID: "conv a"}, wantErr: domain.ErrInvalidRequest},
		{name: "conversationIDWithASlash", req: service.MessagesRequest{ConversationID: "../etc"}, wantErr: domain.ErrInvalidRequest},
		{name: "conversationIDWithAQuote", req: service.MessagesRequest{ConversationID: "conv'--"}, wantErr: domain.ErrInvalidRequest},
		{name: "conversationIDTooLong", req: service.MessagesRequest{ConversationID: strings.Repeat("a", 129)}, wantErr: domain.ErrInvalidRequest},
		{name: "emptyBeforeIsAnInvalidCursor", req: service.MessagesRequest{ConversationID: "conv", Before: ptr("")}, wantErr: domain.ErrInvalidCursor},
		{name: "emptyAfterIsAnInvalidCursor", req: service.MessagesRequest{ConversationID: "conv", After: ptr("")}, wantErr: domain.ErrInvalidCursor},
		{name: "anEmptyCursorBesideARealOneIsStillTwoCursors", req: service.MessagesRequest{ConversationID: "conv", Before: ptr(""), After: ptr("5")}, wantErr: domain.ErrInvalidRequest, wantErrText: "cannot be used together"},
		{name: "cursorNotANumber", req: service.MessagesRequest{ConversationID: "conv", Before: ptr("abc")}, wantErr: domain.ErrInvalidCursor},
		{name: "cursorZero", req: service.MessagesRequest{ConversationID: "conv", After: ptr("0")}, wantErr: domain.ErrInvalidCursor},
		{name: "cursorNegative", req: service.MessagesRequest{ConversationID: "conv", After: ptr("-4")}, wantErr: domain.ErrInvalidCursor},
		{name: "cursorWithAPlusSign", req: service.MessagesRequest{ConversationID: "conv", Before: ptr("+5")}, wantErr: domain.ErrInvalidCursor},
		{name: "cursorWithLeadingZeros", req: service.MessagesRequest{ConversationID: "conv", Before: ptr("05")}, wantErr: domain.ErrInvalidCursor},
		{name: "cursorTooBig", req: service.MessagesRequest{ConversationID: "conv", After: ptr("2147483648")}, wantErr: domain.ErrInvalidCursor},
		{name: "cursorBeyondAnInt64", req: service.MessagesRequest{ConversationID: "conv", After: ptr("9223372036854775808")}, wantErr: domain.ErrInvalidCursor},
		{
			name:      "unknownConversation",
			req:       service.MessagesRequest{ConversationID: "conv"},
			wantQuery: &domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionNewest},
			repoErr:   domain.ErrNotFound, wantErr: domain.ErrNotFound,
		},
		{
			name:      "cursorOfAnotherConversation",
			req:       service.MessagesRequest{ConversationID: "conv", Before: ptr("3")},
			wantQuery: &domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 3},
			repoErr:   domain.ErrInvalidCursor, wantErr: domain.ErrInvalidCursor,
		},
		{
			name:      "storeErrorIsReturnedAsIs",
			req:       service.MessagesRequest{ConversationID: "conv"},
			wantQuery: &domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionNewest},
			repoErr:   storeDown, wantErr: storeDown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A conversation is read for an account, and only the account's own is found: every case here is read as acc-1.
			tt.req.IDYapeAccount = "acc-1"
			repo := mocks.NewMessageRepository(t)
			if tt.wantQuery != nil {
				query := *tt.wantQuery
				query.IDYapeAccount = "acc-1"
				repo.On("ListMessages", mock.Anything, query).Return(tt.page, tt.repoErr).Once()
			}

			got, err := NewService(repo).Messages(context.Background(), tt.req)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				if tt.wantErrText != "" {
					assert.Contains(t, err.Error(), tt.wantErrText)
				}
				assert.Equal(t, domain.MessagePage{}, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMessagesErrorsNeverEchoTheInput(t *testing.T) {
	const input = "secret-cursor-4111"

	tests := []struct {
		name string
		req  service.MessagesRequest
	}{
		{name: "badCursor", req: service.MessagesRequest{ConversationID: "conv", Before: ptr(input)}},
		{name: "badConversationID", req: service.MessagesRequest{ConversationID: input + " x"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewService(mocks.NewMessageRepository(t)).Messages(context.Background(), tt.req)

			require.Error(t, err)
			assert.NotContains(t, err.Error(), input)
		})
	}
}

// instant returns the n-th second of a fixed day, which is also its cursor instant in microseconds / 1e6.
func instant(n int) time.Time { return time.Date(2026, 10, 27, 10, 0, n, 0, time.UTC) }

// conversationsAt returns conversations, most recent first, each last active at the given second.
func conversationsAt(seconds map[string]int, ids ...string) []domain.ConversationSummary {
	out := make([]domain.ConversationSummary, 0, len(ids))
	for _, id := range ids {
		out = append(out, domain.ConversationSummary{ConversationID: id, MessageCount: 2, LastOccurredAt: instant(seconds[id])})
	}

	return out
}

// cursorOf is the cursor string of a conversation last active at the given second.
func cursorOf(second int, id string) string {
	return domain.NewConversationCursor(instant(second), id).String()
}

func TestConversations(t *testing.T) {
	storeDown := errors.New("connection reset")
	at := map[string]int{"c3": 30, "c2": 20, "c1": 10, "b2": 8, "b1": 5, "d1": 40}
	cursorC1 := domain.ConversationCursor{LastOccurredAt: instant(10), ConversationID: "c1"}
	cursorC3 := domain.ConversationCursor{LastOccurredAt: instant(30), ConversationID: "c3"}

	tests := []struct {
		name        string
		req         service.ConversationsRequest
		wantQuery   *domain.ConversationQuery // nil when the store must not be called
		page        domain.ConversationList
		repoErr     error
		want        domain.ConversationList
		wantErr     error
		wantErrText string
	}{
		{
			name:      "noCursorAsksForTheMostRecentPage",
			req:       service.ConversationsRequest{IDYapeAccount: "acc-1"},
			wantQuery: &domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: domain.ConversationListSize},
			page:      domain.ConversationList{Conversations: conversationsAt(at, "c3", "c2", "c1"), HasMore: true},
			want: domain.ConversationList{
				Direction: domain.DirectionNewest, Size: domain.ConversationListSize, Conversations: conversationsAt(at, "c3", "c2", "c1"),
				HasMore: true, BeforeCursor: cursorOf(10, "c1"), AfterCursor: cursorOf(30, "c3"),
			},
		},
		{
			name:      "beforeAsksForOlderConversationsFromThePositionInTheCursor",
			req:       service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr(cursorOf(10, "c1"))},
			wantQuery: &domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionBefore, Cursor: cursorC1, Size: domain.ConversationListSize},
			page:      domain.ConversationList{Conversations: conversationsAt(at, "b2", "b1")},
			want: domain.ConversationList{
				Direction: domain.DirectionBefore, Size: domain.ConversationListSize, Conversations: conversationsAt(at, "b2", "b1"),
				BeforeCursor: cursorOf(5, "b1"), AfterCursor: cursorOf(8, "b2"),
			},
		},
		{
			name:      "afterAsksForMoreRecentConversations",
			req:       service.ConversationsRequest{IDYapeAccount: "acc-1", After: ptr(cursorOf(30, "c3"))},
			wantQuery: &domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionAfter, Cursor: cursorC3, Size: domain.ConversationListSize},
			page:      domain.ConversationList{Conversations: conversationsAt(at, "d1"), HasMore: true},
			want: domain.ConversationList{
				Direction: domain.DirectionAfter, Size: domain.ConversationListSize, Conversations: conversationsAt(at, "d1"),
				HasMore: true, BeforeCursor: cursorOf(40, "d1"), AfterCursor: cursorOf(40, "d1"),
			},
		},
		{
			name:      "pageSizeAsksForFewer",
			req:       service.ConversationsRequest{IDYapeAccount: "acc-1", Size: ptr("20")},
			wantQuery: &domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: 20},
			page:      domain.ConversationList{Conversations: conversationsAt(at, "c1")},
			want:      domain.ConversationList{Direction: domain.DirectionNewest, Size: 20, Conversations: conversationsAt(at, "c1"), BeforeCursor: cursorOf(10, "c1"), AfterCursor: cursorOf(10, "c1")},
		},
		{
			name:      "pageSizeTravelsWithACursor",
			req:       service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr(cursorOf(10, "c1")), Size: ptr("1")},
			wantQuery: &domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionBefore, Cursor: cursorC1, Size: 1},
			page:      domain.ConversationList{Conversations: conversationsAt(at, "b1"), HasMore: true},
			want:      domain.ConversationList{Direction: domain.DirectionBefore, Size: 1, Conversations: conversationsAt(at, "b1"), HasMore: true, BeforeCursor: cursorOf(5, "b1"), AfterCursor: cursorOf(5, "b1")},
		},
		{
			name:      "accountWithNoConversationsIsAnEmptyPageWithoutCursors",
			req:       service.ConversationsRequest{IDYapeAccount: "acc-2"},
			wantQuery: &domain.ConversationQuery{IDYapeAccount: "acc-2", Direction: domain.DirectionNewest, Size: domain.ConversationListSize},
			page:      domain.ConversationList{Conversations: []domain.ConversationSummary{}},
			want:      domain.ConversationList{Direction: domain.DirectionNewest, Size: domain.ConversationListSize, Conversations: []domain.ConversationSummary{}},
		},
		{
			name:      "uuidAccount",
			req:       service.ConversationsRequest{IDYapeAccount: "c59f8a80-6e65-4d1b-9c0e-0a1b2c3d4e5f"},
			wantQuery: &domain.ConversationQuery{IDYapeAccount: "c59f8a80-6e65-4d1b-9c0e-0a1b2c3d4e5f", Direction: domain.DirectionNewest, Size: domain.ConversationListSize},
			page:      domain.ConversationList{Conversations: conversationsAt(at, "c1")},
			want:      domain.ConversationList{Direction: domain.DirectionNewest, Size: domain.ConversationListSize, Conversations: conversationsAt(at, "c1"), BeforeCursor: cursorOf(10, "c1"), AfterCursor: cursorOf(10, "c1")},
		},
		{
			name:      "aCursorOfAnyAccountIsJustAPosition",
			req:       service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr(cursorOf(10, "someone-elses"))},
			wantQuery: &domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionBefore, Cursor: domain.ConversationCursor{LastOccurredAt: instant(10), ConversationID: "someone-elses"}, Size: domain.ConversationListSize},
			page:      domain.ConversationList{Conversations: []domain.ConversationSummary{}},
			want:      domain.ConversationList{Direction: domain.DirectionBefore, Size: domain.ConversationListSize, Conversations: []domain.ConversationSummary{}},
		},
		{name: "missingAccount", req: service.ConversationsRequest{}, wantErr: domain.ErrInvalidRequest, wantErrText: "idYapeAccount"},
		{name: "accountWithSpaces", req: service.ConversationsRequest{IDYapeAccount: "acc 1"}, wantErr: domain.ErrInvalidRequest},
		{name: "accountWithSQLCharacters", req: service.ConversationsRequest{IDYapeAccount: "x'; drop table--"}, wantErr: domain.ErrInvalidRequest},
		{name: "accountTooLong", req: service.ConversationsRequest{IDYapeAccount: strings.Repeat("a", 129)}, wantErr: domain.ErrInvalidRequest},
		{name: "bothCursorsAreRefused", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr("a"), After: ptr("b")}, wantErr: domain.ErrInvalidRequest, wantErrText: "cannot be used together"},
		{name: "anEmptyCursorBesideARealOneIsStillTwoCursors", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr(""), After: ptr("b")}, wantErr: domain.ErrInvalidRequest, wantErrText: "cannot be used together"},
		{name: "emptyBeforeIsAnInvalidCursor", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr("")}, wantErr: domain.ErrInvalidCursor},
		{name: "emptyAfterIsAnInvalidCursor", req: service.ConversationsRequest{IDYapeAccount: "acc-1", After: ptr("")}, wantErr: domain.ErrInvalidCursor},
		{name: "aConversationIDIsNoLongerACursor", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr("conv_v1_a")}, wantErr: domain.ErrInvalidCursor},
		{name: "cursorWithoutTheSeparator", req: service.ConversationsRequest{IDYapeAccount: "acc-1", After: ptr("12345")}, wantErr: domain.ErrInvalidCursor},
		{name: "cursorWithASpaceInTheID", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr("5~c 1")}, wantErr: domain.ErrInvalidCursor},
		{name: "cursorWithSQLCharacters", req: service.ConversationsRequest{IDYapeAccount: "acc-1", After: ptr("5~x'; drop table--")}, wantErr: domain.ErrInvalidCursor},
		{name: "cursorTooLong", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr("5~" + strings.Repeat("c", 129))}, wantErr: domain.ErrInvalidCursor},
		{name: "cursorWithANulCharacter", req: service.ConversationsRequest{IDYapeAccount: "acc-1", After: ptr("5~c\x001")}, wantErr: domain.ErrInvalidCursor},
		{name: "pageSizeZero", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Size: ptr("0")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeOverTheMaximum", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Size: ptr("101")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeEmpty", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Size: ptr("")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeNotANumber", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Size: ptr("many")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeWithAPlusSign", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Size: ptr("+5")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "pageSizeWithLeadingZeros", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Size: ptr("05")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		// The checks run in a fixed order: account, page size, cursors.
		{name: "aMissingAccountIsReportedBeforeABadPageSize", req: service.ConversationsRequest{Size: ptr("0")}, wantErr: domain.ErrInvalidRequest, wantErrText: "idYapeAccount"},
		{name: "aBadPageSizeIsReportedBeforeABadCursor", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Size: ptr("0"), Before: ptr("")}, wantErr: domain.ErrInvalidRequest, wantErrText: "page[size]"},
		{name: "aGoodPageSizeStillLetsABadCursorThrough", req: service.ConversationsRequest{IDYapeAccount: "acc-1", Size: ptr("5"), Before: ptr("")}, wantErr: domain.ErrInvalidCursor},
		{
			name:      "storeError",
			req:       service.ConversationsRequest{IDYapeAccount: "acc-1"},
			wantQuery: &domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: domain.ConversationListSize},
			repoErr:   storeDown, wantErr: storeDown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := mocks.NewMessageRepository(t)
			if tt.wantQuery != nil {
				repo.On("ListConversations", mock.Anything, *tt.wantQuery).Return(tt.page, tt.repoErr).Once()
			}

			got, err := NewService(repo).Conversations(context.Background(), tt.req)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				if tt.wantErrText != "" {
					assert.Contains(t, err.Error(), tt.wantErrText)
				}
				assert.Equal(t, domain.ConversationList{}, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func ptr(s string) *string { return &s }

// A conversation is read for an account: with no account, or a malformed one, nothing is read, and the repository is never asked.
func TestMessages_TheAccountIsRequired(t *testing.T) {
	for name, account := range map[string]string{
		"missing":             "",
		"only spaces":         "   ",
		"with a space inside": "acc 1",
		"with a slash":        "acc/1",
		"with a nul":          "acc\x001",
		"with a line break":   "acc-1\n",
		"of 129 characters":   strings.Repeat("a", 129),
		"not ascii":           "cuenta-ñ",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewService(mocks.NewMessageRepository(t)).Messages(context.Background(),
				service.MessagesRequest{ConversationID: "conv_v1_a", IDYapeAccount: account})

			require.ErrorIs(t, err, domain.ErrInvalidRequest)
			assert.Contains(t, err.Error(), "idYapeAccount")
			assert.NotContains(t, err.Error(), "ñ", "the error does not echo what was sent")
		})
	}

	t.Run("an account of 128 characters is accepted and reaches the store", func(t *testing.T) {
		account := strings.Repeat("a", 128)
		repo := mocks.NewMessageRepository(t)
		repo.On("ListMessages", mock.Anything, domain.PageQuery{
			IDYapeAccount: account, ConversationID: "conv_v1_a", Direction: domain.DirectionNewest, Size: domain.PageSize,
		}).Return(domain.Page{}, domain.ErrNotFound).Once()

		_, err := NewService(repo).Messages(context.Background(), service.MessagesRequest{ConversationID: "conv_v1_a", IDYapeAccount: account})

		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("a conversation of another account is not found, as one that does not exist", func(t *testing.T) {
		repo := mocks.NewMessageRepository(t)
		repo.On("ListMessages", mock.Anything, mock.MatchedBy(func(q domain.PageQuery) bool { return q.IDYapeAccount == "acc-2" })).
			Return(domain.Page{}, domain.ErrNotFound).Once()

		_, err := NewService(repo).Messages(context.Background(), service.MessagesRequest{ConversationID: "conv_v1_a", IDYapeAccount: "acc-2"})

		require.ErrorIs(t, err, domain.ErrNotFound)
	})
}
