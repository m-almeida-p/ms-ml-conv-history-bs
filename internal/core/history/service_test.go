package history

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository/mocks"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
)

func messagesWithIDs(ids ...int64) []domain.Message {
	out := make([]domain.Message, 0, len(ids))
	for i, id := range ids {
		out = append(out, domain.Message{ConversationID: "conv", Sequence: i + 1, OutboxID: id})
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
			wantQuery: &domain.PageQuery{ConversationID: "conv_v1_a", Direction: domain.DirectionNewest},
			page:      domain.Page{Messages: messagesWithIDs(1042, 1043, 1044), HasMore: true},
			want: domain.MessagePage{
				ConversationID: "conv_v1_a", Direction: domain.DirectionNewest, Messages: messagesWithIDs(1042, 1043, 1044),
				HasMore: true, BeforeCursor: "1042", AfterCursor: "1044",
			},
		},
		{
			name:      "beforeAsksForOlderMessages",
			req:       service.MessagesRequest{ConversationID: "conv", Before: ptr("1042")},
			wantQuery: &domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionBefore, CursorID: 1042},
			page:      domain.Page{Messages: messagesWithIDs(1030, 1031)},
			want: domain.MessagePage{
				ConversationID: "conv", Direction: domain.DirectionBefore, Messages: messagesWithIDs(1030, 1031),
				BeforeCursor: "1030", AfterCursor: "1031",
			},
		},
		{
			name:      "afterAsksForNewerMessages",
			req:       service.MessagesRequest{ConversationID: "conv", After: ptr("7")},
			wantQuery: &domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionAfter, CursorID: 7},
			page:      domain.Page{Messages: messagesWithIDs(8), HasMore: true},
			want: domain.MessagePage{
				ConversationID: "conv", Direction: domain.DirectionAfter, Messages: messagesWithIDs(8),
				HasMore: true, BeforeCursor: "8", AfterCursor: "8",
			},
		},
		{
			name:      "emptyPageHasNoCursors",
			req:       service.MessagesRequest{ConversationID: "conv", After: ptr("9")},
			wantQuery: &domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionAfter, CursorID: 9},
			page:      domain.Page{},
			want:      domain.MessagePage{ConversationID: "conv", Direction: domain.DirectionAfter},
		},
		{
			name:      "cursorsAreTheLargestIDsToo",
			req:       service.MessagesRequest{ConversationID: "conv", Before: ptr("9223372036854775807")},
			wantQuery: &domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionBefore, CursorID: 9223372036854775807},
			page:      domain.Page{Messages: messagesWithIDs(1)},
			want:      domain.MessagePage{ConversationID: "conv", Direction: domain.DirectionBefore, Messages: messagesWithIDs(1), BeforeCursor: "1", AfterCursor: "1"},
		},
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
		{name: "cursorTooBig", req: service.MessagesRequest{ConversationID: "conv", After: ptr("9223372036854775808")}, wantErr: domain.ErrInvalidCursor},
		{
			name:      "unknownConversation",
			req:       service.MessagesRequest{ConversationID: "conv"},
			wantQuery: &domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest},
			repoErr:   domain.ErrNotFound, wantErr: domain.ErrNotFound,
		},
		{
			name:      "cursorOfAnotherConversation",
			req:       service.MessagesRequest{ConversationID: "conv", Before: ptr("3")},
			wantQuery: &domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionBefore, CursorID: 3},
			repoErr:   domain.ErrInvalidCursor, wantErr: domain.ErrInvalidCursor,
		},
		{
			name:      "storeErrorIsReturnedAsIs",
			req:       service.MessagesRequest{ConversationID: "conv"},
			wantQuery: &domain.PageQuery{ConversationID: "conv", Direction: domain.DirectionNewest},
			repoErr:   storeDown, wantErr: storeDown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := mocks.NewMessageRepository(t)
			if tt.wantQuery != nil {
				repo.On("ListMessages", mock.Anything, *tt.wantQuery).Return(tt.page, tt.repoErr).Once()
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

func TestConversations(t *testing.T) {
	storeDown := errors.New("connection reset")
	list := domain.ConversationList{Conversations: []domain.ConversationSummary{{ConversationID: "c", MessageCount: 2}}, HasMore: true}

	tests := []struct {
		name      string
		account   string
		callStore bool
		list      domain.ConversationList
		repoErr   error
		wantErr   error
	}{
		{name: "listOfTheAccount", account: "acc-1", callStore: true, list: list},
		{name: "accountWithNoConversationsIsAnEmptyList", account: "acc-2", callStore: true, list: domain.ConversationList{Conversations: []domain.ConversationSummary{}}},
		{name: "uuidAccount", account: "c59f8a80-6e65-4d1b-9c0e-0a1b2c3d4e5f", callStore: true, list: list},
		{name: "missingAccount", account: "", wantErr: domain.ErrInvalidRequest},
		{name: "accountWithSpaces", account: "acc 1", wantErr: domain.ErrInvalidRequest},
		{name: "accountWithSQLCharacters", account: "x'; drop table--", wantErr: domain.ErrInvalidRequest},
		{name: "accountTooLong", account: strings.Repeat("a", 129), wantErr: domain.ErrInvalidRequest},
		{name: "storeError", account: "acc-1", callStore: true, repoErr: storeDown, wantErr: storeDown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := mocks.NewMessageRepository(t)
			if tt.callStore {
				repo.On("ListConversations", mock.Anything, tt.account).Return(tt.list, tt.repoErr).Once()
			}

			got, err := NewService(repo).Conversations(context.Background(), tt.account)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Equal(t, domain.ConversationList{}, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.list, got)
		})
	}
}

func ptr(s string) *string { return &s }
