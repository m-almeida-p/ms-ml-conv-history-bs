package handler

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service/mocks"
)

func TestConversations(t *testing.T) {
	first := time.Date(2026, 10, 20, 14, 7, 23, 500_000_000, time.UTC) // 1792505243.5
	last := first.Add(5 * time.Minute)
	a := domain.ConversationSummary{ConversationID: "conv_v1_a", MessageCount: 5, FirstOccurredAt: first, LastOccurredAt: last}
	b := domain.ConversationSummary{ConversationID: "conv_v1_b", MessageCount: 1, FirstOccurredAt: first, LastOccurredAt: first}
	aJSON := `{"_id":"conv_v1_a","messageCount":5,"firstReceived":1792505243.5,"lastReceived":1792505543.5}`
	bJSON := `{"_id":"conv_v1_b","messageCount":1,"firstReceived":1792505243.5,"lastReceived":1792505243.5}`
	size := domain.ConversationListSize

	tests := []struct {
		name       string
		target     string
		wantReq    service.ConversationsRequest
		list       domain.ConversationList
		err        error
		wantStatus int
		wantJSON   string
		wantLink   string // the Link header, empty when there must be none
		wantLogged bool
	}{
		{
			name:    "conversationsOfAnAccountWithNothingMoreHaveNoLink",
			target:  "/v1/conversations?idYapeAccount=acc-1",
			wantReq: service.ConversationsRequest{IDYapeAccount: "acc-1"},
			list: domain.ConversationList{
				Direction: domain.DirectionNewest, Size: size, Conversations: []domain.ConversationSummary{a, b},
				AfterCursor: "conv_v1_a", BeforeCursor: "conv_v1_b",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[` + aJSON + `,` + bJSON + `]}`,
		},
		{
			name:    "mostRecentPageWithOlderConversationsLinksBackFromItsOldestOne",
			target:  "/v1/conversations?idYapeAccount=acc-1",
			wantReq: service.ConversationsRequest{IDYapeAccount: "acc-1"},
			list: domain.ConversationList{
				Direction: domain.DirectionNewest, Size: size, Conversations: []domain.ConversationSummary{a, b}, HasMore: true,
				AfterCursor: "conv_v1_a", BeforeCursor: "conv_v1_b",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[` + aJSON + `,` + bJSON + `]}`,
			wantLink:   `</v1/conversations?idYapeAccount=acc-1&page[before]=conv_v1_b>; rel="prev"`,
		},
		{
			name:    "pageBeforeACursorPassesItAndLinksBack",
			target:  "/v1/conversations?idYapeAccount=acc-1&page%5Bbefore%5D=conv_v1_z",
			wantReq: service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr("conv_v1_z")},
			list: domain.ConversationList{
				Direction: domain.DirectionBefore, Size: size, Conversations: []domain.ConversationSummary{b}, HasMore: true,
				AfterCursor: "conv_v1_b", BeforeCursor: "conv_v1_b",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[` + bJSON + `]}`,
			wantLink:   `</v1/conversations?idYapeAccount=acc-1&page[before]=conv_v1_b>; rel="prev"`,
		},
		{
			name:    "pageAfterACursorWithMoreRecentOnesLinksForwardFromItsMostRecentOne",
			target:  "/v1/conversations?idYapeAccount=acc-1&page[after]=conv_v1_z",
			wantReq: service.ConversationsRequest{IDYapeAccount: "acc-1", After: ptr("conv_v1_z")},
			list: domain.ConversationList{
				Direction: domain.DirectionAfter, Size: size, Conversations: []domain.ConversationSummary{a, b}, HasMore: true,
				AfterCursor: "conv_v1_a", BeforeCursor: "conv_v1_b",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[` + aJSON + `,` + bJSON + `]}`,
			wantLink:   `</v1/conversations?idYapeAccount=acc-1&page[after]=conv_v1_a>; rel="next"`,
		},
		{
			name:    "aPageSizeIsPassedOnAndKeptInTheLink",
			target:  "/v1/conversations?idYapeAccount=acc-1&page[size]=1",
			wantReq: service.ConversationsRequest{IDYapeAccount: "acc-1", Size: ptr("1")},
			list: domain.ConversationList{
				Direction: domain.DirectionNewest, Size: 1, Conversations: []domain.ConversationSummary{a}, HasMore: true,
				AfterCursor: "conv_v1_a", BeforeCursor: "conv_v1_a",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[` + aJSON + `]}`,
			wantLink:   `</v1/conversations?idYapeAccount=acc-1&page[before]=conv_v1_a&page[size]=1>; rel="prev"`,
		},
		{
			name:    "aPageSizeWithAnAfterCursorKeepsTheSizeInTheNextLink",
			target:  "/v1/conversations?idYapeAccount=acc-1&page[after]=conv_v1_z&page[size]=1",
			wantReq: service.ConversationsRequest{IDYapeAccount: "acc-1", After: ptr("conv_v1_z"), Size: ptr("1")},
			list: domain.ConversationList{
				Direction: domain.DirectionAfter, Size: 1, Conversations: []domain.ConversationSummary{b}, HasMore: true,
				AfterCursor: "conv_v1_b", BeforeCursor: "conv_v1_b",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[` + bJSON + `]}`,
			wantLink:   `</v1/conversations?idYapeAccount=acc-1&page[after]=conv_v1_b&page[size]=1>; rel="next"`,
		},
		{
			name:       "accountWithNoConversationsGetsAnEmptyArrayAndNoLink",
			target:     "/v1/conversations?idYapeAccount=acc-2",
			wantReq:    service.ConversationsRequest{IDYapeAccount: "acc-2"},
			list:       domain.ConversationList{Direction: domain.DirectionNewest, Size: size},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[]}`,
		},
		{
			name:       "emptyPageBesideACursorIsAnEmptyArray",
			target:     "/v1/conversations?idYapeAccount=acc-1&page[before]=conv_v1_b",
			wantReq:    service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr("conv_v1_b")},
			list:       domain.ConversationList{Direction: domain.DirectionBefore, Size: size},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[]}`,
		},
		{
			name:       "missingAccountIsABadRequest",
			target:     "/v1/conversations",
			wantReq:    service.ConversationsRequest{},
			err:        fmt.Errorf("%w: idYapeAccount is missing or malformed", domain.ErrInvalidRequest),
			wantStatus: http.StatusBadRequest,
			wantJSON:   `{"error":{"code":"invalid_request","description":"invalid request: idYapeAccount is missing or malformed"}}`,
		},
		{
			name:       "aCursorOfAnotherAccountIsAnInvalidCursorThatDoesNotEchoIt",
			target:     "/v1/conversations?idYapeAccount=acc-1&page[before]=theirs",
			wantReq:    service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr("theirs")},
			err:        domain.ErrInvalidCursor,
			wantStatus: http.StatusBadRequest,
			wantJSON:   `{"error":{"code":"invalid_cursor","description":"page[before] or page[after] is not the _id of a conversation of this account."}}`,
		},
		{
			name:       "bothCursorsAreABadRequest",
			target:     "/v1/conversations?idYapeAccount=acc-1&page[before]=x&page[after]=y",
			wantReq:    service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr("x"), After: ptr("y")},
			err:        fmt.Errorf("%w: page[before] and page[after] cannot be used together", domain.ErrInvalidRequest),
			wantStatus: http.StatusBadRequest,
			wantJSON:   `{"error":{"code":"invalid_request","description":"invalid request: page[before] and page[after] cannot be used together"}}`,
		},
		{
			name:       "aBadPageSizeIsABadRequest",
			target:     "/v1/conversations?idYapeAccount=acc-1&page[size]=101",
			wantReq:    service.ConversationsRequest{IDYapeAccount: "acc-1", Size: ptr("101")},
			err:        fmt.Errorf("%w: page[size] must be a whole number from 1 to 100", domain.ErrInvalidRequest),
			wantStatus: http.StatusBadRequest,
			wantJSON:   `{"error":{"code":"invalid_request","description":"invalid request: page[size] must be a whole number from 1 to 100"}}`,
		},
		{
			name:       "storeErrorIsAGenericServerError",
			target:     "/v1/conversations?idYapeAccount=acc-1",
			wantReq:    service.ConversationsRequest{IDYapeAccount: "acc-1"},
			err:        errors.New("connection reset"),
			wantStatus: http.StatusInternalServerError,
			wantJSON:   `{"error":{"code":"internal_error","description":"Something went wrong. Try again."}}`,
			wantLogged: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := mocks.NewHistory(t)
			history.On("Conversations", mock.Anything, tt.wantReq).Return(tt.list, tt.err).Once()
			log := &recordingLog{}

			rec := serve(t, history, log, tt.target)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.JSONEq(t, tt.wantJSON, rec.Body.String())
			assert.Equal(t, tt.wantLink, rec.Header().Get("Link"))
			assert.NotContains(t, rec.Body.String(), `"meta"`, "paging lives in the Link header, not the body")
			assert.NotContains(t, rec.Body.String(), `"links"`, "paging lives in the Link header, not the body")
			assert.Equal(t, tt.wantLogged, log.text() != "")
			assert.NotContains(t, log.text(), "acc-1", "the log has no account id")
			assert.NotContains(t, log.text(), "theirs", "the log has no cursor")
		})
	}
}

func TestTheConversationLinkKeepsTheAccountAndEveryCharacterAnIDMayHold(t *testing.T) {
	list := domain.ConversationList{
		Direction: domain.DirectionNewest, Size: 7, HasMore: true,
		Conversations: []domain.ConversationSummary{{ConversationID: "conv.A_b:c-9"}, {ConversationID: "conv.Z_y:x-0"}},
		AfterCursor:   "conv.A_b:c-9", BeforeCursor: "conv.Z_y:x-0",
	}

	link := conversationsContinuation("acc.1_x:y-2", list)

	assert.Equal(t, `</v1/conversations?idYapeAccount=acc.1_x:y-2&page[before]=conv.Z_y:x-0&page[size]=7>; rel="prev"`, link)
	assert.NotContains(t, link, "\n")
}

func TestTheConversationLinkOffersOneRelationPerDirection(t *testing.T) {
	for _, tt := range []struct {
		direction domain.Direction
		want      string
		notWant   string
	}{
		{domain.DirectionNewest, `rel="prev"`, `rel="next"`},
		{domain.DirectionBefore, `rel="prev"`, `rel="next"`},
		{domain.DirectionAfter, `rel="next"`, `rel="prev"`},
	} {
		link := conversationsContinuation("a", domain.ConversationList{Direction: tt.direction, Size: domain.ConversationListSize, HasMore: true, AfterCursor: "n", BeforeCursor: "o"})

		assert.Contains(t, link, tt.want, string(tt.direction))
		assert.NotContains(t, link, tt.notWant, string(tt.direction))
	}
}

func TestTheConversationLinkRepeatsTheSizeOnlyWhenItIsNotTheDefault(t *testing.T) {
	list := domain.ConversationList{Direction: domain.DirectionNewest, HasMore: true, AfterCursor: "n", BeforeCursor: "o"}

	for _, tt := range []struct {
		size int
		want string
	}{
		{domain.ConversationListSize, `</v1/conversations?idYapeAccount=a&page[before]=o>; rel="prev"`},
		{domain.ConversationListSize - 1, `</v1/conversations?idYapeAccount=a&page[before]=o&page[size]=99>; rel="prev"`},
		{1, `</v1/conversations?idYapeAccount=a&page[before]=o&page[size]=1>; rel="prev"`},
	} {
		list.Size = tt.size
		assert.Equal(t, tt.want, conversationsContinuation("a", list), "size %d", tt.size)
	}
}

func TestNoConversationLinkWhenThereIsNothingMore(t *testing.T) {
	for _, direction := range []domain.Direction{domain.DirectionNewest, domain.DirectionBefore, domain.DirectionAfter} {
		assert.Empty(t, conversationsContinuation("a", domain.ConversationList{Direction: direction, Size: 5, AfterCursor: "n", BeforeCursor: "o"}), string(direction))
	}
}

func TestRepeatedParametersOfTheConversationListAreRefused(t *testing.T) {
	for _, tt := range []struct{ name, target, want string }{
		{"beforeTwice", "/v1/conversations?idYapeAccount=a&page[before]=1&page[before]=2", "page[before] is given more than once"},
		{"afterTwice", "/v1/conversations?idYapeAccount=a&page[after]=1&page[after]=2", "page[after] is given more than once"},
		{"sizeTwice", "/v1/conversations?idYapeAccount=a&page[size]=1&page[size]=2", "page[size] is given more than once"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			history := mocks.NewHistory(t) // no call is expected: the service never sees the request

			rec := serve(t, history, &recordingLog{}, tt.target)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), `"code":"invalid_request"`)
			assert.Contains(t, rec.Body.String(), tt.want)
		})
	}
}

func TestAnEmptyConversationCursorOrSizeReachesTheServiceSoItIsRefused(t *testing.T) {
	history := mocks.NewHistory(t)
	history.On("Conversations", mock.Anything, service.ConversationsRequest{IDYapeAccount: "a", Before: ptr(""), Size: ptr("")}).
		Return(domain.ConversationList{}, fmt.Errorf("%w: page[size] must be a whole number from 1 to 100", domain.ErrInvalidRequest)).Once()

	rec := serve(t, history, &recordingLog{}, "/v1/conversations?idYapeAccount=a&page[before]=&page[size]=")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
