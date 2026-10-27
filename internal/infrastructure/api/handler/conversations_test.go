package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service/mocks"
)

func TestConversations(t *testing.T) {
	first := time.Date(2026, 10, 20, 14, 7, 23, 500_000_000, time.UTC) // 1792505243.5
	last := first.Add(5 * time.Minute)
	a := domain.ConversationSummary{ConversationID: "conv_v1_a", MessageCount: 5, FirstOccurredAt: first, LastOccurredAt: last}
	b := domain.ConversationSummary{ConversationID: "conv_v1_b", MessageCount: 1, FirstOccurredAt: first, LastOccurredAt: first}
	aJSON := `{"_id":"conv_v1_a","cursor":"1792505543500000~conv_v1_a","messageCount":5,"firstReceived":1792505243.5,"lastReceived":1792505543.5}`
	bJSON := `{"_id":"conv_v1_b","cursor":"1792505243500000~conv_v1_b","messageCount":1,"firstReceived":1792505243.5,"lastReceived":1792505243.5}`
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
				AfterCursor: "1792505543500000~conv_v1_a", BeforeCursor: "1792505243500000~conv_v1_b",
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
				AfterCursor: "1792505543500000~conv_v1_a", BeforeCursor: "1792505243500000~conv_v1_b",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[` + aJSON + `,` + bJSON + `]}`,
			wantLink:   `</v1/conversations?idYapeAccount=acc-1&page[before]=1792505243500000~conv_v1_b>; rel="prev"`,
		},
		{
			name:    "pageBeforeACursorPassesItAndLinksBack",
			target:  "/v1/conversations?idYapeAccount=acc-1&page%5Bbefore%5D=1792505243500000~conv_v1_z",
			wantReq: service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr("1792505243500000~conv_v1_z")},
			list: domain.ConversationList{
				Direction: domain.DirectionBefore, Size: size, Conversations: []domain.ConversationSummary{b}, HasMore: true,
				AfterCursor: "1792505243500000~conv_v1_b", BeforeCursor: "1792505243500000~conv_v1_b",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[` + bJSON + `]}`,
			wantLink:   `</v1/conversations?idYapeAccount=acc-1&page[before]=1792505243500000~conv_v1_b>; rel="prev"`,
		},
		{
			name:    "pageAfterACursorWithMoreRecentOnesLinksForwardFromItsMostRecentOne",
			target:  "/v1/conversations?idYapeAccount=acc-1&page[after]=1792505243500000~conv_v1_z",
			wantReq: service.ConversationsRequest{IDYapeAccount: "acc-1", After: ptr("1792505243500000~conv_v1_z")},
			list: domain.ConversationList{
				Direction: domain.DirectionAfter, Size: size, Conversations: []domain.ConversationSummary{a, b}, HasMore: true,
				AfterCursor: "1792505543500000~conv_v1_a", BeforeCursor: "1792505243500000~conv_v1_b",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[` + aJSON + `,` + bJSON + `]}`,
			wantLink:   `</v1/conversations?idYapeAccount=acc-1&page[after]=1792505543500000~conv_v1_a>; rel="next"`,
		},
		{
			name:    "aPageSizeIsPassedOnAndKeptInTheLink",
			target:  "/v1/conversations?idYapeAccount=acc-1&page[size]=1",
			wantReq: service.ConversationsRequest{IDYapeAccount: "acc-1", Size: ptr("1")},
			list: domain.ConversationList{
				Direction: domain.DirectionNewest, Size: 1, Conversations: []domain.ConversationSummary{a}, HasMore: true,
				AfterCursor: "1792505543500000~conv_v1_a", BeforeCursor: "1792505543500000~conv_v1_a",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[` + aJSON + `]}`,
			wantLink:   `</v1/conversations?idYapeAccount=acc-1&page[before]=1792505543500000~conv_v1_a&page[size]=1>; rel="prev"`,
		},
		{
			name:    "aPageSizeWithAnAfterCursorKeepsTheSizeInTheNextLink",
			target:  "/v1/conversations?idYapeAccount=acc-1&page[after]=1792505243500000~conv_v1_z&page[size]=1",
			wantReq: service.ConversationsRequest{IDYapeAccount: "acc-1", After: ptr("1792505243500000~conv_v1_z"), Size: ptr("1")},
			list: domain.ConversationList{
				Direction: domain.DirectionAfter, Size: 1, Conversations: []domain.ConversationSummary{b}, HasMore: true,
				AfterCursor: "1792505243500000~conv_v1_b", BeforeCursor: "1792505243500000~conv_v1_b",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[` + bJSON + `]}`,
			wantLink:   `</v1/conversations?idYapeAccount=acc-1&page[after]=1792505243500000~conv_v1_b&page[size]=1>; rel="next"`,
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
			target:     "/v1/conversations?idYapeAccount=acc-1&page[before]=1792505243500000~conv_v1_b",
			wantReq:    service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr("1792505243500000~conv_v1_b")},
			list:       domain.ConversationList{Direction: domain.DirectionBefore, Size: size},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversations":[]}`,
		},
		{
			name:       "missingAccountIsABadRequest",
			target:     "/v1/conversations",
			wantReq:    service.ConversationsRequest{},
			err:        domain.InvalidRequest("idYapeAccount is missing or malformed"),
			wantStatus: http.StatusBadRequest,
			wantJSON:   problemJSON(400, "invalid-request", "Invalid request", "idYapeAccount is missing or malformed"),
		},
		{
			name:       "aValueThatIsNotACursorIsAnInvalidCursorThatDoesNotEchoIt",
			target:     "/v1/conversations?idYapeAccount=acc-1&page[before]=theirs",
			wantReq:    service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr("theirs")},
			err:        domain.ErrInvalidCursor,
			wantStatus: http.StatusBadRequest,
			wantJSON:   problemJSON(400, "invalid-cursor", "Invalid cursor", "page[before] or page[after] is not a cursor from the conversation list."),
		},
		{
			name:       "bothCursorsAreABadRequest",
			target:     "/v1/conversations?idYapeAccount=acc-1&page[before]=x&page[after]=y",
			wantReq:    service.ConversationsRequest{IDYapeAccount: "acc-1", Before: ptr("x"), After: ptr("y")},
			err:        domain.InvalidRequest("page[before] and page[after] cannot be used together"),
			wantStatus: http.StatusBadRequest,
			wantJSON:   problemJSON(400, "invalid-request", "Invalid request", "page[before] and page[after] cannot be used together"),
		},
		{
			name:       "aBadPageSizeIsABadRequest",
			target:     "/v1/conversations?idYapeAccount=acc-1&page[size]=101",
			wantReq:    service.ConversationsRequest{IDYapeAccount: "acc-1", Size: ptr("101")},
			err:        domain.InvalidRequest("page[size] must be a whole number from 1 to 100"),
			wantStatus: http.StatusBadRequest,
			wantJSON:   problemJSON(400, "invalid-request", "Invalid request", "page[size] must be a whole number from 1 to 100"),
		},
		{
			name:       "storeErrorIsAGenericServerError",
			target:     "/v1/conversations?idYapeAccount=acc-1",
			wantReq:    service.ConversationsRequest{IDYapeAccount: "acc-1"},
			err:        errors.New("connection reset"),
			wantStatus: http.StatusInternalServerError,
			wantJSON:   problemJSON(500, "", "Internal Server Error", "Something went wrong. Try again."),
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
			assert.Contains(t, rec.Body.String(), `"type":"`+problemTypeBase+`invalid-request"`)
			assert.Contains(t, rec.Body.String(), tt.want)
		})
	}
}

func TestAnEmptyConversationCursorOrSizeReachesTheServiceSoItIsRefused(t *testing.T) {
	history := mocks.NewHistory(t)
	history.On("Conversations", mock.Anything, service.ConversationsRequest{IDYapeAccount: "a", Before: ptr(""), Size: ptr("")}).
		Return(domain.ConversationList{}, domain.InvalidRequest("page[size] must be a whole number from 1 to 100")).Once()

	rec := serve(t, history, &recordingLog{}, "/v1/conversations?idYapeAccount=a&page[before]=&page[size]=")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestEachConversationCarriesItsOwnCursor(t *testing.T) {
	last := time.Date(2026, 10, 27, 14, 7, 23, 792_123_000, time.UTC)
	history := mocks.NewHistory(t)
	history.On("Conversations", mock.Anything, service.ConversationsRequest{IDYapeAccount: "acc-1"}).Return(domain.ConversationList{
		Direction: domain.DirectionNewest, Size: domain.ConversationListSize,
		Conversations: []domain.ConversationSummary{{ConversationID: "conv_v1_a", MessageCount: 3, FirstOccurredAt: last, LastOccurredAt: last}},
	}, nil).Once()

	rec := serve(t, history, &recordingLog{}, "/v1/conversations?idYapeAccount=acc-1")

	var body struct {
		Conversations []struct {
			ID     string `json:"_id"`
			Cursor string `json:"cursor"`
		} `json:"conversations"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Conversations, 1)
	parsed, err := domain.ParseConversationCursor(body.Conversations[0].Cursor)
	require.NoError(t, err, "the cursor is one the service accepts back")
	assert.True(t, parsed.LastOccurredAt.Equal(last), "it holds the position of the conversation when it was listed, to the microsecond")
	assert.Equal(t, "conv_v1_a", parsed.ConversationID)
}

func TestNoLinkIsBuiltFromAnAccountOrIDThatIsNotSafeInAURL(t *testing.T) {
	list := domain.ConversationList{Direction: domain.DirectionNewest, Size: 5, HasMore: true, AfterCursor: "5~n", BeforeCursor: "5~o"}
	for _, account := range []string{"", "acc 1", "acc&page[size]=1", "acc\r\nX-Injected: 1", "acc#x", "a/b", "é"} {
		assert.Empty(t, conversationsContinuation(account, list), "account %q", account)
	}

	page := domain.MessagePage{Direction: domain.DirectionNewest, Size: 5, HasMore: true, BeforeCursor: "3", AfterCursor: "8"}
	for _, id := range []string{"", "conv a", "c&page[size]=1", "c\r\nX-Injected: 1", "c#x", "a/b"} {
		page.ConversationID = id
		assert.Empty(t, continuation(page), "conversation id %q", id)
	}
}
