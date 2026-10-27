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

func TestMessages(t *testing.T) {
	received := time.Date(2026, 10, 20, 14, 7, 23, 792_000_000, time.UTC) // 1792505243.792
	user := domain.Message{
		ConversationID: "conv_v1_a", Sequence: 2, Role: domain.RoleUser, AuthorID: "acc-1", Type: "text",
		Text: "quiero yapear", EventName: "UserMessageSent", OccurredAt: received,
	}
	agent := domain.Message{
		ConversationID: "conv_v1_a", Sequence: 3, Role: domain.RoleMaker, AuthorID: "ms-ml-genai-agent-bs", Type: "text",
		Text: "¿A quién?", Truncated: true, EventName: "AgentMessageSent", Flow: "p2p", OccurredAt: received,
	}
	storeDown := errors.New("connection reset: password=hunter2")

	userJSON := `{"_id":"2","type":"text","role":"appUser","authorId":"acc-1","text":"quiero yapear","received":1792505243.792,
		"source":{"type":"api"},"metadata":{"sequence":2,"eventName":"UserMessageSent","truncated":false}}`
	agentJSON := `{"_id":"3","type":"text","role":"appMaker","authorId":"ms-ml-genai-agent-bs","text":"¿A quién?","received":1792505243.792,
		"source":{"type":"api"},"metadata":{"sequence":3,"eventName":"AgentMessageSent","truncated":true,"flow":"p2p"}}`

	tests := []struct {
		name       string
		target     string
		wantReq    service.MessagesRequest
		page       domain.MessagePage
		err        error
		wantStatus int
		wantJSON   string
		wantLink   string // the Link header, empty when there must be none
		wantLogged bool
	}{
		{
			name:    "newestPageWithMoreOlderMessagesLinksBackInTheHeader",
			target:  "/v1/conversations/conv_v1_a/messages",
			wantReq: service.MessagesRequest{ConversationID: "conv_v1_a"},
			page: domain.MessagePage{
				ConversationID: "conv_v1_a", Direction: domain.DirectionNewest, Size: domain.PageSize, Messages: []domain.Message{user, agent},
				HasMore: true, BeforeCursor: "2", AfterCursor: "3",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversation":{"_id":"conv_v1_a"},"messages":[` + userJSON + `,` + agentJSON + `]}`,
			wantLink:   `</v1/conversations/conv_v1_a/messages?page[before]=2>; rel="prev"`,
		},
		{
			name:    "pageBeforeACursorPassesItAndLinksBack",
			target:  "/v1/conversations/conv_v1_a/messages?page%5Bbefore%5D=2",
			wantReq: service.MessagesRequest{ConversationID: "conv_v1_a", Before: ptr("2")},
			page: domain.MessagePage{
				ConversationID: "conv_v1_a", Direction: domain.DirectionBefore, Size: domain.PageSize, Messages: []domain.Message{user},
				HasMore: true, BeforeCursor: "2", AfterCursor: "2",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversation":{"_id":"conv_v1_a"},"messages":[` + userJSON + `]}`,
			wantLink:   `</v1/conversations/conv_v1_a/messages?page[before]=2>; rel="prev"`,
		},
		{
			name:    "pageAfterACursorWithMoreNewerMessagesLinksForward",
			target:  "/v1/conversations/conv_v1_a/messages?page[after]=1040",
			wantReq: service.MessagesRequest{ConversationID: "conv_v1_a", After: ptr("1040")},
			page: domain.MessagePage{
				ConversationID: "conv_v1_a", Direction: domain.DirectionAfter, Size: domain.PageSize, Messages: []domain.Message{agent},
				HasMore: true, BeforeCursor: "3", AfterCursor: "3",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversation":{"_id":"conv_v1_a"},"messages":[` + agentJSON + `]}`,
			wantLink:   `</v1/conversations/conv_v1_a/messages?page[after]=3>; rel="next"`,
		},
		{
			name:    "aPageSizeBelowTheDefaultIsPassedOnAndKeptInTheLink",
			target:  "/v1/conversations/conv_v1_a/messages?page%5Bsize%5D=1",
			wantReq: service.MessagesRequest{ConversationID: "conv_v1_a", Size: ptr("1")},
			page: domain.MessagePage{
				ConversationID: "conv_v1_a", Direction: domain.DirectionNewest, Size: 1, Messages: []domain.Message{agent},
				HasMore: true, BeforeCursor: "3", AfterCursor: "3",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversation":{"_id":"conv_v1_a"},"messages":[` + agentJSON + `]}`,
			wantLink:   `</v1/conversations/conv_v1_a/messages?page[before]=3&page[size]=1>; rel="prev"`,
		},
		{
			name:    "aPageSizeWithAnAfterCursorKeepsTheSizeInTheNextLink",
			target:  "/v1/conversations/conv_v1_a/messages?page[after]=1&page[size]=1",
			wantReq: service.MessagesRequest{ConversationID: "conv_v1_a", After: ptr("1"), Size: ptr("1")},
			page: domain.MessagePage{
				ConversationID: "conv_v1_a", Direction: domain.DirectionAfter, Size: 1, Messages: []domain.Message{user},
				HasMore: true, BeforeCursor: "2", AfterCursor: "2",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversation":{"_id":"conv_v1_a"},"messages":[` + userJSON + `]}`,
			wantLink:   `</v1/conversations/conv_v1_a/messages?page[after]=2&page[size]=1>; rel="next"`,
		},
		{
			name:    "lastPageHasNoLinkHeader",
			target:  "/v1/conversations/conv_v1_a/messages",
			wantReq: service.MessagesRequest{ConversationID: "conv_v1_a"},
			page: domain.MessagePage{
				ConversationID: "conv_v1_a", Direction: domain.DirectionNewest, Size: domain.PageSize, Messages: []domain.Message{user},
				BeforeCursor: "2", AfterCursor: "2",
			},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversation":{"_id":"conv_v1_a"},"messages":[` + userJSON + `]}`,
		},
		{
			name:       "emptyPageIsAnEmptyArrayWithoutALink",
			target:     "/v1/conversations/conv_v1_a/messages?page[after]=3",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_a", After: ptr("3")},
			page:       domain.MessagePage{ConversationID: "conv_v1_a", Direction: domain.DirectionAfter, Size: domain.PageSize},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversation":{"_id":"conv_v1_a"},"messages":[]}`,
		},
		{
			name:       "aBadPageSizeIsABadRequest",
			target:     "/v1/conversations/conv_v1_a/messages?page[size]=101",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_a", Size: ptr("101")},
			err:        domain.InvalidRequest("page[size] must be a whole number from 1 to 100"),
			wantStatus: http.StatusBadRequest,
			wantJSON:   problemJSON(400, "invalid-request", "Invalid request", "page[size] must be a whole number from 1 to 100"),
		},
		{
			name:       "anEmptyCursorIsPassedOnSoItIsRejectedAsACursor",
			target:     "/v1/conversations/conv_v1_a/messages?page[before]=",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_a", Before: ptr("")},
			err:        fmt.Errorf("%w: the cursor is not a message _id", domain.ErrInvalidCursor),
			wantStatus: http.StatusBadRequest,
			wantJSON:   problemJSON(400, "invalid-cursor", "Invalid cursor", "page[before] or page[after] is not the _id of a message in this conversation."),
		},
		{
			name:       "anEmptyCursorBesideARealOneReachesTheBothCursorsRule",
			target:     "/v1/conversations/conv_v1_a/messages?page[before]=&page[after]=7",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_a", Before: ptr(""), After: ptr("7")},
			err:        domain.InvalidRequest("page[before] and page[after] cannot be used together"),
			wantStatus: http.StatusBadRequest,
			wantJSON:   problemJSON(400, "invalid-request", "Invalid request", "page[before] and page[after] cannot be used together"),
		},
		{
			name:       "bothCursorsAreABadRequest",
			target:     "/v1/conversations/conv_v1_a/messages?page[before]=1&page[after]=2",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_a", Before: ptr("1"), After: ptr("2")},
			err:        domain.InvalidRequest("page[before] and page[after] cannot be used together"),
			wantStatus: http.StatusBadRequest,
			wantJSON:   problemJSON(400, "invalid-request", "Invalid request", "page[before] and page[after] cannot be used together"),
		},
		{
			name:       "malformedConversationIDIsABadRequest",
			target:     "/v1/conversations/bad%20id/messages",
			wantReq:    service.MessagesRequest{ConversationID: "bad id"},
			err:        domain.InvalidRequest("the conversation id is malformed"),
			wantStatus: http.StatusBadRequest,
			wantJSON:   problemJSON(400, "invalid-request", "Invalid request", "the conversation id is malformed"),
		},
		{
			name:       "badCursorIsABadRequestThatDoesNotEchoIt",
			target:     "/v1/conversations/conv_v1_a/messages?page[before]=nope",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_a", Before: ptr("nope")},
			err:        fmt.Errorf("%w: the cursor is not a message _id", domain.ErrInvalidCursor),
			wantStatus: http.StatusBadRequest,
			wantJSON:   problemJSON(400, "invalid-cursor", "Invalid cursor", "page[before] or page[after] is not the _id of a message in this conversation."),
		},
		{
			name:       "unknownConversationIsNotFound",
			target:     "/v1/conversations/conv_v1_zzz/messages",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_zzz"},
			err:        domain.ErrNotFound,
			wantStatus: http.StatusNotFound,
			wantJSON:   problemJSON(404, "conversation-not-found", "Conversation not found", "No chat messages were found for this conversation."),
		},
		{
			name:       "storeErrorIsAGenericServerErrorThatIsLogged",
			target:     "/v1/conversations/conv_v1_a/messages",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_a"},
			err:        storeDown,
			wantStatus: http.StatusInternalServerError,
			wantJSON:   problemJSON(500, "", "Internal Server Error", "Something went wrong. Try again."),
			wantLogged: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := mocks.NewHistory(t)
			history.On("Messages", mock.Anything, tt.wantReq).Return(tt.page, tt.err).Once()
			log := &recordingLog{}

			rec := serve(t, history, log, tt.target)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, wantContentType(tt.wantStatus), contentType(rec))
			assert.JSONEq(t, tt.wantJSON, rec.Body.String())
			assert.Equal(t, tt.wantLink, rec.Header().Get("Link"))
			assert.NotContains(t, rec.Body.String(), `"meta"`, "paging lives in the Link header, not the body")
			assert.NotContains(t, rec.Body.String(), `"links"`, "paging lives in the Link header, not the body")
			assert.Equal(t, tt.wantLogged, log.text() != "")
			assert.NotContains(t, rec.Body.String(), "hunter2", "a server error never shows its cause")
			assert.NotContains(t, log.text(), "quiero yapear", "the log has no message text")
		})
	}
}

func TestMessagesLogHasTheRouteNotTheValues(t *testing.T) {
	history := mocks.NewHistory(t)
	history.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{}, errors.New("boom")).Once()
	log := &recordingLog{}

	serve(t, history, log, "/v1/conversations/conv_secret_id/messages?page[before]=987654")

	assert.Contains(t, log.text(), "/v1/conversations/:conversationId/messages")
	assert.NotContains(t, log.text(), "conv_secret_id")
	assert.NotContains(t, log.text(), "987654")
}

func ptr(s string) *string { return &s }

func TestRepeatedParametersAreAmbiguousAndRefused(t *testing.T) {
	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "beforeTwice", target: "/v1/conversations/conv_v1_a/messages?page[before]=1&page[before]=2", want: "page[before] is given more than once"},
		{name: "afterTwice", target: "/v1/conversations/conv_v1_a/messages?page[after]=1&page[after]=2", want: "page[after] is given more than once"},
		{name: "sizeTwice", target: "/v1/conversations/conv_v1_a/messages?page[size]=1&page[size]=2", want: "page[size] is given more than once"},
		{name: "accountTwice", target: "/v1/conversations?idYapeAccount=a&idYapeAccount=b", want: "idYapeAccount is given more than once"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := mocks.NewHistory(t) // no call is expected: the service never sees the request

			rec := serve(t, history, &recordingLog{}, tt.target)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), `"type":"`+problemTypeBase+`invalid-request"`)
			assert.Contains(t, rec.Body.String(), tt.want)
		})
	}
}

func TestUnknownParametersAreIgnored(t *testing.T) {
	history := mocks.NewHistory(t)
	history.On("Messages", mock.Anything, service.MessagesRequest{ConversationID: "conv_v1_a"}).
		Return(domain.MessagePage{ConversationID: "conv_v1_a"}, nil).Once()

	rec := serve(t, history, &recordingLog{}, "/v1/conversations/conv_v1_a/messages?page[number]=5&foo=bar")

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAnEmptyPageSizeReachesTheServiceSoItIsRefused(t *testing.T) {
	history := mocks.NewHistory(t)
	history.On("Messages", mock.Anything, service.MessagesRequest{ConversationID: "conv_v1_a", Size: ptr("")}).
		Return(domain.MessagePage{}, domain.InvalidRequest("page[size] must be a whole number from 1 to 100")).Once()

	rec := serve(t, history, &recordingLog{}, "/v1/conversations/conv_v1_a/messages?page[size]=")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), `"type":"`+problemTypeBase+`invalid-request"`)
}

func TestTheLinkKeepsEveryCharacterAConversationIDMayHold(t *testing.T) {
	const id = "conv.A_b:c-9"
	history := mocks.NewHistory(t)
	history.On("Messages", mock.Anything, service.MessagesRequest{ConversationID: id}).Return(domain.MessagePage{
		ConversationID: id, Direction: domain.DirectionNewest, Size: domain.PageSize, HasMore: true, BeforeCursor: "7", AfterCursor: "9",
	}, nil).Once()

	rec := serve(t, history, &recordingLog{}, "/v1/conversations/"+id+"/messages")

	assert.Equal(t, `</v1/conversations/conv.A_b:c-9/messages?page[before]=7>; rel="prev"`, rec.Header().Get("Link"))
	assert.Len(t, rec.Header().Values("Link"), 1, "one header, not one per relation")
	assert.NotContains(t, rec.Header().Get("Link"), "\n", "a header value never holds a line break")
}

func TestTheLinkOfAnAfterPageNeverOffersPrevAndTheOtherWayRound(t *testing.T) {
	tests := []struct {
		name      string
		direction domain.Direction
		wantRel   string
		notRel    string
	}{
		{name: "newestOffersPrev", direction: domain.DirectionNewest, wantRel: `rel="prev"`, notRel: `rel="next"`},
		{name: "beforeOffersPrev", direction: domain.DirectionBefore, wantRel: `rel="prev"`, notRel: `rel="next"`},
		{name: "afterOffersNext", direction: domain.DirectionAfter, wantRel: `rel="next"`, notRel: `rel="prev"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			link := continuation(domain.MessagePage{ConversationID: "c", Direction: tt.direction, Size: domain.PageSize, HasMore: true, BeforeCursor: "3", AfterCursor: "8"})

			assert.Contains(t, link, tt.wantRel)
			assert.NotContains(t, link, tt.notRel)
		})
	}
}

func TestTheLinkRepeatsTheSizeOnlyWhenItIsNotTheDefault(t *testing.T) {
	page := domain.MessagePage{ConversationID: "c", Direction: domain.DirectionNewest, HasMore: true, BeforeCursor: "3", AfterCursor: "8"}

	for _, tt := range []struct {
		size int
		want string
	}{
		{size: domain.PageSize, want: `</v1/conversations/c/messages?page[before]=3>; rel="prev"`},
		{size: domain.PageSize - 1, want: `</v1/conversations/c/messages?page[before]=3&page[size]=99>; rel="prev"`},
		{size: 1, want: `</v1/conversations/c/messages?page[before]=3&page[size]=1>; rel="prev"`},
	} {
		page.Size = tt.size
		assert.Equal(t, tt.want, continuation(page), "size %d", tt.size)
	}
}

func TestNoLinkWhenThereIsNothingMore(t *testing.T) {
	for _, direction := range []domain.Direction{domain.DirectionNewest, domain.DirectionBefore, domain.DirectionAfter} {
		assert.Empty(t, continuation(domain.MessagePage{ConversationID: "c", Direction: direction, Size: 5, BeforeCursor: "3", AfterCursor: "8"}), string(direction))
	}
}

func TestParameterNamesAreCaseSensitive(t *testing.T) {
	history := mocks.NewHistory(t)
	history.On("Messages", mock.Anything, service.MessagesRequest{ConversationID: "conv_v1_a"}).
		Return(domain.MessagePage{ConversationID: "conv_v1_a", Size: domain.PageSize}, nil).Once()

	rec := serve(t, history, &recordingLog{}, "/v1/conversations/conv_v1_a/messages?Page[Size]=1&PAGE[SIZE]=2")

	assert.Equal(t, http.StatusOK, rec.Code, "a name spelled in another case is an unknown parameter")
}
