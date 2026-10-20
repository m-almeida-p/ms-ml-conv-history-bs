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
		ConversationID: "conv_v1_a", Sequence: 2, OutboxID: 1042, Role: domain.RoleUser, AuthorID: "acc-1", Type: "text",
		Text: "quiero yapear", SourceEvent: "UserMessageSent", ReceivedAt: received,
	}
	agent := domain.Message{
		ConversationID: "conv_v1_a", Sequence: 3, OutboxID: 1043, Role: domain.RoleMaker, AuthorID: "ms-ml-genai-agent-bs", Type: "text",
		Text: "¿A quién?", Truncated: true, SourceEvent: "AgentMessageSent", ReceivedAt: received,
	}
	storeDown := errors.New("connection reset: password=hunter2")

	userJSON := `{"_id":"1042","type":"text","role":"appUser","authorId":"acc-1","text":"quiero yapear","received":1792505243.792,
		"source":{"type":"api"},"metadata":{"sequence":2,"sourceEvent":"UserMessageSent","truncated":false}}`
	agentJSON := `{"_id":"1043","type":"text","role":"appMaker","authorId":"ms-ml-genai-agent-bs","text":"¿A quién?","received":1792505243.792,
		"source":{"type":"api"},"metadata":{"sequence":3,"sourceEvent":"AgentMessageSent","truncated":true}}`

	tests := []struct {
		name       string
		target     string
		wantReq    service.MessagesRequest
		page       domain.MessagePage
		err        error
		wantStatus int
		wantJSON   string
		wantLogged bool
	}{
		{
			name:    "newestPageWithMoreOlderMessagesLinksBack",
			target:  "/v1/conversations/conv_v1_a/messages",
			wantReq: service.MessagesRequest{ConversationID: "conv_v1_a"},
			page: domain.MessagePage{
				ConversationID: "conv_v1_a", Direction: domain.DirectionNewest, Messages: []domain.Message{user, agent},
				HasMore: true, BeforeCursor: "1042", AfterCursor: "1043",
			},
			wantStatus: http.StatusOK,
			wantJSON: `{"conversation":{"_id":"conv_v1_a"},"messages":[` + userJSON + `,` + agentJSON + `],
				"meta":{"hasMore":true,"beforeCursor":"1042","afterCursor":"1043"},
				"links":{"prev":"/v1/conversations/conv_v1_a/messages?page[before]=1042"}}`,
		},
		{
			name:    "pageBeforeACursorPassesItAndLinksBack",
			target:  "/v1/conversations/conv_v1_a/messages?page%5Bbefore%5D=1042",
			wantReq: service.MessagesRequest{ConversationID: "conv_v1_a", Before: ptr("1042")},
			page: domain.MessagePage{
				ConversationID: "conv_v1_a", Direction: domain.DirectionBefore, Messages: []domain.Message{user},
				HasMore: true, BeforeCursor: "1042", AfterCursor: "1042",
			},
			wantStatus: http.StatusOK,
			wantJSON: `{"conversation":{"_id":"conv_v1_a"},"messages":[` + userJSON + `],
				"meta":{"hasMore":true,"beforeCursor":"1042","afterCursor":"1042"},
				"links":{"prev":"/v1/conversations/conv_v1_a/messages?page[before]=1042"}}`,
		},
		{
			name:    "pageAfterACursorWithMoreNewerMessagesLinksForward",
			target:  "/v1/conversations/conv_v1_a/messages?page[after]=1040",
			wantReq: service.MessagesRequest{ConversationID: "conv_v1_a", After: ptr("1040")},
			page: domain.MessagePage{
				ConversationID: "conv_v1_a", Direction: domain.DirectionAfter, Messages: []domain.Message{agent},
				HasMore: true, BeforeCursor: "1043", AfterCursor: "1043",
			},
			wantStatus: http.StatusOK,
			wantJSON: `{"conversation":{"_id":"conv_v1_a"},"messages":[` + agentJSON + `],
				"meta":{"hasMore":true,"beforeCursor":"1043","afterCursor":"1043"},
				"links":{"next":"/v1/conversations/conv_v1_a/messages?page[after]=1043"}}`,
		},
		{
			name:    "lastPageHasNoLinks",
			target:  "/v1/conversations/conv_v1_a/messages",
			wantReq: service.MessagesRequest{ConversationID: "conv_v1_a"},
			page: domain.MessagePage{
				ConversationID: "conv_v1_a", Direction: domain.DirectionNewest, Messages: []domain.Message{user},
				BeforeCursor: "1042", AfterCursor: "1042",
			},
			wantStatus: http.StatusOK,
			wantJSON: `{"conversation":{"_id":"conv_v1_a"},"messages":[` + userJSON + `],
				"meta":{"hasMore":false,"beforeCursor":"1042","afterCursor":"1042"},"links":{}}`,
		},
		{
			name:       "emptyPageIsAnEmptyArrayWithoutCursors",
			target:     "/v1/conversations/conv_v1_a/messages?page[after]=1043",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_a", After: ptr("1043")},
			page:       domain.MessagePage{ConversationID: "conv_v1_a", Direction: domain.DirectionAfter},
			wantStatus: http.StatusOK,
			wantJSON:   `{"conversation":{"_id":"conv_v1_a"},"messages":[],"meta":{"hasMore":false},"links":{}}`,
		},
		{
			name:       "anEmptyCursorIsPassedOnSoItIsRejectedAsACursor",
			target:     "/v1/conversations/conv_v1_a/messages?page[before]=",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_a", Before: ptr("")},
			err:        fmt.Errorf("%w: the cursor is not a message _id", domain.ErrInvalidCursor),
			wantStatus: http.StatusBadRequest,
			wantJSON:   `{"error":{"code":"invalid_cursor","description":"page[before] or page[after] is not the _id of a message in this conversation."}}`,
		},
		{
			name:       "anEmptyCursorBesideARealOneReachesTheBothCursorsRule",
			target:     "/v1/conversations/conv_v1_a/messages?page[before]=&page[after]=7",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_a", Before: ptr(""), After: ptr("7")},
			err:        fmt.Errorf("%w: page[before] and page[after] cannot be used together", domain.ErrInvalidRequest),
			wantStatus: http.StatusBadRequest,
			wantJSON:   `{"error":{"code":"invalid_request","description":"invalid request: page[before] and page[after] cannot be used together"}}`,
		},
		{
			name:       "bothCursorsAreABadRequest",
			target:     "/v1/conversations/conv_v1_a/messages?page[before]=1&page[after]=2",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_a", Before: ptr("1"), After: ptr("2")},
			err:        fmt.Errorf("%w: page[before] and page[after] cannot be used together", domain.ErrInvalidRequest),
			wantStatus: http.StatusBadRequest,
			wantJSON:   `{"error":{"code":"invalid_request","description":"invalid request: page[before] and page[after] cannot be used together"}}`,
		},
		{
			name:       "malformedConversationIDIsABadRequest",
			target:     "/v1/conversations/bad%20id/messages",
			wantReq:    service.MessagesRequest{ConversationID: "bad id"},
			err:        fmt.Errorf("%w: the conversation id is malformed", domain.ErrInvalidRequest),
			wantStatus: http.StatusBadRequest,
			wantJSON:   `{"error":{"code":"invalid_request","description":"invalid request: the conversation id is malformed"}}`,
		},
		{
			name:       "badCursorIsABadRequestThatDoesNotEchoIt",
			target:     "/v1/conversations/conv_v1_a/messages?page[before]=nope",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_a", Before: ptr("nope")},
			err:        fmt.Errorf("%w: the cursor is not a message _id", domain.ErrInvalidCursor),
			wantStatus: http.StatusBadRequest,
			wantJSON:   `{"error":{"code":"invalid_cursor","description":"page[before] or page[after] is not the _id of a message in this conversation."}}`,
		},
		{
			name:       "unknownConversationIsNotFound",
			target:     "/v1/conversations/conv_v1_zzz/messages",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_zzz"},
			err:        domain.ErrNotFound,
			wantStatus: http.StatusNotFound,
			wantJSON:   `{"error":{"code":"not_found","description":"No chat messages were found for this conversation."}}`,
		},
		{
			name:       "storeErrorIsAGenericServerErrorThatIsLogged",
			target:     "/v1/conversations/conv_v1_a/messages",
			wantReq:    service.MessagesRequest{ConversationID: "conv_v1_a"},
			err:        storeDown,
			wantStatus: http.StatusInternalServerError,
			wantJSON:   `{"error":{"code":"internal_error","description":"Something went wrong. Try again."}}`,
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
			assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")
			assert.JSONEq(t, tt.wantJSON, rec.Body.String())
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
		{name: "accountTwice", target: "/v1/conversations?idYapeAccount=a&idYapeAccount=b", want: "idYapeAccount is given more than once"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := mocks.NewHistory(t) // no call is expected: the service never sees the request

			rec := serve(t, history, &recordingLog{}, tt.target)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), `"code":"invalid_request"`)
			assert.Contains(t, rec.Body.String(), tt.want)
		})
	}
}

func TestUnknownParametersAreIgnored(t *testing.T) {
	history := mocks.NewHistory(t)
	history.On("Messages", mock.Anything, service.MessagesRequest{ConversationID: "conv_v1_a"}).
		Return(domain.MessagePage{ConversationID: "conv_v1_a"}, nil).Once()

	rec := serve(t, history, &recordingLog{}, "/v1/conversations/conv_v1_a/messages?page[size]=5&foo=bar")

	assert.Equal(t, http.StatusOK, rec.Code, "page[size] does not apply: the page is always 100")
}
