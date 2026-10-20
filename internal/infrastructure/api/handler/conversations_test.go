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
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service/mocks"
)

func TestConversations(t *testing.T) {
	first := time.Date(2026, 10, 20, 14, 7, 23, 500_000_000, time.UTC) // 1792505243.5
	last := first.Add(5 * time.Minute)

	tests := []struct {
		name        string
		target      string
		wantAccount string
		list        domain.ConversationList
		err         error
		wantStatus  int
		wantJSON    string
		wantLogged  bool
	}{
		{
			name:        "conversationsOfAnAccount",
			target:      "/v1/conversations?idYapeAccount=acc-1",
			wantAccount: "acc-1",
			list: domain.ConversationList{Conversations: []domain.ConversationSummary{
				{ConversationID: "conv_v1_a", MessageCount: 5, FirstReceivedAt: first, LastReceivedAt: last},
				{ConversationID: "conv_v1_b", MessageCount: 1, FirstReceivedAt: first, LastReceivedAt: first},
			}},
			wantStatus: http.StatusOK,
			wantJSON: `{"conversations":[
				{"_id":"conv_v1_a","messageCount":5,"firstReceived":1792505243.5,"lastReceived":1792505543.5},
				{"_id":"conv_v1_b","messageCount":1,"firstReceived":1792505243.5,"lastReceived":1792505243.5}],
				"meta":{"hasMore":false}}`,
		},
		{
			name:        "moreThanAPageSaysSo",
			target:      "/v1/conversations?idYapeAccount=acc-1",
			wantAccount: "acc-1",
			list:        domain.ConversationList{Conversations: []domain.ConversationSummary{{ConversationID: "c", MessageCount: 1, FirstReceivedAt: first, LastReceivedAt: first}}, HasMore: true},
			wantStatus:  http.StatusOK,
			wantJSON:    `{"conversations":[{"_id":"c","messageCount":1,"firstReceived":1792505243.5,"lastReceived":1792505243.5}],"meta":{"hasMore":true}}`,
		},
		{
			name:        "accountWithNoConversationsGetsAnEmptyArray",
			target:      "/v1/conversations?idYapeAccount=acc-2",
			wantAccount: "acc-2",
			list:        domain.ConversationList{},
			wantStatus:  http.StatusOK,
			wantJSON:    `{"conversations":[],"meta":{"hasMore":false}}`,
		},
		{
			name:        "missingAccountIsABadRequest",
			target:      "/v1/conversations",
			wantAccount: "",
			err:         fmt.Errorf("%w: idYapeAccount is missing or malformed", domain.ErrInvalidRequest),
			wantStatus:  http.StatusBadRequest,
			wantJSON:    `{"error":{"code":"invalid_request","description":"invalid request: idYapeAccount is missing or malformed"}}`,
		},
		{
			name:        "storeErrorIsAGenericServerError",
			target:      "/v1/conversations?idYapeAccount=acc-1",
			wantAccount: "acc-1",
			err:         errors.New("connection reset"),
			wantStatus:  http.StatusInternalServerError,
			wantJSON:    `{"error":{"code":"internal_error","description":"Something went wrong. Try again."}}`,
			wantLogged:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := mocks.NewHistory(t)
			history.On("Conversations", mock.Anything, tt.wantAccount).Return(tt.list, tt.err).Once()
			log := &recordingLog{}

			rec := serve(t, history, log, tt.target)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.JSONEq(t, tt.wantJSON, rec.Body.String())
			assert.Equal(t, tt.wantLogged, log.text() != "")
			assert.NotContains(t, log.text(), "acc-1", "the log has no account id")
		})
	}
}
