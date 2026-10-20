package api

import (
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
	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport/concurrent"
)

func ptr(s string) *string { return &s }

// The server must hold no state that one request can change for another: no
// cache, no counter, no shared buffer. Sixty-four requests over eight routes at
// once must each answer exactly as they do one at a time. Run with -race.
func TestServerAnswersTheSameWhenRequestsArriveAtOnce(t *testing.T) {
	at := time.Date(2026, 10, 20, 14, 7, 23, 0, time.UTC)
	message := func(id int64) domain.Message {
		return domain.Message{ConversationID: "conv_a", Sequence: int(id), OutboxID: id, Role: domain.RoleUser, AuthorID: "acc-1", Type: "text", Text: "hola", SourceEvent: "UserMessageSent", ReceivedAt: at}
	}
	page := func(direction domain.Direction, ids ...int64) domain.MessagePage {
		messages := concurrent.Map(ids, message)

		return domain.MessagePage{ConversationID: "conv_a", Direction: direction, Messages: messages, HasMore: true, BeforeCursor: "1", AfterCursor: "9"}
	}

	history := mocks.NewHistory(t)
	history.On("Messages", mock.Anything, service.MessagesRequest{ConversationID: "conv_a"}).Return(page(domain.DirectionNewest, 7, 8, 9), nil).Maybe()
	history.On("Messages", mock.Anything, service.MessagesRequest{ConversationID: "conv_a", Before: ptr("5")}).Return(page(domain.DirectionBefore, 3, 4), nil).Maybe()
	history.On("Messages", mock.Anything, service.MessagesRequest{ConversationID: "conv_a", After: ptr("5")}).Return(page(domain.DirectionAfter, 6), nil).Maybe()
	history.On("Messages", mock.Anything, service.MessagesRequest{ConversationID: "conv_zzz"}).Return(domain.MessagePage{}, domain.ErrNotFound).Maybe()
	history.On("Messages", mock.Anything, service.MessagesRequest{ConversationID: "conv_a", Before: ptr("abc")}).Return(domain.MessagePage{}, domain.ErrInvalidCursor).Maybe()
	history.On("Conversations", mock.Anything, "acc-1").Return(domain.ConversationList{Conversations: []domain.ConversationSummary{{ConversationID: "conv_a", MessageCount: 3, FirstReceivedAt: at, LastReceivedAt: at}}}, nil).Maybe()
	history.On("Conversations", mock.Anything, "").Return(domain.ConversationList{}, domain.ErrInvalidRequest).Maybe()
	history.On("Messages", mock.Anything, service.MessagesRequest{ConversationID: "conv_boom"}).Return(domain.MessagePage{}, errors.New("store down")).Maybe()

	targets := []struct {
		path       string
		wantStatus int
	}{
		{"/v1/conversations/conv_a/messages", http.StatusOK},
		{"/v1/conversations/conv_a/messages?page[before]=5", http.StatusOK},
		{"/v1/conversations/conv_a/messages?page[after]=5", http.StatusOK},
		{"/v1/conversations/conv_zzz/messages", http.StatusNotFound},
		{"/v1/conversations/conv_a/messages?page[before]=abc", http.StatusBadRequest},
		{"/v1/conversations?idYapeAccount=acc-1", http.StatusOK},
		{"/v1/conversations", http.StatusBadRequest},
		{"/v1/conversations/conv_boom/messages", http.StatusInternalServerError},
	}
	server := NewServer(history, &recordingLog{}, nil)

	type reply struct {
		Status int
		Body   string
	}
	scenario := func(exec concurrent.Executor) []reply {
		return concurrent.Collect(exec, 64, func(i int) reply {
			rec := do(server, http.MethodGet, targets[i%len(targets)].path)

			return reply{rec.Code, rec.Body.String()}
		})
	}

	outcomes := concurrent.Outcomes(scenario, concurrent.All)

	require.Len(t, outcomes, len(concurrent.All))
	for i, executor := range concurrent.All {
		assert.Equal(t, outcomes[0], outcomes[i], "%s: every reply is the one-at-a-time reply", executor.Name)
	}
	for i, r := range outcomes[0] {
		assert.Equal(t, targets[i%len(targets)].wantStatus, r.Status, targets[i%len(targets)].path)
		assert.Equal(t, outcomes[0][i%len(targets)], r, "the same request always gets the same answer")
	}
}
