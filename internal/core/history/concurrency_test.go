package history

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository/mocks"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport/concurrent"
)

// The service keeps no state between calls. The same requests at once must give
// the answers they give one at a time. Run with -race.
func TestServiceAnswersTheSameWhenCalledAtOnce(t *testing.T) {
	repo := mocks.NewMessageRepository(t)
	repo.On("ListMessages", mock.Anything, domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionNewest}).
		Return(domain.Page{Messages: messagesAt(7, 8, 9), HasMore: true}, nil).Maybe()
	repo.On("ListMessages", mock.Anything, domain.PageQuery{Size: domain.PageSize, ConversationID: "conv", Direction: domain.DirectionBefore, CursorSequence: 7}).
		Return(domain.Page{Messages: messagesAt(5, 6)}, nil).Maybe()
	repo.On("ListConversations", mock.Anything, domain.ConversationQuery{IDYapeAccount: "acc", Direction: domain.DirectionNewest, Size: domain.ConversationListSize}).
		Return(domain.ConversationList{Conversations: []domain.ConversationSummary{{ConversationID: "conv", MessageCount: 3}}}, nil).Maybe()
	svc := NewService(repo)

	requests := []service.MessagesRequest{
		{ConversationID: "conv"},
		{ConversationID: "conv", Before: ptr("7")},
		{ConversationID: "conv", Before: ptr("7"), After: ptr("1")},
		{ConversationID: "bad id"},
		{ConversationID: "conv", After: ptr("")},
	}

	type reply struct {
		Page domain.MessagePage
		Err  string
		List domain.ConversationList
	}
	scenario := func(exec concurrent.Executor) []reply {
		return concurrent.Collect(exec, 60, func(i int) reply {
			if i%6 == 5 {
				list, err := svc.Conversations(context.Background(), service.ConversationsRequest{IDYapeAccount: "acc"})

				return reply{List: list, Err: errText(err)}
			}
			page, err := svc.Messages(context.Background(), requests[i%len(requests)])

			return reply{Page: page, Err: errText(err)}
		})
	}

	outcomes := concurrent.Outcomes(scenario, concurrent.All)

	for i, executor := range concurrent.All {
		assert.Equal(t, outcomes[0], outcomes[i], "%s: the one-at-a-time answers", executor.Name)
	}
	assert.Equal(t, 10, concurrent.Count(outcomes[0], func(r reply) bool { return len(r.List.Conversations) == 1 }))
}

func errText(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}
