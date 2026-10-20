package projection

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository/mocks"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport/concurrent"
)

// Project is a pure function of its event and the service holds no state, so the
// same events projected at once must give the messages they give one at a time.
// Run with -race.
func TestProjectionGivesTheSameWhenEventsAreProjectedAtOnce(t *testing.T) {
	inbox := mocks.NewInboxRepository(t)
	inbox.On("Apply", mock.Anything, mock.Anything, mock.Anything).Return(domain.ApplyResult{Applied: true}, nil).Maybe()
	svc := NewService(inbox)

	events := []domain.InboxEvent{
		{OutboxID: 1, ConversationID: "c", Sequence: 1, EventName: "UserMessageSent", Payload: json.RawMessage(`{"text": "hola"}`), IDYapeAccount: "acc"},
		{OutboxID: 2, ConversationID: "c", Sequence: 2, EventName: "AgentMessageSent", Payload: json.RawMessage(`{"text": "¿qué tal?", "truncated": true}`)},
		{OutboxID: 3, ConversationID: "c", Sequence: 3, EventName: "GuardrailResponseSent", Payload: json.RawMessage(`{"response_text": "no"}`)},
		{OutboxID: 4, ConversationID: "c", Sequence: 4, EventName: "ToolActionSummarized", Payload: json.RawMessage(`{}`)},
		{OutboxID: 5, ConversationID: "c", Sequence: 5, EventName: "UserMessageSent", Payload: json.RawMessage(`{"no_text": 1}`), IDYapeAccount: "acc"},
	}

	type projected struct {
		Message *domain.Message
		Err     string
		Handled domain.ApplyResult
	}
	scenario := func(exec concurrent.Executor) []projected {
		return concurrent.Collect(exec, 100, func(i int) projected {
			event := events[i%len(events)]
			message, err := svc.Project(event)
			handled, _ := svc.Handle(context.Background(), event)

			out := projected{Message: message, Handled: handled}
			if err != nil {
				out.Err = err.Error()
			}

			return out
		})
	}

	outcomes := concurrent.Outcomes(scenario, concurrent.All)

	for i, executor := range concurrent.All {
		assert.Equal(t, outcomes[0], outcomes[i], "%s: the one-at-a-time messages", executor.Name)
	}
	assert.Equal(t, 60, concurrent.Count(outcomes[0], func(p projected) bool { return p.Message != nil }))
}
