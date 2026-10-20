package history_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql/repository"
)

// TestApplyRefusesWhatTheStoreCannotHold: data the database rejects for good
// must come back as ErrRejected, so a consumer skips it. Retried, it would
// block its partition forever.
func TestApplyRefusesWhatTheStoreCannotHold(t *testing.T) {
	pool := newPool(t)
	migrateUp(t, pool)
	ctx := context.Background()
	repo := repository.NewInbox(pool)

	valid := func() domain.InboxEvent {
		return domain.InboxEvent{
			OutboxID: 1, ConversationID: "conv", Sequence: 1, EventName: "UserMessageSent", EventVersion: 1,
			Payload: json.RawMessage(`{"text":"hola"}`), Metadata: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
			KafkaTopic: "t",
		}
	}

	tests := []struct {
		name     string
		mutate   func(*domain.InboxEvent)
		wantCode string
	}{
		{name: "nulCharacterInThePayload", mutate: func(e *domain.InboxEvent) { e.Payload = json.RawMessage(`{"text":"a\u0000b"}`) }, wantCode: "22P05"},
		{name: "conversationIDLongerThanTheColumn", mutate: func(e *domain.InboxEvent) { e.ConversationID = strings.Repeat("a", 129) }, wantCode: "22001"},
		{name: "eventNameLongerThanTheColumn", mutate: func(e *domain.InboxEvent) { e.EventName = strings.Repeat("a", 129) }, wantCode: "22001"},
		{name: "timeOutOfRange", mutate: func(e *domain.InboxEvent) { e.OccurredAt = time.Date(300000, 1, 1, 0, 0, 0, 0, time.UTC) }, wantCode: "22008"},
		{name: "nulCharacterInTheConversationID", mutate: func(e *domain.InboxEvent) { e.ConversationID = "a\x00b" }, wantCode: "22021"},
		{name: "actorNotInTheList", mutate: func(e *domain.InboxEvent) { e.Actor = "robot" }, wantCode: "23514"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := valid()
			tt.mutate(&event)

			result, err := repo.Apply(ctx, event, nil)

			require.Error(t, err)
			assert.ErrorIs(t, err, domain.ErrRejected, "refused for good, not worth a retry")
			assert.Contains(t, err.Error(), "("+tt.wantCode+")")
			assert.Equal(t, domain.ApplyResult{}, result)
			assert.Zero(t, count(t, pool, "conversation_inbox"), "nothing was written")
		})
	}

	t.Run("theStoreStillWorksAfterwards", func(t *testing.T) {
		result, err := repo.Apply(ctx, valid(), nil)

		require.NoError(t, err)
		assert.True(t, result.Applied)
	})
}
