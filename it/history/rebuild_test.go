package history_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/projection"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql/repository"
)

func TestRebuild(t *testing.T) {
	pool := newPool(t)
	migrateUp(t, pool)

	ctx := context.Background()
	repo := repository.NewInbox(pool)
	service := projection.NewService(repo)

	load := func(t *testing.T, events []domain.InboxEvent) {
		t.Helper()
		_, err := pool.Exec(ctx, "TRUNCATE conversation_messages, conversation_inbox")
		require.NoError(t, err)

		for _, e := range events {
			message, err := service.Project(e)
			if err != nil {
				message = nil // an invalid event is kept in the inbox, without a message
			}
			_, err = repo.Apply(ctx, e, message)
			require.NoError(t, err)
		}
	}

	events := stream()
	// Event 7 should project but has no text: it stays in the inbox and adds no message.
	broken := events[1]
	broken.Sequence, broken.Payload = 7, json.RawMessage(`{"message_id": "m-9"}`)
	withBroken := append(append([]domain.InboxEvent{}, events...), broken)

	load(t, withBroken)
	referenceInbox, referenceMessages := digest(t, pool)
	require.Equal(t, 7, count(t, pool, "conversation_inbox"))
	require.Equal(t, 4, count(t, pool, "conversation_messages"))

	t.Run("rebuildRestoresTheMessages", func(t *testing.T) {
		tests := []struct {
			name   string
			damage string
		}{
			{name: "messagesWereEmptied", damage: "DELETE FROM conversation_messages"},
			{name: "aTextWasWrong", damage: "UPDATE conversation_messages SET text = 'wrong', role = 'appMaker'"},
			{name: "aStrayRowExisted", damage: "INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, source_event, received_at) VALUES ('stray', 1, 'appUser', 'a', 't', 'E', now())"},
			{name: "nothingWasWrong", damage: "SELECT 1"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				load(t, withBroken)
				_, err := pool.Exec(ctx, tt.damage)
				require.NoError(t, err)

				result, err := service.Rebuild(ctx)

				require.NoError(t, err)
				assert.Equal(t, domain.RebuildResult{Events: 7, Messages: 4, Invalid: 1}, result)
				inbox, messages := digest(t, pool)
				assert.Equal(t, referenceInbox, inbox, "the inbox is never modified")
				assert.Equal(t, referenceMessages, messages)
			})
		}
	})

	t.Run("runningItAgainGivesTheSameRows", func(t *testing.T) {
		tests := []struct{ name string }{{"second"}, {"third"}}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := service.Rebuild(ctx)

				require.NoError(t, err)
				inbox, messages := digest(t, pool)
				assert.Equal(t, referenceInbox, inbox)
				assert.Equal(t, referenceMessages, messages)
			})
		}
	})

	t.Run("batchSizeDoesNotChangeTheRows", func(t *testing.T) {
		tests := []struct {
			name string
			size int
		}{
			{name: "oneRowPerBatch", size: 1},
			{name: "twoRowsPerBatch", size: 2},
			{name: "batchEqualToTheInbox", size: 7},
			{name: "batchLargerThanTheInbox", size: 100},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				load(t, withBroken)
				_, err := pool.Exec(ctx, "UPDATE conversation_messages SET text = 'stale'")
				require.NoError(t, err)

				result, err := projection.NewService(repository.NewInbox(pool, repository.WithBatchSize(tt.size))).Rebuild(ctx)

				require.NoError(t, err)
				assert.Equal(t, domain.RebuildResult{Events: 7, Messages: 4, Invalid: 1}, result)
				inbox, messages := digest(t, pool)
				assert.Equal(t, referenceInbox, inbox)
				assert.Equal(t, referenceMessages, messages)
			})
		}
	})

	t.Run("cancelledReplayRollsBack", func(t *testing.T) {
		load(t, withBroken)
		_, err := pool.Exec(ctx, "UPDATE conversation_messages SET text = 'before the rebuild'")
		require.NoError(t, err)
		inboxBefore, messagesBefore := digest(t, pool)

		cancelCtx, cancel := context.WithCancel(ctx)
		seen := 0
		project := func(e domain.InboxEvent) (*domain.Message, error) {
			seen++
			if seen == 3 {
				cancel()
			}

			return service.Project(e)
		}

		_, err = repo.Rebuild(cancelCtx, project)

		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		inboxAfter, messagesAfter := digest(t, pool)
		assert.Equal(t, inboxBefore, inboxAfter)
		assert.Equal(t, messagesBefore, messagesAfter, "the old messages stay")
	})

	t.Run("projectionErrorRollsBack", func(t *testing.T) {
		load(t, withBroken)
		_, messagesBefore := digest(t, pool)

		_, err := repo.Rebuild(ctx, func(domain.InboxEvent) (*domain.Message, error) {
			return nil, assert.AnError
		})

		require.ErrorIs(t, err, assert.AnError)
		_, messagesAfter := digest(t, pool)
		assert.Equal(t, messagesBefore, messagesAfter)
	})
}
