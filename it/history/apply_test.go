package history_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/projection"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql/repository"
)

const conversation = "conv_v1_a"

// stream is one conversation: four chat messages (sequences 2, 3, 5, 6) among
// two events that are not chat.
func stream() []domain.InboxEvent {
	at := time.Date(2026, 10, 20, 14, 0, 0, 0, time.UTC)
	event := func(seq int, name, payload string) domain.InboxEvent {
		return domain.InboxEvent{
			OutboxID: int64(seq), ConversationID: conversation, Sequence: seq, EventName: name, EventVersion: 1,
			Payload: json.RawMessage(payload), Metadata: json.RawMessage(`{}`), IDYapeAccount: "acc-1", Actor: "user",
			OccurredAt: at.Add(time.Duration(seq) * time.Second),
			KafkaTopic: "yape.public.conversation_outbox", KafkaPartition: int32(seq % 3), KafkaOffset: int64(seq),
		}
	}

	return []domain.InboxEvent{
		event(1, "ConversationStarted", `{"channel": "a2a"}`),
		event(2, "UserMessageSent", `{"text": "quiero yapear"}`),
		event(3, "AgentMessageSent", `{"text": "¿A quién necesitas yapearle?"}`),
		event(4, "ToolActionSummarized", `{"tool_name": "draft_transfer"}`),
		event(5, "UserMessageSent", `{"text": "@(Juan Perez) 20 soles"}`),
		event(6, "AgentMessageSent", `{"text": "Revisa tu yapeo", "truncated": true}`),
	}
}

func pick(events []domain.InboxEvent, order ...int) []domain.InboxEvent {
	out := make([]domain.InboxEvent, 0, len(order))
	for _, seq := range order {
		out = append(out, events[seq-1])
	}

	return out
}

// digest fingerprints both tables without the columns that hold the clock.
func digest(t *testing.T, pool *pgxpool.Pool) (inbox, messages string) {
	t.Helper()

	ctx := context.Background()
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COALESCE(md5(string_agg(concat_ws('|', outbox_id, conversation_id, sequence, event_name, event_version, payload::text,
			metadata::text, correlation_id, id_yape_account, actor, occurred_at, kafka_topic, kafka_partition, kafka_offset,
			projected_at IS NOT NULL), E'\n' ORDER BY conversation_id, sequence)), '')
		FROM conversation_inbox`).Scan(&inbox))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COALESCE(md5(string_agg(m::text, E'\n' ORDER BY conversation_id, sequence)), '') FROM conversation_messages m`).Scan(&messages))

	return inbox, messages
}

func count(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()

	var n int
	require.NoError(t, pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n))

	return n
}

func TestApply(t *testing.T) {
	pool := newPool(t)
	migrateUp(t, pool)

	ctx := context.Background()
	repo := repository.NewInbox(pool)
	projector := projection.NewService(repo)

	reset := func(t *testing.T) {
		t.Helper()
		_, err := pool.Exec(ctx, "TRUNCATE conversation_messages, conversation_inbox")
		require.NoError(t, err)
	}

	// applyAll runs the events through the real projection and repository.
	applyAll := func(t *testing.T, events []domain.InboxEvent) (applied []bool) {
		t.Helper()

		for _, e := range events {
			message, err := projector.Project(e)
			require.NoError(t, err)

			result, err := repo.Apply(ctx, e, message)
			require.NoError(t, err)
			applied = append(applied, result.Applied)
		}

		return applied
	}

	events := stream()

	reset(t)
	applyAll(t, events)
	referenceInbox, referenceMessages := digest(t, pool)
	require.Equal(t, 6, count(t, pool, "conversation_inbox"))
	require.Equal(t, 4, count(t, pool, "conversation_messages"))

	t.Run("orderDoesNotChangeTheRows", func(t *testing.T) {
		tests := []struct {
			name  string
			order []int
		}{
			{name: "inOrder", order: []int{1, 2, 3, 4, 5, 6}},
			{name: "reversed", order: []int{6, 5, 4, 3, 2, 1}},
			{name: "fiveBeforeThree", order: []int{1, 2, 5, 3, 4, 6}},
			{name: "interleaved", order: []int{4, 1, 6, 2, 5, 3}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				reset(t)

				applied := applyAll(t, pick(events, tt.order...))

				assert.Equal(t, []bool{true, true, true, true, true, true}, applied)
				inbox, messages := digest(t, pool)
				assert.Equal(t, referenceInbox, inbox)
				assert.Equal(t, referenceMessages, messages)
			})
		}
	})

	t.Run("redeliveryChangesNothing", func(t *testing.T) {
		tests := []struct {
			name        string
			order       []int
			wantApplied []bool
		}{
			{name: "everyEventTwiceInARow", order: []int{1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6}, wantApplied: []bool{true, false, true, false, true, false, true, false, true, false, true, false}},
			{name: "wholeStreamReplayed", order: []int{1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6}, wantApplied: []bool{true, true, true, true, true, true, false, false, false, false, false, false}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				reset(t)

				applied := applyAll(t, pick(events, tt.order...))

				assert.Equal(t, tt.wantApplied, applied)
				inbox, messages := digest(t, pool)
				assert.Equal(t, referenceInbox, inbox)
				assert.Equal(t, referenceMessages, messages)
			})
		}
	})

	t.Run("skippedEventIsStoredAndStamped", func(t *testing.T) {
		reset(t)

		applyAll(t, pick(events, 4))

		assert.Equal(t, 1, count(t, pool, "conversation_inbox"))
		assert.Zero(t, count(t, pool, "conversation_messages"))
		var stamped bool
		require.NoError(t, pool.QueryRow(ctx, "SELECT projected_at IS NOT NULL FROM conversation_inbox").Scan(&stamped))
		assert.True(t, stamped)
	})

	t.Run("failureRollsBackTheInboxRow", func(t *testing.T) {
		good := pick(events, 2)[0]
		message, err := projector.Project(good)
		require.NoError(t, err)

		stream := pick(events, 3)[0]
		sameStreamOtherID := stream
		sameStreamOtherID.OutboxID = 99

		tests := []struct {
			name    string
			event   domain.InboxEvent
			message *domain.Message
			prior   []domain.InboxEvent
			wantErr string
		}{
			{
				name:  "badRoleFailsTheMessageInsert",
				event: good,
				message: func() *domain.Message {
					m := *message
					m.Role = "bot"
					return &m
				}(),
				wantErr: "conversation_messages_role_check",
			},
			{
				name:    "sameStreamWithAnotherOutboxID",
				event:   sameStreamOtherID,
				prior:   []domain.InboxEvent{stream},
				wantErr: "conversation_inbox_stream_key",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				reset(t)
				applyAll(t, tt.prior)
				inboxBefore, messagesBefore := digest(t, pool)

				_, err := repo.Apply(ctx, tt.event, tt.message)

				require.Error(t, err)
				assert.ErrorIs(t, err, domain.ErrRejected, "the store refuses it for good, so a consumer skips it")
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.NotContains(t, err.Error(), "Failing row", "the failing row never reaches the error")
				inboxAfter, messagesAfter := digest(t, pool)
				assert.Equal(t, inboxBefore, inboxAfter, "nothing from the failed event stays")
				assert.Equal(t, messagesBefore, messagesAfter)
			})
		}
	})

	t.Run("anotherEventWithAnUsedIDIsRefusedNotLost", func(t *testing.T) {
		tests := []struct {
			name         string
			conversation string
			sequence     int
			wantRejected bool
		}{
			{name: "theSameEvent", conversation: conversation, sequence: 2},
			{name: "sameConversationOtherPosition", conversation: conversation, sequence: 99, wantRejected: true},
			{name: "otherConversationSamePosition", conversation: "conv_other", sequence: 2, wantRejected: true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				reset(t)
				first := pick(events, 2)[0]
				applyAll(t, []domain.InboxEvent{first})
				inboxBefore, messagesBefore := digest(t, pool)

				reused := first
				reused.ConversationID, reused.Sequence = tt.conversation, tt.sequence
				result, err := repo.Apply(ctx, reused, nil)

				if tt.wantRejected {
					require.ErrorIs(t, err, domain.ErrRejected)
				} else {
					require.NoError(t, err)
				}
				assert.False(t, result.Applied)
				inboxAfter, messagesAfter := digest(t, pool)
				assert.Equal(t, inboxBefore, inboxAfter)
				assert.Equal(t, messagesBefore, messagesAfter)
			})
		}
	})

	t.Run("repeatKeepsTheFirstCopy", func(t *testing.T) {
		reset(t)
		first := pick(events, 2)[0]
		applyAll(t, []domain.InboxEvent{first})
		changed := first
		changed.Payload = json.RawMessage(`{"text": "changed"}`)

		message, err := projector.Project(changed)
		require.NoError(t, err)
		result, err := repo.Apply(ctx, changed, message)

		require.NoError(t, err)
		assert.False(t, result.Applied)
		var text string
		require.NoError(t, pool.QueryRow(ctx, "SELECT text FROM conversation_messages WHERE outbox_id = 2").Scan(&text))
		assert.Equal(t, "quiero yapear", text, fmt.Sprintf("the stored message %q is the first one", text))
	})
}
