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
			ConversationID: conversation, Sequence: seq, EventName: name, EventVersion: 1,
			Payload: json.RawMessage(payload), Metadata: json.RawMessage(`{}`), IDYapeAccount: "acc-1", Actor: "user", Flow: flowOf(seq),
			OccurredAt: at.Add(time.Duration(seq) * time.Second),
			KafkaTopic: "yape.public.conversation_events", KafkaPartition: int32(seq % 3), KafkaOffset: int64(seq),
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

// flowOf says which flow the sample stream is in at a position: none while the user
// is still talking to the dispatcher, the P2P flow from the third event on.
func flowOf(seq int) string {
	if seq >= 3 {
		return "p2p"
	}

	return ""
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
		SELECT COALESCE(md5(string_agg(concat_ws('|', conversation_id, sequence, event_name, event_version, payload::text,
			metadata::text, correlation_id, id_yape_account, actor, flow, occurred_at, kafka_topic, kafka_partition, kafka_offset,
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

		earlier := pick(events, 3)[0]
		withNUL := *message
		withNUL.Text = "a\x00b"

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
				name:    "aNULCharacterFailsTheMessageInsertAndKeepsTheEarlierEvent",
				event:   good,
				message: &withNUL,
				prior:   []domain.InboxEvent{earlier},
				wantErr: "data the store cannot hold",
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

	t.Run("anotherEventAtAUsedPositionIsRefusedNotLost", func(t *testing.T) {
		tests := []struct {
			name         string
			change       func(e *domain.InboxEvent)
			withFlow     string // the flow of the stored event
			wantRejected bool
		}{
			{name: "theSameEvent", change: func(*domain.InboxEvent) {}},
			{name: "theSamePayloadWrittenWithOtherSpacing", change: func(e *domain.InboxEvent) { e.Payload = json.RawMessage(`{"text":"quiero yapear"}`) }},
			{name: "anotherPayload", change: func(e *domain.InboxEvent) { e.Payload = json.RawMessage(`{"text": "otro"}`) }, wantRejected: true},
			{name: "anotherEventName", change: func(e *domain.InboxEvent) { e.EventName = "AgentMessageSent" }, wantRejected: true},
			{name: "anotherEventVersion", change: func(e *domain.InboxEvent) { e.EventVersion = 2 }, wantRejected: true},
			{name: "anotherMetadata", change: func(e *domain.InboxEvent) { e.Metadata = json.RawMessage(`{"actor": "agent"}`) }, wantRejected: true},
			{name: "anotherAccount", change: func(e *domain.InboxEvent) { e.IDYapeAccount = "acc-2" }, wantRejected: true},
			{name: "noAccountWhereThereWasOne", change: func(e *domain.InboxEvent) { e.IDYapeAccount = "" }, wantRejected: true},
			{name: "aFlowWhereThereWasNone", change: func(e *domain.InboxEvent) { e.Flow = "p2p" }, wantRejected: true},
			{name: "anotherActor", change: func(e *domain.InboxEvent) { e.Actor = "agent" }, wantRejected: true},
			{name: "aFlowRemovedAfterwards", change: func(e *domain.InboxEvent) { e.Flow = "" }, withFlow: "p2p", wantRejected: true},
			{name: "anotherFlow", change: func(e *domain.InboxEvent) { e.Flow = "bill_payment" }, withFlow: "p2p", wantRejected: true},
			{name: "theSameFlow", change: func(*domain.InboxEvent) {}, withFlow: "p2p"},
			{name: "aCorrelationIDWhereThereWasNone", change: func(e *domain.InboxEvent) { e.CorrelationID = "corr-1" }, wantRejected: true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				reset(t)
				first := pick(events, 2)[0]
				first.Flow = tt.withFlow
				applyAll(t, []domain.InboxEvent{first})
				inboxBefore, messagesBefore := digest(t, pool)

				again := first
				tt.change(&again)
				result, err := repo.Apply(ctx, again, nil)

				if tt.wantRejected {
					require.ErrorIs(t, err, domain.ErrRejected)
					assert.Contains(t, err.Error(), "already holds another event")
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

	t.Run("aChangedRepeatIsRefusedAndTheFirstCopyStays", func(t *testing.T) {
		reset(t)
		first := pick(events, 2)[0]
		applyAll(t, []domain.InboxEvent{first})
		changed := first
		changed.Payload = json.RawMessage(`{"text": "changed"}`)

		message, err := projector.Project(changed)
		require.NoError(t, err)
		result, err := repo.Apply(ctx, changed, message)

		require.ErrorIs(t, err, domain.ErrRejected)
		assert.False(t, result.Applied)
		var text string
		require.NoError(t, pool.QueryRow(ctx, "SELECT text FROM conversation_messages WHERE sequence = 2").Scan(&text))
		assert.Equal(t, "quiero yapear", text, fmt.Sprintf("the stored message %q is the first one", text))
	})
}
