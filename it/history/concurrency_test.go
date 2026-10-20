package history_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/projection"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql/repository"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport/concurrent"
)

// longStream is 60 events of one conversation, every third one not chat.
func longStream() []domain.InboxEvent {
	at := time.Date(2026, 10, 20, 14, 0, 0, 0, time.UTC)
	events := make([]domain.InboxEvent, 0, 60)
	for seq := 1; seq <= 60; seq++ {
		name, payload := "UserMessageSent", fmt.Sprintf(`{"text": "message %d"}`, seq)
		switch {
		case seq%3 == 0:
			name, payload = "ToolActionSummarized", `{"tool_name": "t"}`
		case seq%2 == 0:
			name = "AgentMessageSent"
		}
		events = append(events, domain.InboxEvent{
			OutboxID: int64(seq), ConversationID: "conv", Sequence: seq, EventName: name, EventVersion: 1,
			Payload: json.RawMessage(payload), Metadata: json.RawMessage(`{}`), IDYapeAccount: "acc-1", Actor: "user",
			OccurredAt: at.Add(time.Duration(seq) * time.Second), KafkaTopic: "t", KafkaPartition: int32(seq % 3), KafkaOffset: int64(seq),
		})
	}

	return events
}

// outcome is what a scenario leaves behind. It is a plain value, so outcomes of
// different executors compare with one assertion.
type outcome struct {
	Applied    int
	Duplicates int
	Failures   int
	Inbox      string
	Messages   string
}

// TestConcurrentWriters: several consumers, or a consumer and a rebuild, at the
// same time must leave the same rows as one writer. Each scenario is a function of
// an executor, run one task at a time (the reference) and all at once, and the
// outcomes must be equal.
func TestConcurrentWriters(t *testing.T) {
	pool := newPool(t)
	migrateUp(t, pool)
	ctx := context.Background()
	service := projection.NewService(repository.NewInbox(pool))
	events := longStream()

	reset := func() {
		_, err := pool.Exec(ctx, "TRUNCATE conversation_messages, conversation_inbox")
		require.NoError(t, err)
	}
	handle := func(e domain.InboxEvent) (domain.ApplyResult, error) { return service.Handle(ctx, e) }

	// feed stores every event in the given order and returns what each store said.
	feed := func(order []int) []domain.ApplyResult {
		return concurrent.Collect(concurrent.Sequential, len(order), func(i int) domain.ApplyResult {
			result, err := handle(events[order[i]])
			require.NoError(t, err)

			return result
		})
	}

	reset()
	feed(concurrent.Orders(len(events))[0])
	referenceInbox, referenceMessages := digest(t, pool)
	require.Equal(t, 60, count(t, pool, "conversation_inbox"))
	require.Equal(t, 40, count(t, pool, "conversation_messages"))

	scenarios := []struct {
		name string
		run  func(exec concurrent.Executor) outcome
		want outcome
	}{
		{
			name: "theSameEventFromThirtyConsumersIsStoredOnce",
			run: func(exec concurrent.Executor) outcome {
				reset()
				type stored struct {
					Result domain.ApplyResult
					Err    error
				}
				results := concurrent.Collect(exec, 30, func(int) stored {
					result, err := handle(events[0])
					return stored{result, err}
				})
				inbox, messages := digest(t, pool)

				return outcome{
					Applied:    concurrent.Count(results, func(r stored) bool { return r.Err == nil && r.Result.Applied }),
					Duplicates: concurrent.Count(results, func(r stored) bool { return r.Err == nil && !r.Result.Applied }),
					Failures:   concurrent.Count(results, func(r stored) bool { return r.Err != nil }),
					Inbox:      inbox, Messages: messages,
				}
			},
			want: outcome{Applied: 1, Duplicates: 29},
		},
		{
			name: "theWholeStreamFromFiveConsumersInFiveOrders",
			run: func(exec concurrent.Executor) outcome {
				reset()
				orders := concurrent.Orders(len(events))
				perConsumer := concurrent.Collect(exec, len(orders), func(i int) []domain.ApplyResult { return feed(orders[i]) })
				all := slices.Concat(perConsumer...)
				inbox, messages := digest(t, pool)

				return outcome{
					Applied:    concurrent.Count(all, func(r domain.ApplyResult) bool { return r.Applied }),
					Duplicates: concurrent.Count(all, func(r domain.ApplyResult) bool { return !r.Applied }),
					Inbox:      inbox, Messages: messages,
				}
			},
			want: outcome{Applied: 60, Duplicates: 240, Inbox: referenceInbox, Messages: referenceMessages},
		},
		{
			name: "rebuildsRunningAlongsideTwoConsumers",
			run: func(exec concurrent.Executor) outcome {
				reset()
				orders := concurrent.Orders(len(events))[:2]
				// Five tasks at once: two consumers with the whole stream and three rebuilds.
				failures := concurrent.Collect(exec, 5, func(i int) int {
					if i < 2 {
						return concurrent.Count(concurrent.Map(concurrent.Reorder(events, orders[i]), func(e domain.InboxEvent) error {
							_, err := handle(e)
							return err
						}), func(err error) bool { return err != nil })
					}
					_, err := service.Rebuild(ctx)
					if err != nil {
						return 1
					}

					return 0
				})
				// Whatever the interleaving was, a rebuild after the writers stopped must
				// settle the table on exactly what the inbox says.
				_, err := service.Rebuild(ctx)
				require.NoError(t, err)
				inbox, messages := digest(t, pool)

				return outcome{Failures: concurrent.Count(failures, func(n int) bool { return n > 0 }), Inbox: inbox, Messages: messages}
			},
			want: outcome{Inbox: referenceInbox, Messages: referenceMessages},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			outcomes := concurrent.Outcomes(sc.run, concurrent.All)

			for i, executor := range concurrent.All {
				assert.Equal(t, sc.want.Failures, outcomes[i].Failures, "%s: no store failed, no deadlock", executor.Name)
				assert.Equal(t, outcomes[0], outcomes[i], "%s: the same outcome as one at a time", executor.Name)
			}
			assert.Equal(t, sc.want.Applied, outcomes[0].Applied)
			assert.Equal(t, sc.want.Duplicates, outcomes[0].Duplicates)
			if sc.want.Inbox != "" {
				assert.Equal(t, sc.want.Inbox, outcomes[0].Inbox)
				assert.Equal(t, sc.want.Messages, outcomes[0].Messages)
			}
		})
	}
}
