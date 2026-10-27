package history_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/projection"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql/repository"
)

type summary struct {
	Count       int
	First, Last time.Time
}

func TestSummariesFollowTheMessages(t *testing.T) {
	pool := newPool(t)
	migrateUp(t, pool)
	ctx := context.Background()
	repo := repository.NewPostgresInboxRepository(pool)
	projector := projection.NewService(repo)

	reset := func(t *testing.T) {
		t.Helper()
		_, err := pool.Exec(ctx, "TRUNCATE conversation_summaries, conversation_messages, conversation_inbox")
		require.NoError(t, err)
	}
	apply := func(t *testing.T, events ...domain.InboxEvent) {
		t.Helper()
		for _, e := range events {
			message, err := projector.Project(e)
			require.NoError(t, err)
			_, err = repo.Apply(ctx, e, message)
			require.NoError(t, err)
		}
	}
	read := func(t *testing.T, account, id string) (s summary, found bool) {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT message_count, first_occurred_at, last_occurred_at FROM conversation_summaries WHERE id_yape_account = $1 AND conversation_id = $2`, account, id)
		require.NoError(t, err)
		defer rows.Close()
		if rows.Next() {
			require.NoError(t, rows.Scan(&s.Count, &s.First, &s.Last))
			s.First, s.Last = s.First.UTC(), s.Last.UTC()
			found = true
		}
		require.NoError(t, rows.Err())

		return s, found
	}
	at := time.Date(2026, 10, 20, 14, 0, 0, 0, time.UTC)

	t.Run("aMessageCreatesTheSummaryAndTheNextOnesWidenIt", func(t *testing.T) {
		reset(t)
		events := stream() // four chat messages at 14:00:02, :03, :05 and :06 among two other events

		apply(t, events[1])
		got, found := read(t, "acc-1", conversation)
		require.True(t, found)
		assert.Equal(t, summary{1, at.Add(2 * time.Second), at.Add(2 * time.Second)}, got)

		apply(t, events[0], events[2], events[3], events[4], events[5])
		got, _ = read(t, "acc-1", conversation)
		assert.Equal(t, summary{4, at.Add(2 * time.Second), at.Add(6 * time.Second)}, got, "events that are not chat add nothing")
		assertNoSummaryDrift(t, pool)
	})

	t.Run("theOrderTheEventsArriveInChangesNothing", func(t *testing.T) {
		reset(t)
		events := stream()
		apply(t, events...)
		reference, _ := read(t, "acc-1", conversation)

		for run := 1; run <= 20; run++ {
			reset(t)
			shuffled := append([]domain.InboxEvent(nil), events...)
			rand.New(rand.NewSource(int64(run))).Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
			apply(t, shuffled...)

			got, found := read(t, "acc-1", conversation)
			require.True(t, found, "order %d", run)
			assert.Equal(t, reference, got, "order %d", run)
		}
	})

	t.Run("aRedeliveredEventCountsOnce", func(t *testing.T) {
		reset(t)
		events := stream()
		apply(t, events...)
		apply(t, events...)
		apply(t, events[1], events[1], events[1])

		got, _ := read(t, "acc-1", conversation)
		assert.Equal(t, 4, got.Count)
		assertNoSummaryDrift(t, pool)
	})

	t.Run("aMessageAlreadyStoredIsRefreshedAndNotCountedAgain", func(t *testing.T) {
		// Normal flow never gets here, because the inbox refuses a repeat first. It is the state after a
		// manual repair: the message is stored, its inbox row is not, and the event arrives as new.
		reset(t)
		events := stream()
		apply(t, events...)
		_, err := pool.Exec(ctx, `DELETE FROM conversation_inbox WHERE conversation_id = $1 AND sequence = 2`, conversation)
		require.NoError(t, err)

		apply(t, events[1])

		got, _ := read(t, "acc-1", conversation)
		assert.Equal(t, 4, got.Count, "the message was counted when it first arrived")
		assert.Equal(t, 4, count(t, pool, "conversation_messages"))
		assertNoSummaryDrift(t, pool)
	})

	t.Run("aMessageWithNoAccountIsInNoSummary", func(t *testing.T) {
		reset(t)
		e := stream()[2] // an agent message: the assistant's reply can arrive with no account
		e.IDYapeAccount = ""
		apply(t, e)

		require.Equal(t, 1, count(t, pool, "conversation_messages"), "the message is stored")
		assert.Zero(t, count(t, pool, "conversation_summaries"), "and belongs to no account's list")
		assertNoSummaryDrift(t, pool)
	})

	t.Run("eachAccountAndConversationHasItsOwnRow", func(t *testing.T) {
		reset(t)
		for _, c := range []struct{ account, id string }{{"acc-1", "conv_a"}, {"acc-1", "conv_b"}, {"acc-2", "conv_c"}} {
			e := stream()[1]
			e.ConversationID, e.IDYapeAccount = c.id, c.account
			apply(t, e)
		}

		assert.Equal(t, 3, count(t, pool, "conversation_summaries"))
		for _, c := range []struct{ account, id string }{{"acc-1", "conv_a"}, {"acc-1", "conv_b"}, {"acc-2", "conv_c"}} {
			got, found := read(t, c.account, c.id)
			require.True(t, found, c.id)
			assert.Equal(t, 1, got.Count, c.id)
		}
	})

	t.Run("writersAtOnceOnOneConversationCountEveryMessageOnce", func(t *testing.T) {
		reset(t)
		const writers, perWriter = 8, 25
		var wg sync.WaitGroup
		errs := make(chan error, writers)
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perWriter; i++ {
					seq := w*perWriter + i + 1
					e := domain.InboxEvent{
						ConversationID: "conv_hot", Sequence: seq, EventName: "UserMessageSent", EventVersion: 1,
						Payload: []byte(fmt.Sprintf(`{"text": "m%d"}`, seq)), Metadata: []byte(`{}`), IDYapeAccount: "acc-hot", Actor: "user",
						OccurredAt: at.Add(time.Duration(seq) * time.Second), KafkaTopic: "t", KafkaPartition: int32(w % 3), KafkaOffset: int64(seq),
					}
					message, err := projector.Project(e)
					if err == nil {
						_, err = repo.Apply(ctx, e, message)
					}
					if err != nil {
						errs <- err
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}

		got, found := read(t, "acc-hot", "conv_hot")
		require.True(t, found)
		assert.Equal(t, writers*perWriter, got.Count, "no increment was lost to a concurrent writer")
		assert.True(t, got.First.Equal(at.Add(1*time.Second)), "first %s", got.First)
		assert.True(t, got.Last.Equal(at.Add(time.Duration(writers*perWriter)*time.Second)), "last %s", got.Last)
		assertNoSummaryDrift(t, pool)
	})

	t.Run("theSameEventFromManyWritersCountsOnce", func(t *testing.T) {
		reset(t)
		e := stream()[1]
		var wg sync.WaitGroup
		for w := 0; w < 30; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				message, err := projector.Project(e)
				assert.NoError(t, err)
				_, err = repo.Apply(ctx, e, message)
				assert.NoError(t, err)
			}()
		}
		wg.Wait()

		got, _ := read(t, "acc-1", conversation)
		assert.Equal(t, 1, got.Count)
		assertNoSummaryDrift(t, pool)
	})

	t.Run("rebuildRestoresSummariesThatDrifted", func(t *testing.T) {
		for name, damage := range map[string]string{
			"aCountIsWrong":          `UPDATE conversation_summaries SET message_count = message_count + 5`,
			"aTimeIsWrong":           `UPDATE conversation_summaries SET last_occurred_at = last_occurred_at + interval '1 day'`,
			"aSummaryIsMissing":      `DELETE FROM conversation_summaries`,
			"aSummaryHasNoMessages":  `INSERT INTO conversation_summaries VALUES ('acc-ghost', 'conv_ghost', 3, now(), now())`,
			"theMessagesWereEmptied": `DELETE FROM conversation_messages`,
		} {
			t.Run(name, func(t *testing.T) {
				reset(t)
				apply(t, stream()...)
				want, _ := read(t, "acc-1", conversation)

				_, err := pool.Exec(ctx, damage)
				require.NoError(t, err)
				_, err = repo.Rebuild(ctx, projector.Project)
				require.NoError(t, err)

				assertNoSummaryDrift(t, pool)
				got, found := read(t, "acc-1", conversation)
				require.True(t, found)
				assert.Equal(t, want, got)
				_, ghost := read(t, "acc-ghost", "conv_ghost")
				assert.False(t, ghost, "a summary with no messages does not survive a rebuild")
			})
		}
	})

	t.Run("rebuildIsAllOrNothing", func(t *testing.T) {
		reset(t)
		apply(t, stream()...)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		_, err := repo.Rebuild(cancelled, projector.Project)

		require.Error(t, err)
		assert.Equal(t, 4, count(t, pool, "conversation_messages"))
		got, found := read(t, "acc-1", conversation)
		require.True(t, found, "a rebuild that stops leaves the summaries as they were")
		assert.Equal(t, 4, got.Count)
	})
}
