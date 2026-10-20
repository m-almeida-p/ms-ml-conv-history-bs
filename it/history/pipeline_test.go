package history_test

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chatRow is the part of a history message the pipeline tests compare.
type chatRow struct {
	Sequence    int
	Role        string
	Author      string
	Text        string
	SourceEvent string
	Truncated   bool
}

func messagesOf(t *testing.T, pool *pgxpool.Pool, conversationID string) []chatRow {
	t.Helper()

	rows, err := pool.Query(context.Background(),
		`SELECT sequence, role, author_id, text, source_event, truncated FROM conversation_messages WHERE conversation_id = $1 ORDER BY sequence`, conversationID)
	require.NoError(t, err)
	defer rows.Close()

	var out []chatRow
	for rows.Next() {
		var r chatRow
		require.NoError(t, rows.Scan(&r.Sequence, &r.Role, &r.Author, &r.Text, &r.SourceEvent, &r.Truncated))
		out = append(out, r)
	}
	require.NoError(t, rows.Err())

	return out
}

func countWhere(t *testing.T, pool *pgxpool.Pool, table, conversationID string) int {
	t.Helper()

	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		"SELECT count(*) FROM "+table+" WHERE conversation_id = $1", conversationID).Scan(&n))

	return n
}

// waitForMessages blocks until the conversation has n chat messages.
func waitForMessages(t *testing.T, pool *pgxpool.Pool, conversationID string, n int) {
	t.Helper()

	eventually(t, 90*time.Second, fmt.Sprintf("%d messages of %s", n, conversationID), func() bool {
		return countWhere(t, pool, "conversation_messages", conversationID) >= n
	})
}

// TestPipelineThroughDebezium inserts rows into the agent's outbox, as the agent
// does, and waits for the chat to appear in the history: outbox -> WAL ->
// Debezium -> Kafka -> consumer -> inbox -> messages. It needs the local cluster
// with the conversation-outbox connector running. The history database is a
// throwaway one; the topic and the agent's database are the real ones, and the
// rows are removed afterwards.
func TestPipelineThroughDebezium(t *testing.T) {
	s := requireKafka(t)
	requireDebezium(t, s)

	history, historyDSN := newHistoryDB(t)
	ctx := context.Background()

	agent, err := pgxpool.New(ctx, s.agentDSN)
	require.NoError(t, err)
	t.Cleanup(agent.Close) // registered first, so it runs after the delete below

	conversation := uniqueName("pipeline-dbz")
	t.Cleanup(func() {
		_, err := agent.Exec(context.Background(), "DELETE FROM conversation_outbox WHERE conversation_id = $1", conversation)
		assert.NoError(t, err, "the test rows are removed from the agent's outbox")
	})

	// Three chat messages among six events, as an agent turn leaves them.
	events := []struct {
		name    string
		payload string
	}{
		{"ConversationStarted", `{"channel": "a2a"}`},
		{"UserMessageSent", `{"text": "quiero yapear", "sequence": 1}`},
		{"ToolActionSummarized", `{"tool_name": "draft_transfer"}`},
		{"AgentMessageSent", `{"text": "¿A quién y cuánto?", "sequence": 2}`},
		{"GuardrailResponseSent", `{"response_text": "No puedo ayudarte con eso", "level": "level1"}`},
		{"UserMessageBlocked", `{"reason": "jailbreak"}`},
	}

	consumer := startConsumer(t, consumerBinary(t), s.brokers, "yape.public.conversation_outbox", uniqueName("pipeline-group"), historyDSN)

	tx, err := agent.Begin(ctx)
	require.NoError(t, err)
	for i, e := range events {
		_, err := tx.Exec(ctx, `
			INSERT INTO conversation_outbox (conversation_id, event_name, event_version, sequence, payload, metadata, correlation_id, id_yape_account, actor, published, created_at)
			VALUES ($1, $2, 1, $3, $4::jsonb, '{"actor": "user"}'::jsonb, 'corr-pipeline', 'acc-pipeline', 'user', FALSE, now())`,
			conversation, e.name, i+1, e.payload)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit(ctx))

	waitForMessages(t, history, conversation, 3)

	assert.Equal(t, []chatRow{
		{Sequence: 2, Role: "appUser", Author: "acc-pipeline", Text: "quiero yapear", SourceEvent: "UserMessageSent"},
		{Sequence: 4, Role: "appMaker", Author: "ms-ml-genai-agent-bs", Text: "¿A quién y cuánto?", SourceEvent: "AgentMessageSent"},
		{Sequence: 5, Role: "appMaker", Author: "ms-ml-genai-agent-bs", Text: "No puedo ayudarte con eso", SourceEvent: "GuardrailResponseSent"},
	}, messagesOf(t, history, conversation))
	assert.Equal(t, 6, countWhere(t, history, "conversation_inbox", conversation), "every event is in the inbox, chat or not")

	var unstamped int
	require.NoError(t, history.QueryRow(ctx,
		"SELECT count(*) FROM conversation_inbox WHERE conversation_id = $1 AND projected_at IS NULL", conversation).Scan(&unstamped))
	assert.Zero(t, unstamped)

	consumer.stop(t)
}

// TestPipelineTopic feeds the consumer binary records it builds itself, on a
// topic of its own, to check what Debezium cannot be made to do on demand:
// redeliveries, records out of order across partitions, a tombstone and a
// malformed record. At the end the process must stop on SIGTERM with every offset
// committed.
func TestPipelineTopic(t *testing.T) {
	s := requireKafka(t)

	history, historyDSN := newHistoryDB(t)
	topic, group := uniqueName("pipeline-test"), uniqueName("pipeline-group")
	createTopic(t, s.brokers, topic)
	prod := newProducer(t, s.brokers)
	consumer := startConsumer(t, consumerBinary(t), s.brokers, topic, group, historyDSN)

	// marker is an event after which we know the partition has been read this far.
	nextID := int64(9_000_000)
	marker := func(conversation string, partition int32, sequence int) {
		nextID++
		prod.send(topic, partition, outboxKey(nextID), outboxValue(nextID, conversation, sequence, "AgentMessageSent", map[string]any{"text": "marker"}))
	}

	t.Run("sameRecordThreeTimesIsStoredOnce", func(t *testing.T) {
		conversation := uniqueName("dup")
		nextID++
		id := nextID
		for range 3 {
			prod.send(topic, 0, outboxKey(id), outboxValue(id, conversation, 1, "UserMessageSent", map[string]any{"text": "hola"}))
		}
		marker(conversation, 0, 2)

		waitForMessages(t, history, conversation, 2)

		assert.Equal(t, 2, countWhere(t, history, "conversation_inbox", conversation), "one inbox row for the repeated record, one for the marker")
		assert.Equal(t, []chatRow{
			{Sequence: 1, Role: "appUser", Author: "acc-pipeline", Text: "hola", SourceEvent: "UserMessageSent"},
			{Sequence: 2, Role: "appMaker", Author: "ms-ml-genai-agent-bs", Text: "marker", SourceEvent: "AgentMessageSent"},
		}, messagesOf(t, history, conversation))
	})

	t.Run("reverseOrderAcrossPartitionsReadsBackInOrder", func(t *testing.T) {
		conversation := uniqueName("rev")
		for seq := 5; seq >= 1; seq-- {
			nextID++
			name := "UserMessageSent"
			if seq%2 == 0 {
				name = "AgentMessageSent"
			}
			prod.send(topic, int32(seq%3), outboxKey(nextID), outboxValue(nextID, conversation, seq, name, map[string]any{"text": fmt.Sprintf("m%d", seq)}))
		}

		waitForMessages(t, history, conversation, 5)

		got := messagesOf(t, history, conversation)
		require.Len(t, got, 5)
		for i, row := range got {
			assert.Equal(t, i+1, row.Sequence)
			assert.Equal(t, fmt.Sprintf("m%d", i+1), row.Text)
		}
		assert.Equal(t, "appUser", got[0].Role)
		assert.Equal(t, "appMaker", got[1].Role)
	})

	t.Run("tombstoneChangesNothing", func(t *testing.T) {
		conversation := uniqueName("tomb")
		var before int
		require.NoError(t, history.QueryRow(context.Background(), "SELECT count(*) FROM conversation_inbox").Scan(&before))

		prod.send(topic, 2, outboxKey(9_999_999), nil)
		marker(conversation, 2, 1)
		waitForMessages(t, history, conversation, 1)

		var after int
		require.NoError(t, history.QueryRow(context.Background(), "SELECT count(*) FROM conversation_inbox").Scan(&after))
		assert.Equal(t, before+1, after, "only the marker was added")
	})

	t.Run("malformedRecordDoesNotBlockThePartition", func(t *testing.T) {
		conversation := uniqueName("bad")
		prod.send(topic, 1, []byte("k"), framed(4, []byte(`{"id": "not a number", "payload": "secret-card-4111"}`)))
		prod.send(topic, 1, []byte("k"), []byte("not json at all"))
		marker(conversation, 1, 1)

		waitForMessages(t, history, conversation, 1)
	})

	t.Run("recordsTheStoreCannotHoldDoNotBlockThePartition", func(t *testing.T) {
		conversation := uniqueName("unstorable")

		// A NUL character in the payload: well-formed JSON that Postgres refuses for good.
		nextID++
		prod.send(topic, 0, outboxKey(nextID), outboxValue(nextID, conversation, 1, "UserMessageSent", map[string]any{"text": "a\x00b"}))
		// A sequence no INT column can hold: refused by the decoder.
		nextID++
		prod.send(topic, 0, outboxKey(nextID), outboxValue(nextID, conversation, 3_000_000_000, "UserMessageSent", map[string]any{"text": "far away"}))
		marker(conversation, 0, 2)

		waitForMessages(t, history, conversation, 1)

		assert.Equal(t, 1, countWhere(t, history, "conversation_inbox", conversation), "only the marker is stored")
	})

	consumer.stop(t)
	assertCommittedEverything(t, s.brokers, topic, group)

	summary := regexp.MustCompile(`consumer stopped: ([^"]+)`).FindStringSubmatch(consumer.out.String())
	require.NotNil(t, summary, "the consumer logs its totals when it stops:\n%s", consumer.out.String())
	totals := map[string]int{}
	for _, m := range regexp.MustCompile(`(\w+)=(\d+)`).FindAllStringSubmatch(summary[1], -1) {
		totals[m[1]], _ = strconv.Atoi(m[2])
	}
	assert.Equal(t, 2, totals["duplicates"], "the record sent three times")
	assert.Equal(t, 1, totals["tombstones"])
	assert.Equal(t, 3, totals["malformed"], "two broken records and the sequence out of range")
	assert.Equal(t, 1, totals["rejected"], "the NUL character the store refuses")
	assert.Zero(t, totals["retries"])
	assert.NotContains(t, consumer.out.String(), "secret-card-4111", "no record content in the logs")
}
