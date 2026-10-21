package history_test

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	volumeConversations = 20_000
	volumeEventsEach    = 20
	volumeAccounts      = 2_000 // ten conversations per account
)

// loadVolume stores 400,000 inbox events: 20,000 conversations of twenty events each,
// every one the same story so the expected runs are known. The start is outside any
// flow, a P2P transfer follows (a card, its approval), then the chat goes back outside.
//
//	1       ConversationStarted
//	2-9     the transfer: a user message handled by P2P, the agent, a card, its approval
//	10-20   back outside any flow; 10 was written inside P2P
func loadVolume(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	started := time.Now()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO conversation_inbox
		    (conversation_id, sequence, event_name, event_version, payload, metadata, id_yape_account, actor, flow, occurred_at,
		     kafka_topic, kafka_partition, kafka_offset)
		SELECT 'conv_' || c, s,
		       CASE s WHEN 1 THEN 'ConversationStarted'
		              WHEN 7 THEN 'ConfirmationRequested'
		              WHEN 8 THEN 'ConfirmationResolved'
		              WHEN 3 THEN 'AgentMessageSent' WHEN 5 THEN 'AgentMessageSent' WHEN 9 THEN 'AgentMessageSent'
		              ELSE CASE WHEN s % 2 = 0 THEN 'UserMessageSent' ELSE 'AgentMessageSent' END END,
		       1,
		       CASE s WHEN 7 THEN jsonb_build_object('confirmation_ref', 'ref_' || c)
		              WHEN 8 THEN jsonb_build_object('confirmation_ref', 'ref_' || c, 'decision', 'approved')
		              ELSE '{}'::jsonb END,
		       '{}'::jsonb,
		       'acc_' || (c % $1), 'agent',
		       CASE WHEN s BETWEEN 3 AND 10 THEN 'p2p' END,
		       now() - interval '1 day' + (s * interval '1 second'),
		       't', c % 3, c::bigint * 100 + s
		FROM generate_series(1, $2) c, generate_series(1, $3) s`,
		volumeAccounts, volumeConversations, volumeEventsEach)
	require.NoError(t, err)

	// The messages table is what finds the conversations of an account.
	_, err = pool.Exec(context.Background(), `
		INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, source_event, id_yape_account, flow, received_at)
		SELECT conversation_id, sequence, 'appUser', id_yape_account, 'x', event_name, id_yape_account, flow, occurred_at
		FROM conversation_inbox WHERE event_name IN ('UserMessageSent', 'AgentMessageSent')`)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `ANALYZE conversation_inbox; ANALYZE conversation_messages`)
	require.NoError(t, err)

	t.Logf("loaded %d events in %s", volumeConversations*volumeEventsEach, time.Since(started).Round(time.Millisecond))
}

// The runs every conversation of the volume has.
var volumeRuns = []run{
	{1, "", 1, 1, 1, "none"},
	{2, "p2p", 2, 9, 8, "approved"},
	{3, "", 10, 20, 11, "none"},
}

var (
	scanRows    = regexp.MustCompile(`actual rows=(\d+)`)
	sharedPages = regexp.MustCompile(`Buffers: shared ([^\n]*)`)
	pageCount   = regexp.MustCompile(`(?:hit|read)=(\d+)`)
)

// planOf runs EXPLAIN (ANALYZE, BUFFERS) and returns its lines.
func planOf(t *testing.T, pool *pgxpool.Pool, query string, args ...any) []string {
	t.Helper()

	rows, err := pool.Query(context.Background(), "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) "+query, args...)
	require.NoError(t, err)
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(t, rows.Err())

	return lines
}

// inboxRowsRead is the most rows any scan of the inbox returned, and how many scans there are.
func inboxRowsRead(t *testing.T, plan []string) (most, scans int) {
	t.Helper()

	for _, line := range plan {
		if !strings.Contains(line, "conversation_inbox") || !strings.Contains(line, "Scan") {
			continue
		}
		match := scanRows.FindStringSubmatch(line)
		require.NotNil(t, match, line)
		read, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		most = max(most, read)
		scans++
	}

	return most, scans
}

// report logs what a statement cost: the rows its scans read, the pages it touched, the time it took.
func report(t *testing.T, what string, plan []string) {
	t.Helper()

	most, _ := inboxRowsRead(t, plan)
	took := ""
	for _, line := range plan {
		if strings.HasPrefix(line, "Execution Time:") {
			took = strings.TrimSpace(strings.TrimPrefix(line, "Execution Time:"))
		}
	}
	t.Logf("MEASURE %-34s rows read (largest scan) %-7d pages %-6d execution %s", what, most, pagesTouched(t, plan), took)
}

// pagesTouched is the pages the whole statement touched: the first buffers line of the plan.
func pagesTouched(t *testing.T, plan []string) int {
	t.Helper()

	for _, line := range plan {
		if match := sharedPages.FindStringSubmatch(line); match != nil {
			total := 0
			for _, part := range pageCount.FindAllStringSubmatch(match[1], -1) {
				n, err := strconv.Atoi(part[1])
				require.NoError(t, err)
				total += n
			}

			return total
		}
	}
	require.Fail(t, "the plan has no buffers line", strings.Join(plan, "\n"))

	return 0
}

// TestFlowSegmentsAtVolume: with 400,000 events stored, reading the view for one
// conversation or for an account's conversations touches only those conversations.
// The checks count rows and pages, which do not depend on the machine; the time
// ceilings are a hundred times what a run takes, so they only fail on a scan of the table.
func TestFlowSegmentsAtVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("loads 400,000 events")
	}

	pool := newPool(t)
	migrateUp(t, pool)
	loadVolume(t, pool)
	ctx := context.Background()

	t.Run("oneConversationReadsItsOwnRowsOnly", func(t *testing.T) {
		started := time.Now()
		got := runsOf(t, pool, "conv_12345")
		took := time.Since(started)

		assert.Equal(t, volumeRuns, got, "the runs of a conversation among 20,000")
		assert.Less(t, took, 2*time.Second)

		plan := planOf(t, pool, `SELECT * FROM conversation_flow_segments WHERE conversation_id = 'conv_12345'`)
		report(t, "view, one conversation", plan)
		most, scans := inboxRowsRead(t, plan)
		assert.Positive(t, scans)
		assert.LessOrEqual(t, most, volumeEventsEach, "a scan read other conversations:\n%s", strings.Join(plan, "\n"))
		// The events are read twice (as facts and as the conversation's own rows), so about 46
		// pages; the whole table is thousands. A fiftieth of the table is far above one conversation
		// and far below a scan.
		var tablePages int
		require.NoError(t, pool.QueryRow(ctx, `SELECT relpages FROM pg_class WHERE relname = 'conversation_inbox'`).Scan(&tablePages))
		t.Logf("MEASURE conversation_inbox: %d pages", tablePages)
		require.Greater(t, tablePages, 2000, "the volume should fill thousands of pages")
		assert.Less(t, pagesTouched(t, plan), tablePages/50, "pages touched for one conversation:\n%s", strings.Join(plan, "\n"))
	})

	t.Run("anAccountsConversationsReadTheirOwnRowsOnly", func(t *testing.T) {
		const account = "acc_77" // ten conversations
		query := `
			SELECT conversation_id, count(*) FROM conversation_flow_segments
			WHERE conversation_id = ANY (ARRAY(SELECT conversation_id FROM conversation_messages WHERE id_yape_account = $1 GROUP BY 1))
			GROUP BY conversation_id`

		started := time.Now()
		rows, err := pool.Query(ctx, query, account)
		require.NoError(t, err)
		perConversation := map[string]int{}
		for rows.Next() {
			var id string
			var runs int
			require.NoError(t, rows.Scan(&id, &runs))
			perConversation[id] = runs
		}
		require.NoError(t, rows.Err())
		rows.Close()

		assert.Len(t, perConversation, volumeConversations/volumeAccounts)
		for id, runs := range perConversation {
			assert.Equal(t, len(volumeRuns), runs, id)
		}
		assert.Less(t, time.Since(started), 3*time.Second)

		plan := planOf(t, pool, query, account)
		report(t, "view, ten conversations of an account", plan)
		most, scans := inboxRowsRead(t, plan)
		assert.Positive(t, scans)
		assert.LessOrEqual(t, most, volumeEventsEach*volumeConversations/volumeAccounts,
			"a scan of the inbox read more than the account's ten conversations:\n%s", strings.Join(plan, "\n"))
	})

	t.Run("everyConversationOfTheTableHasTheSameRuns", func(t *testing.T) {
		var conversations, runs, wrong int
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT count(DISTINCT conversation_id), count(*),
			       count(*) FILTER (WHERE NOT ((run = 1 AND flow IS NULL AND from_sequence = 1 AND to_sequence = 1 AND outcome = 'none')
			                                OR (run = 2 AND flow = 'p2p' AND from_sequence = 2 AND to_sequence = 9 AND events = 8 AND outcome = 'approved'
			                                    AND confirmation_ref = 'ref_' || substr(conversation_id, 6))
			                                OR (run = 3 AND flow IS NULL AND from_sequence = 10 AND to_sequence = 20 AND events = 11 AND outcome = 'none')))
			FROM conversation_flow_segments`).Scan(&conversations, &runs, &wrong))

		assert.Equal(t, volumeConversations, conversations)
		assert.Equal(t, volumeConversations*len(volumeRuns), runs)
		assert.Zero(t, wrong, "runs that differ from the story every conversation tells")
	})

	t.Run("aNewConversationIsReadAtOnceAmongTheOthers", func(t *testing.T) {
		// What the consumer does all day: a conversation grows while the table is large.
		_, err := pool.Exec(ctx, `
			INSERT INTO conversation_inbox (conversation_id, sequence, event_name, event_version, payload, metadata, flow, occurred_at, kafka_topic, kafka_partition, kafka_offset)
			VALUES ('conv_new', 1, 'UserMessageSent', 1, '{}', '{}', NULL, now(), 't', 0, 900000001),
			       ('conv_new', 2, 'AgentMessageSent', 1, '{}', '{}', 'p2p', now(), 't', 0, 900000002)`)
		require.NoError(t, err)

		assert.Equal(t, []run{{1, "p2p", 1, 2, 2, "none"}}, runsOf(t, pool, "conv_new"))
	})
}
