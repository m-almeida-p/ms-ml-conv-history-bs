package history_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql/repository"
)

const (
	giantConversation = "conv_giant"
	giantMessages     = 60_000
	giantTextBytes    = 200 // a chat line; the agent cuts longer ones
)

// messagesRowsRead is the most rows any scan of conversation_messages returned.
func messagesRowsRead(t *testing.T, plan []string) int {
	t.Helper()

	most := 0
	for _, line := range plan {
		if !strings.Contains(line, "conversation_messages") || !strings.Contains(line, "Scan") {
			continue
		}
		match := scanRows.FindStringSubmatch(line)
		require.NotNil(t, match, line)
		read, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		most = max(most, read)
	}

	return most
}

// measure logs what one paging statement cost: the rows its scan of conversation_messages
// returned, the pages it touched and the time it took.
func measure(t *testing.T, what string, plan []string) {
	t.Helper()

	took := ""
	for _, line := range plan {
		if strings.HasPrefix(line, "Execution Time:") {
			took = strings.TrimSpace(strings.TrimPrefix(line, "Execution Time:"))
		}
	}
	t.Logf("MEASURE %-30s rows read %-4d pages %-3d execution %s", what, messagesRowsRead(t, plan), pagesTouched(t, plan), took)
}

// pagingSQL reads one of the embedded paging statements from the repository's SQL folder.
func pagingSQL(t *testing.T, name string) string {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("..", "..", "internal", "infrastructure", "postgresql", "repository", "SQL", name))
	require.NoError(t, err)

	return string(raw)
}

// TestMessagePagingAtVolume: with 400,000 messages stored and one conversation of 60,000, what a page
// costs follows its size and not its position in the conversation or the size of the table. The
// checks count rows and pages, which do not depend on the machine; the time ceilings are far above a
// run, so they only fail on a scan of the table.
func TestMessagePagingAtVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("loads 400,000 messages")
	}

	pool := newPool(t)
	migrateUp(t, pool)
	loadVolume(t, pool)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
		SELECT $1, s, CASE WHEN s % 2 = 1 THEN 'appUser' ELSE 'appMaker' END, 'acc_giant', repeat('x', $3), 'UserMessageSent', 'acc_giant',
		       now() - interval '2 days' + s * interval '1 second'
		FROM generate_series(1, $2) s`, giantConversation, giantMessages, giantTextBytes)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `ANALYZE conversation_messages`)
	require.NoError(t, err)

	var tableRows int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM conversation_messages`).Scan(&tableRows))
	t.Logf("MEASURE conversation_messages: %d rows", tableRows)
	require.Greater(t, tableRows, 300_000, "the volume should be large")

	newest := pagingSQL(t, "list_messages_newest.sql")
	before := pagingSQL(t, "list_messages_before.sql")
	repo := repository.NewPostgresMessageRepository(pool)

	sizes := []int{1, 10, 50, domain.PageSize}

	t.Run("aPageReadsSizePlusOneRowsAndTouchesPagesInProportionToTheSize", func(t *testing.T) {
		var previous int
		for _, size := range sizes {
			plan := planOf(t, pool, newest, giantConversation, size+1)
			measure(t, "newest page, size "+strconv.Itoa(size), plan)

			assert.Equal(t, size+1, messagesRowsRead(t, plan), "size %d reads one row more than the page, never the conversation:\n%s", size, strings.Join(plan, "\n"))
			assert.NotContains(t, strings.Join(plan, "\n"), "Seq Scan", "size %d scans the table:\n%s", size, strings.Join(plan, "\n"))
			assert.NotContains(t, strings.Join(plan, "\n"), "Sort", "size %d sorts instead of reading the index in order:\n%s", size, strings.Join(plan, "\n"))

			pages := pagesTouched(t, plan)
			assert.LessOrEqual(t, pages, size+8, "size %d touches %d pages", size, pages)
			assert.GreaterOrEqual(t, pages, previous, "a larger page never touches fewer pages")
			previous = pages
		}
	})

	t.Run("aPageCostsTheSameAtTheStartTheMiddleAndTheEndOfAConversation", func(t *testing.T) {
		for _, size := range []int{1, domain.PageSize} {
			cost := map[string]int{}
			for name, cursor := range map[string]int{"end": giantMessages - 5, "middle": giantMessages / 2, "start": size + 5} {
				plan := planOf(t, pool, before, giantConversation, cursor, size+1)
				measure(t, "before "+name+", size "+strconv.Itoa(size), plan)

				assert.Equal(t, size+1, messagesRowsRead(t, plan), "%s, size %d:\n%s", name, size, strings.Join(plan, "\n"))
				assert.NotContains(t, strings.Join(plan, "\n"), "Seq Scan", "%s, size %d", name, size)
				cost[name] = pagesTouched(t, plan)
			}

			lowest, highest := cost["end"], cost["end"]
			for _, pages := range cost {
				lowest, highest = min(lowest, pages), max(highest, pages)
			}
			assert.LessOrEqual(t, highest-lowest, 4, "size %d: the pages touched do not follow the depth: %v", size, cost)
		}
	})

	t.Run("walkingTheWholeConversationAtEverySizeVisitsEachMessageOnce", func(t *testing.T) {
		for _, size := range []int{100, 50} {
			started := time.Now()
			var latencies []time.Duration
			seen := 0
			expect := giantMessages
			query := domain.PageQuery{Size: size, ConversationID: giantConversation, Direction: domain.DirectionNewest}
			for {
				at := time.Now()
				page, err := repo.ListMessages(ctx, query)
				latencies = append(latencies, time.Since(at))
				require.NoError(t, err)
				require.LessOrEqual(t, len(page.Messages), size)
				first := expect - len(page.Messages) + 1
				for i, m := range page.Messages {
					require.Equal(t, first+i, m.Sequence, "size %d: a page skipped or repeated a message", size)
				}
				expect -= len(page.Messages)
				seen += len(page.Messages)
				if !page.HasMore {
					break
				}
				query = domain.PageQuery{Size: size, ConversationID: giantConversation, Direction: domain.DirectionBefore, CursorSequence: page.Messages[0].Sequence}
			}

			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			t.Logf("MEASURE walk size %-3d %d pages in %s; page latency median %s, slowest %s",
				size, len(latencies), time.Since(started).Round(time.Millisecond),
				latencies[len(latencies)/2].Round(10*time.Microsecond), latencies[len(latencies)-1].Round(10*time.Microsecond))
			assert.Equal(t, giantMessages, seen)
			assert.Less(t, latencies[len(latencies)-1], 2*time.Second, "size %d: the slowest page", size)
		}
	})

	t.Run("aSizeOfOneIsStillAnIndexSeekInTheBiggestConversation", func(t *testing.T) {
		started := time.Now()
		page, err := repo.ListMessages(ctx, domain.PageQuery{Size: 1, ConversationID: giantConversation, Direction: domain.DirectionBefore, CursorSequence: giantMessages / 2})

		require.NoError(t, err)
		require.Len(t, page.Messages, 1)
		assert.Equal(t, giantMessages/2-1, page.Messages[0].Sequence)
		assert.True(t, page.HasMore)
		assert.Less(t, time.Since(started), 2*time.Second)
	})
}

const (
	heavyAccount       = "acc_heavy"
	heavyConversations = 5_000
	heavyMessagesEach  = 4
)

// TestConversationPagingAtVolume: one account with 5,000 conversations (20,000 messages) among 400,000
// messages. A page of conversations is built by grouping the account's messages, so its cost follows
// the account's history, not the table and not the size of the page. The checks count rows and pages,
// which do not depend on the machine; the time ceilings are far above a run.
func TestConversationPagingAtVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("loads 400,000 messages")
	}

	pool := newPool(t)
	migrateUp(t, pool)
	loadVolume(t, pool)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
		SELECT 'heavy_' || lpad(c::text, 5, '0'), s, 'appUser', $1, 'hola', 'UserMessageSent', $1,
		       now() - interval '3 days' + c * interval '1 minute' + s * interval '1 second'
		FROM generate_series(1, $2) c, generate_series(1, $3) s`, heavyAccount, heavyConversations, heavyMessagesEach)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `ANALYZE conversation_messages`)
	require.NoError(t, err)

	var tablePages int
	require.NoError(t, pool.QueryRow(ctx, `SELECT relpages FROM pg_class WHERE relname = 'conversation_messages'`).Scan(&tablePages))
	t.Logf("MEASURE conversation_messages: %d pages", tablePages)

	newest := pagingSQL(t, "list_conversations.sql")
	before := pagingSQL(t, "list_conversations_before.sql")
	after := pagingSQL(t, "list_conversations_after.sql")
	repo := repository.NewPostgresMessageRepository(pool)

	var cursorAt time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT max(occurred_at) FROM conversation_messages WHERE conversation_id = 'heavy_02500'`).Scan(&cursorAt))

	t.Run("aPageGroupsTheAccountsMessagesAndNothingElse", func(t *testing.T) {
		for name, run := range map[string][]any{"newest": {newest, heavyAccount, 101}, "before": {before, heavyAccount, cursorAt, "heavy_02500", 101}, "after": {after, heavyAccount, cursorAt, "heavy_02500", 101}} {
			query := run[0].(string)
			plan := planOf(t, pool, query, run[1:]...)
			measure(t, "conversations "+name+", size 100", plan)

			joined := strings.Join(plan, "\n")
			assert.NotContains(t, joined, "Seq Scan", "%s scans the table:\n%s", name, joined)
			assert.LessOrEqual(t, messagesRowsRead(t, plan), heavyConversations*heavyMessagesEach, "%s reads other accounts' messages:\n%s", name, joined)
			assert.Less(t, pagesTouched(t, plan), tablePages/10, "%s touches a tenth of the table or more:\n%s", name, joined)
		}
	})

	t.Run("theCostDoesNotFollowThePageSize", func(t *testing.T) {
		pages := map[int]int{}
		for _, size := range []int{1, 10, 100} {
			plan := planOf(t, pool, newest, heavyAccount, size+1)
			measure(t, "conversations newest, size "+strconv.Itoa(size), plan)
			pages[size] = pagesTouched(t, plan)
		}

		assert.LessOrEqual(t, pages[100]-pages[1], pages[1]/2+10, "the pages touched are the account's history, whatever the page size: %v", pages)
	})

	t.Run("walkingAllConversationsVisitsEachOnce", func(t *testing.T) {
		started := time.Now()
		var latencies []time.Duration
		seen := map[string]bool{}
		query := domain.ConversationQuery{IDYapeAccount: heavyAccount, Direction: domain.DirectionNewest, Size: domain.ConversationListSize}
		for {
			at := time.Now()
			list, err := repo.ListConversations(ctx, query)
			latencies = append(latencies, time.Since(at))
			require.NoError(t, err)
			for _, c := range list.Conversations {
				require.False(t, seen[c.ConversationID], "%s came twice", c.ConversationID)
				seen[c.ConversationID] = true
				require.Equal(t, heavyMessagesEach, c.MessageCount)
			}
			if !list.HasMore {
				break
			}
			query = domain.ConversationQuery{IDYapeAccount: heavyAccount, Direction: domain.DirectionBefore, Size: domain.ConversationListSize,
				CursorID: list.Conversations[len(list.Conversations)-1].ConversationID}
		}

		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		t.Logf("MEASURE walk of %d conversations in %d pages: %s; page latency median %s, slowest %s",
			len(seen), len(latencies), time.Since(started).Round(time.Millisecond),
			latencies[len(latencies)/2].Round(100*time.Microsecond), latencies[len(latencies)-1].Round(100*time.Microsecond))
		assert.Len(t, seen, heavyConversations)
		assert.Less(t, latencies[len(latencies)-1], 3*time.Second, "the slowest page")
	})
}
