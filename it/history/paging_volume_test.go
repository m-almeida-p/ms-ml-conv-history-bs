package history_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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

// rowsRead is the most rows any scan of the named table returned.
func rowsRead(t *testing.T, plan []string, table string) int {
	t.Helper()

	most := 0
	for _, line := range plan {
		if !strings.Contains(line, table) || !strings.Contains(line, "Scan") {
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

// messagesRowsRead is the most rows any scan of conversation_messages returned.
func messagesRowsRead(t *testing.T, plan []string) int {
	return rowsRead(t, plan, "conversation_messages")
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

	// The conversation statements carry the first message, whose limit the repository writes into them.
	return strings.ReplaceAll(string(raw), "{{first_message_chars}}", strconv.Itoa(domain.FirstMessageMaxChars))
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
	createSchema(t, pool)
	loadVolume(t, pool)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
		SELECT $1, s, CASE WHEN s % 2 = 1 THEN 'appUser' ELSE 'appMaker' END, 'acc_giant', repeat('x', $3), 'UserMessageSent', 'acc_giant',
		       now() - interval '2 days' + s * interval '1 second'
		FROM generate_series(1, $2) s`, giantConversation, giantMessages, giantTextBytes)
	require.NoError(t, err)
	refreshSummaries(t, pool)
	_, err = pool.Exec(ctx, `ANALYZE conversation_messages; ANALYZE conversation_summaries`)
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
	smallAccount       = "acc_77" // ten conversations, from loadVolume
)

// TestConversationPagingAtVolume: one account with 5,000 conversations (20,000 messages) among 400,000
// messages. A page of conversations reads conversation_summaries, one row per conversation, so what it
// costs follows the page and not the account's history: it reads the rows it returns and one more, from
// the index, and touches the same pages for the heavy account as for a small one. (Grouping the messages,
// as the list did before the table, read all 20,000 of the account's messages for every page.) The checks
// count rows and pages, which do not depend on the machine; the time ceilings are far above a run.
func TestConversationPagingAtVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("loads 400,000 messages")
	}

	pool := newPool(t)
	createSchema(t, pool)
	loadVolume(t, pool)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
		SELECT 'heavy_' || lpad(c::text, 5, '0'), s, 'appUser', $1, 'hola', 'UserMessageSent', $1,
		       now() - interval '3 days' + c * interval '1 minute' + s * interval '1 second'
		FROM generate_series(1, $2) c, generate_series(1, $3) s`, heavyAccount, heavyConversations, heavyMessagesEach)
	require.NoError(t, err)
	refreshSummaries(t, pool)
	_, err = pool.Exec(ctx, `ANALYZE conversation_messages; ANALYZE conversation_summaries`)
	require.NoError(t, err)

	var summaryRows int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM conversation_summaries`).Scan(&summaryRows))
	t.Logf("MEASURE conversation_summaries: %d rows", summaryRows)
	require.Greater(t, summaryRows, 20_000, "the volume should be large")

	newest := pagingSQL(t, "list_conversations.sql")
	before := pagingSQL(t, "list_conversations_before.sql")
	after := pagingSQL(t, "list_conversations_after.sql")
	repo := repository.NewPostgresMessageRepository(pool)

	var cursorAt time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT last_occurred_at FROM conversation_summaries WHERE conversation_id = 'heavy_02500'`).Scan(&cursorAt))

	// planFor runs the statement of a direction for an account with a page of the given size.
	planFor := func(account, direction string, size int) []string {
		switch direction {
		case "newest":
			return planOf(t, pool, newest, account, size+1)
		case "before":
			return planOf(t, pool, before, account, cursorAt, "heavy_02500", size+1)
		}

		return planOf(t, pool, after, account, cursorAt, "heavy_02500", size+1)
	}
	measure := func(what string, plan []string) {
		took := ""
		for _, line := range plan {
			if strings.HasPrefix(line, "Execution Time:") {
				took = strings.TrimSpace(strings.TrimPrefix(line, "Execution Time:"))
			}
		}
		t.Logf("MEASURE %-34s rows read %-4d pages %-3d execution %s", what, rowsRead(t, plan, "conversation_summaries"), pagesTouched(t, plan), took)
	}

	t.Run("aPageReadsSizePlusOneRowsFromTheIndexAndNothingElse", func(t *testing.T) {
		for _, direction := range []string{"newest", "before", "after"} {
			for _, size := range []int{1, 10, domain.ConversationListSize} {
				plan := planFor(heavyAccount, direction, size)
				measure(fmt.Sprintf("conversations %s, size %d", direction, size), plan)
				joined := strings.Join(plan, "\n")

				assert.Equal(t, size+1, rowsRead(t, plan, "conversation_summaries"), "%s, size %d reads one row more than the page:\n%s", direction, size, joined)
				assert.NotContains(t, joined, "Seq Scan", "%s, size %d scans the table:\n%s", direction, size, joined)
				assert.NotContains(t, joined, "Sort", "%s, size %d sorts instead of reading the index in order:\n%s", direction, size, joined)
				// The first message of each conversation of the page, and of no other: one row of the messages for each row of the page,
				// read through the key and never by scanning the table.
				assert.LessOrEqual(t, rowsRead(t, plan, "conversation_messages"), 1, "%s, size %d reads more than the first message of a conversation:\n%s", direction, size, joined)
				assert.NotContains(t, joined, "Seq Scan on conversation_messages", "%s, size %d scans the messages:\n%s", direction, size, joined)
				assert.LessOrEqual(t, loopsOver(plan, "conversation_messages"), size+1, "%s, size %d looks up more conversations than the page has:\n%s", direction, size, joined)
				assert.Contains(t, joined, "conversation_summaries_list_idx", "%s, size %d does not use the list index:\n%s", direction, size, joined)
				// The index and the row of the summaries, and for each conversation of the page the lookup of its first message in the key of
				// the messages: about five pages each, whatever the size of the account or of the table.
				assert.LessOrEqual(t, pagesTouched(t, plan), pagesPerConversation*(size+1)+8, "%s, size %d touches %d pages", direction, size, pagesTouched(t, plan))
			}
		}
	})

	t.Run("theCostFollowsTheRowsAPageReadsAndNotTheSizeOfTheAccount", func(t *testing.T) {
		for _, direction := range []string{"newest", "before", "after"} {
			for _, account := range []string{heavyAccount, smallAccount} {
				plan := planFor(account, direction, 10)
				measure(account+", "+direction, plan)

				// About a few pages for each row read (the summary and its first message), plus the index: the same for an account
				// with five thousand conversations as for one with ten.
				assert.LessOrEqual(t, pagesTouched(t, plan), pagesPerConversation*rowsRead(t, plan, "conversation_summaries")+4, "%s, %s", account, direction)
			}
		}
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
			oldest := list.Conversations[len(list.Conversations)-1]
			query = domain.ConversationQuery{IDYapeAccount: heavyAccount, Direction: domain.DirectionBefore, Size: domain.ConversationListSize,
				Cursor: domain.NewConversationCursor(oldest.LastOccurredAt, oldest.ConversationID)}
		}

		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		t.Logf("MEASURE walk of %d conversations in %d pages: %s; page latency median %s, slowest %s",
			len(seen), len(latencies), time.Since(started).Round(time.Millisecond),
			latencies[len(latencies)/2].Round(10*time.Microsecond), latencies[len(latencies)-1].Round(10*time.Microsecond))
		assert.Len(t, seen, heavyConversations)
		assert.Less(t, latencies[len(latencies)-1], 3*time.Second, "the slowest page")
	})
}

// loopsOver is the most times any scan of the table ran: for the first message, once for each conversation of the page.
func loopsOver(plan []string, table string) int {
	most := 0
	for _, line := range plan {
		if !strings.Contains(line, table) || !strings.Contains(line, "Scan") {
			continue
		}
		if match := scanLoops.FindStringSubmatch(line); match != nil {
			n, _ := strconv.Atoi(match[1])
			most = max(most, n)
		}
	}

	return most
}

// pagesPerConversation is the most pages a conversation of a list page costs: its summary and the lookup of its first message.
const pagesPerConversation = 6

var scanLoops = regexp.MustCompile(`loops=(\d+)`)
