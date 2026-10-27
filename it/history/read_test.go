package history_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql/repository"
)

func sequenceRange(from, to int) []int {
	var out []int
	for s := from; s <= to; s++ {
		out = append(out, s)
	}

	return out
}

func sequencesOf(messages []domain.Message) []int {
	out := make([]int, 0, len(messages))
	for _, m := range messages {
		out = append(out, m.Sequence)
	}

	return out
}

func TestRead(t *testing.T) {
	pool := newPool(t)
	migrateUp(t, pool)
	ctx := context.Background()
	repo := repository.NewPostgresMessageRepository(pool)

	// "big" holds 250 messages, sequences 1 to 250; "exact" holds
	// exactly one page; "small" and "other" belong to other accounts.
	_, err := pool.Exec(ctx, `
		INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
		SELECT 'big', s, CASE WHEN s % 2 = 1 THEN 'appUser' ELSE 'appMaker' END, 'a', 'm' || s, 'UserMessageSent', 'acc-1', '2026-10-20T10:00:00Z'::timestamptz + s * interval '1 second'
		FROM generate_series(1, 250) s;
		INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
		SELECT 'exact', s, 'appUser', 'a', 'm' || s, 'UserMessageSent', 'acc-1', '2026-10-20T12:00:00Z'::timestamptz + s * interval '1 second'
		FROM generate_series(1, 100) s;
		INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
		VALUES ('small', 2, 'appUser', 'a', 'hola', 'UserMessageSent', 'acc-1', '2026-10-20T11:00:00Z'),
		       ('small', 5, 'appMaker', 'a', 'chau', 'AgentMessageSent', 'acc-1', '2026-10-20T11:05:00Z'),
		       ('other', 1, 'appUser', 'a', 'x', 'UserMessageSent', 'acc-2', '2026-10-20T13:00:00Z'),
		       ('noaccount', 1, 'appMaker', 'a', 'y', 'GuardrailResponseSent', NULL, '2026-10-20T13:30:00Z')`)
	require.NoError(t, err)
	refreshSummaries(t, pool)

	t.Run("pages", func(t *testing.T) {
		tests := []struct {
			name        string
			query       domain.PageQuery
			wantSeqs    []int
			wantHasMore bool
			wantErr     error
		}{
			{name: "newestPage", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "big", Direction: domain.DirectionNewest}, wantSeqs: sequenceRange(151, 250), wantHasMore: true},
			{name: "beforeTheOldestOfTheNewestPage", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "big", Direction: domain.DirectionBefore, CursorSequence: 151}, wantSeqs: sequenceRange(51, 150), wantHasMore: true},
			{name: "beforeLeavesTheFirstFifty", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "big", Direction: domain.DirectionBefore, CursorSequence: 51}, wantSeqs: sequenceRange(1, 50)},
			{name: "beforeTheVeryFirstMessageIsEmpty", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "big", Direction: domain.DirectionBefore, CursorSequence: 1}, wantSeqs: []int{}},
			{name: "afterTheFirstHundred", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: 100}, wantSeqs: sequenceRange(101, 200), wantHasMore: true},
			{name: "afterLeavesTheLastFifty", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: 200}, wantSeqs: sequenceRange(201, 250)},
			{name: "afterTheLastMessageIsEmpty", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: 250}, wantSeqs: []int{}},
			{name: "aConversationOfExactlyOnePage", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "exact", Direction: domain.DirectionNewest}, wantSeqs: sequenceRange(1, 100)},
			{name: "gapsInTheSequenceAreFine", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "small", Direction: domain.DirectionNewest}, wantSeqs: []int{2, 5}},
			{name: "afterAMessageAcrossAGap", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "small", Direction: domain.DirectionAfter, CursorSequence: 2}, wantSeqs: []int{5}},
			{name: "beforeAMessageAcrossAGap", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "small", Direction: domain.DirectionBefore, CursorSequence: 5}, wantSeqs: []int{2}},
			{name: "aSmallPageOfTheNewest", query: domain.PageQuery{Size: 7, ConversationID: "big", Direction: domain.DirectionNewest}, wantSeqs: sequenceRange(244, 250), wantHasMore: true},
			{name: "aSmallPageBefore", query: domain.PageQuery{Size: 7, ConversationID: "big", Direction: domain.DirectionBefore, CursorSequence: 244}, wantSeqs: sequenceRange(237, 243), wantHasMore: true},
			{name: "aSmallPageAfter", query: domain.PageQuery{Size: 7, ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: 100}, wantSeqs: sequenceRange(101, 107), wantHasMore: true},
			{name: "aSmallPageThatEndsTheConversationHasNoMore", query: domain.PageQuery{Size: 7, ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: 243}, wantSeqs: sequenceRange(244, 250)},
			{name: "aSizeOfOne", query: domain.PageQuery{Size: 1, ConversationID: "small", Direction: domain.DirectionNewest}, wantSeqs: []int{5}, wantHasMore: true},
			{name: "unknownConversation", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "nope", Direction: domain.DirectionNewest}, wantErr: domain.ErrNotFound},
			{name: "cursorThatIsNoMessageOfTheConversation", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "big", Direction: domain.DirectionBefore, CursorSequence: 999}, wantErr: domain.ErrInvalidCursor},
			{name: "cursorThatDoesNotExist", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: 424242}, wantErr: domain.ErrInvalidCursor},
			{name: "cursorInAnUnknownConversation", query: domain.PageQuery{Size: domain.PageSize, ConversationID: "nope", Direction: domain.DirectionAfter, CursorSequence: 1}, wantErr: domain.ErrNotFound},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				page, err := repo.ListMessages(ctx, tt.query)

				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tt.wantSeqs, sequencesOf(page.Messages))
				assert.Equal(t, tt.wantHasMore, page.HasMore)
			})
		}
	})

	t.Run("walkingBackThroughAConversationVisitsEveryMessageOnce", func(t *testing.T) {
		var got []int
		query := domain.PageQuery{Size: domain.PageSize, ConversationID: "big", Direction: domain.DirectionNewest}
		for pages := 0; ; pages++ {
			require.Less(t, pages, 10, "the walk ends")
			page, err := repo.ListMessages(ctx, query)
			require.NoError(t, err)
			got = append(sequencesOf(page.Messages), got...)
			if !page.HasMore {
				break
			}
			query = domain.PageQuery{Size: domain.PageSize, ConversationID: "big", Direction: domain.DirectionBefore, CursorSequence: page.Messages[0].Sequence}
		}

		assert.Equal(t, sequenceRange(1, 250), got)
	})

	t.Run("walkingForwardThroughAConversationVisitsEveryMessageOnce", func(t *testing.T) {
		got := sequenceRange(1, 1) // the first message is the starting cursor
		query := domain.PageQuery{Size: domain.PageSize, ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: 1}
		for pages := 0; ; pages++ {
			require.Less(t, pages, 10, "the walk ends")
			page, err := repo.ListMessages(ctx, query)
			require.NoError(t, err)
			got = append(got, sequencesOf(page.Messages)...)
			if !page.HasMore {
				break
			}
			query = domain.PageQuery{Size: domain.PageSize, ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: page.Messages[len(page.Messages)-1].Sequence}
		}

		assert.Equal(t, sequenceRange(1, 250), got)
	})

	t.Run("walkingBackWithASmallPageSizeVisitsEveryMessageOnce", func(t *testing.T) {
		var got []int
		query := domain.PageQuery{Size: 33, ConversationID: "big", Direction: domain.DirectionNewest}
		for pages := 0; ; pages++ {
			require.Less(t, pages, 10, "the walk ends")
			page, err := repo.ListMessages(ctx, query)
			require.NoError(t, err)
			require.LessOrEqual(t, len(page.Messages), 33)
			got = append(sequencesOf(page.Messages), got...)
			if !page.HasMore {
				break
			}
			query = domain.PageQuery{Size: 33, ConversationID: "big", Direction: domain.DirectionBefore, CursorSequence: page.Messages[0].Sequence}
		}

		assert.Equal(t, sequenceRange(1, 250), got)
	})

	t.Run("messageFieldsAreRead", func(t *testing.T) {
		page, err := repo.ListMessages(ctx, domain.PageQuery{Size: domain.PageSize, ConversationID: "noaccount", Direction: domain.DirectionNewest})

		require.NoError(t, err)
		require.Len(t, page.Messages, 1)
		m := page.Messages[0]
		assert.Equal(t, "noaccount", m.ConversationID)
		assert.Equal(t, 1, m.Sequence)
		assert.Equal(t, domain.RoleMaker, m.Role)
		assert.Equal(t, "text", m.Type)
		assert.Equal(t, "GuardrailResponseSent", m.EventName)
		assert.Empty(t, m.IDYapeAccount, "a NULL account reads as empty")
		assert.False(t, m.Truncated)
		assert.Equal(t, 2026, m.OccurredAt.Year())
	})

	t.Run("conversationsOfAnAccount", func(t *testing.T) {
		tests := []struct {
			account string
			want    []string
			counts  []int
		}{
			{account: "acc-1", want: []string{"exact", "small", "big"}, counts: []int{100, 2, 250}},
			{account: "acc-2", want: []string{"other"}, counts: []int{1}},
			{account: "nobody", want: []string{}, counts: []int{}},
		}

		for _, tt := range tests {
			t.Run(tt.account, func(t *testing.T) {
				list, err := repo.ListConversations(ctx, domain.ConversationQuery{IDYapeAccount: tt.account, Direction: domain.DirectionNewest, Size: domain.ConversationListSize})

				require.NoError(t, err)
				ids, counts := []string{}, []int{}
				for _, c := range list.Conversations {
					ids, counts = append(ids, c.ConversationID), append(counts, c.MessageCount)
				}
				assert.Equal(t, tt.want, ids, "newest conversation first")
				assert.Equal(t, tt.counts, counts)
				assert.False(t, list.HasMore)
			})
		}
	})

	t.Run("conversationTimesSpanItsMessages", func(t *testing.T) {
		list, err := repo.ListConversations(ctx, domain.ConversationQuery{IDYapeAccount: "acc-1", Direction: domain.DirectionNewest, Size: domain.ConversationListSize})

		require.NoError(t, err)
		for _, c := range list.Conversations {
			if c.ConversationID != "small" {
				continue
			}
			assert.Equal(t, "2026-10-20T11:00:00Z", c.FirstOccurredAt.UTC().Format("2006-01-02T15:04:05Z"))
			assert.Equal(t, "2026-10-20T11:05:00Z", c.LastOccurredAt.UTC().Format("2006-01-02T15:04:05Z"))
		}
	})

	t.Run("moreThanOneListPageSaysSo", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
			SELECT 'many-' || lpad(s::text, 3, '0'), 1, 'appUser', 'a', 'x', 'UserMessageSent', 'acc-many', '2026-10-21T00:00:00Z'::timestamptz + s * interval '1 second'
			FROM generate_series(1, 120) s`)
		require.NoError(t, err)
		refreshSummaries(t, pool)

		list, err := repo.ListConversations(ctx, domain.ConversationQuery{IDYapeAccount: "acc-many", Direction: domain.DirectionNewest, Size: domain.ConversationListSize})

		require.NoError(t, err)
		assert.Len(t, list.Conversations, domain.ConversationListSize)
		assert.True(t, list.HasMore)
		assert.Equal(t, "many-120", list.Conversations[0].ConversationID, fmt.Sprintf("newest first, got %s", list.Conversations[0].ConversationID))
	})

	// idsOf reads the ids of a page of conversations, in the order the page has them.
	idsOf := func(list domain.ConversationList) []string {
		ids := make([]string, 0, len(list.Conversations))
		for _, c := range list.Conversations {
			ids = append(ids, c.ConversationID)
		}

		return ids
	}
	// positionOf is the cursor of a conversation as it stands now: the time of its last message and its id.
	positionOf := func(id string) domain.ConversationCursor {
		var last time.Time
		require.NoError(t, pool.QueryRow(ctx, `SELECT max(occurred_at) FROM conversation_messages WHERE conversation_id = $1`, id).Scan(&last))

		return domain.NewConversationCursor(last, id)
	}
	// conversations reads a page; cursorID names the conversation whose current position is the cursor.
	conversations := func(account string, direction domain.Direction, cursorID string, size int) (domain.ConversationList, error) {
		query := domain.ConversationQuery{IDYapeAccount: account, Direction: direction, Size: size}
		if cursorID != "" {
			query.Cursor = positionOf(cursorID)
		}

		return repo.ListConversations(ctx, query)
	}

	t.Run("conversationPages", func(t *testing.T) {
		tests := []struct {
			name        string
			direction   domain.Direction
			cursor      string
			size        int
			want        []string
			wantHasMore bool
		}{
			{name: "mostRecentOneOfThree", direction: domain.DirectionNewest, size: 1, want: []string{"exact"}, wantHasMore: true},
			{name: "mostRecentTwoOfThree", direction: domain.DirectionNewest, size: 2, want: []string{"exact", "small"}, wantHasMore: true},
			{name: "allThreeInAPageOfThreeHaveNoMore", direction: domain.DirectionNewest, size: 3, want: []string{"exact", "small", "big"}},
			{name: "beforeTheMostRecentIsTheNextOlderOne", direction: domain.DirectionBefore, cursor: "exact", size: 1, want: []string{"small"}, wantHasMore: true},
			{name: "beforeSmallIsTheOldestOne", direction: domain.DirectionBefore, cursor: "small", size: 1, want: []string{"big"}},
			{name: "beforeTheOldestIsEmpty", direction: domain.DirectionBefore, cursor: "big", size: 5, want: []string{}},
			{name: "afterTheOldestReadsUpwardsButAnswersMostRecentFirst", direction: domain.DirectionAfter, cursor: "big", size: 2, want: []string{"exact", "small"}},
			{name: "afterTheOldestWithRoomForOneMoreRecent", direction: domain.DirectionAfter, cursor: "big", size: 1, want: []string{"small"}, wantHasMore: true},
			{name: "afterTheMostRecentIsEmpty", direction: domain.DirectionAfter, cursor: "exact", size: 5, want: []string{}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				list, err := conversations("acc-1", tt.direction, tt.cursor, tt.size)

				require.NoError(t, err)
				assert.Equal(t, tt.want, idsOf(list))
				assert.Equal(t, tt.wantHasMore, list.HasMore)
			})
		}
	})

	t.Run("aCursorIsAPositionAndNeedsNoConversationOfTheAccount", func(t *testing.T) {
		// "other" belongs to another account and is more recent than all three of acc-1's: as a cursor
		// it is only the position 2026-10-20T13:00:00Z, which says nothing about the other account.
		older, err := conversations("acc-1", domain.DirectionBefore, "other", 10)
		require.NoError(t, err)
		assert.Equal(t, []string{"exact", "small", "big"}, idsOf(older))

		newer, err := conversations("acc-1", domain.DirectionAfter, "other", 10)
		require.NoError(t, err)
		assert.Empty(t, newer.Conversations)

		// A position that no conversation holds works the same.
		between, err := repo.ListConversations(ctx, domain.ConversationQuery{
			IDYapeAccount: "acc-1", Direction: domain.DirectionBefore, Size: 10,
			Cursor: domain.NewConversationCursor(time.Date(2026, 10, 20, 11, 30, 0, 0, time.UTC), "zzz"),
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"small", "big"}, idsOf(between), "the conversations last active before 11:30 are small (11:05) and big (10:04)")
	})

	t.Run("aConversationThatMovesBetweenPagesDoesNotRepeatOrSkipAnyOther", func(t *testing.T) {
		// Four conversations, newest first: c4 c3 c2 c1. The client reads two, then c3 (the cursor of
		// page 1) receives a message and jumps to the top before the client asks for page 2.
		_, err := pool.Exec(ctx, `
			INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
			VALUES ('move-c1', 1, 'appUser', 'a', 'x', 'UserMessageSent', 'acc-move', '2026-10-27T10:00:00Z'),
			       ('move-c2', 1, 'appUser', 'a', 'x', 'UserMessageSent', 'acc-move', '2026-10-27T11:00:00Z'),
			       ('move-c3', 1, 'appUser', 'a', 'x', 'UserMessageSent', 'acc-move', '2026-10-27T12:00:00Z'),
			       ('move-c4', 1, 'appUser', 'a', 'x', 'UserMessageSent', 'acc-move', '2026-10-27T13:00:00Z')`)
		require.NoError(t, err)
		refreshSummaries(t, pool)

		page1, err := repo.ListConversations(ctx, domain.ConversationQuery{IDYapeAccount: "acc-move", Direction: domain.DirectionNewest, Size: 2})
		require.NoError(t, err)
		require.Equal(t, []string{"move-c4", "move-c3"}, idsOf(page1))
		oldest := page1.Conversations[1]
		cursor := domain.NewConversationCursor(oldest.LastOccurredAt, oldest.ConversationID) // what the Link carries

		_, err = pool.Exec(ctx, `INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
			VALUES ('move-c3', 2, 'appUser', 'a', 'again', 'UserMessageSent', 'acc-move', '2026-10-27T14:00:00Z')`)
		require.NoError(t, err)
		refreshSummaries(t, pool)

		page2, err := repo.ListConversations(ctx, domain.ConversationQuery{IDYapeAccount: "acc-move", Direction: domain.DirectionBefore, Cursor: cursor, Size: 2})
		require.NoError(t, err)
		assert.Equal(t, []string{"move-c2", "move-c1"}, idsOf(page2), "page 2 is where page 1 ended, whatever happened to the cursor conversation since")
	})

	t.Run("catchUpFromTheCursorOfAConversationThatMovedFindsItAndTheOnesBeforeIt", func(t *testing.T) {
		// The client saw c4 at the top with the cursor it was given. Later c3 jumped above it. Reading
		// "after" that cursor returns what is more recent than where c4 stood, not than where it is now.
		_, err := pool.Exec(ctx, `
			INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
			VALUES ('catch-c1', 1, 'appUser', 'a', 'x', 'UserMessageSent', 'acc-catch', '2026-10-27T10:00:00Z')`)
		require.NoError(t, err)
		refreshSummaries(t, pool)
		seen, err := repo.ListConversations(ctx, domain.ConversationQuery{IDYapeAccount: "acc-catch", Direction: domain.DirectionNewest, Size: 1})
		require.NoError(t, err)
		topCursor := domain.NewConversationCursor(seen.Conversations[0].LastOccurredAt, seen.Conversations[0].ConversationID)

		// A new conversation appears, then the one the client holds is active again, above it.
		_, err = pool.Exec(ctx, `
			INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
			VALUES ('catch-d1', 1, 'appUser', 'a', 'x', 'UserMessageSent', 'acc-catch', '2026-10-27T11:00:00Z'),
			       ('catch-c1', 2, 'appUser', 'a', 'again', 'UserMessageSent', 'acc-catch', '2026-10-27T12:00:00Z')`)
		require.NoError(t, err)
		refreshSummaries(t, pool)

		news, err := repo.ListConversations(ctx, domain.ConversationQuery{IDYapeAccount: "acc-catch", Direction: domain.DirectionAfter, Cursor: topCursor, Size: 10})
		require.NoError(t, err)
		assert.Equal(t, []string{"catch-c1", "catch-d1"}, idsOf(news), "the new conversation is found, and so is the one that moved")
	})

	t.Run("walkingBackThroughConversationsVisitsEachOnce", func(t *testing.T) {
		var got []string
		list, err := conversations("acc-1", domain.DirectionNewest, "", 1)
		for pages := 0; ; pages++ {
			require.NoError(t, err)
			require.Less(t, pages, 10, "the walk ends")
			got = append(got, idsOf(list)...)
			if !list.HasMore {
				break
			}
			list, err = conversations("acc-1", domain.DirectionBefore, list.Conversations[len(list.Conversations)-1].ConversationID, 1)
		}

		assert.Equal(t, []string{"exact", "small", "big"}, got)
	})

	t.Run("walkingForwardThroughConversationsVisitsEachOnce", func(t *testing.T) {
		got := []string{"big"} // the oldest is the starting cursor
		list, err := conversations("acc-1", domain.DirectionAfter, "big", 1)
		for pages := 0; ; pages++ {
			require.NoError(t, err)
			require.Less(t, pages, 10, "the walk ends")
			got = append(idsOf(list), got...)
			if !list.HasMore {
				break
			}
			list, err = conversations("acc-1", domain.DirectionAfter, list.Conversations[0].ConversationID, 1)
		}

		assert.Equal(t, []string{"exact", "small", "big"}, got)
	})

	t.Run("conversationsWithTheSameLastMessageTimeAreOrderedByIDAndNeverRepeatedOrSkipped", func(t *testing.T) {
		// Five conversations whose last message is the same instant: only the id tells them apart.
		_, err := pool.Exec(ctx, `
			INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
			SELECT 'tie-' || c, s, 'appUser', 'a', 'x', 'UserMessageSent', 'acc-tie', '2026-10-22T10:00:00Z'::timestamptz
			FROM unnest(ARRAY['a','b','c','d','e']) c, generate_series(1, 2) s`)
		require.NoError(t, err)
		refreshSummaries(t, pool)
		want := []string{"tie-e", "tie-d", "tie-c", "tie-b", "tie-a"} // most recent first, then by id, the larger first

		for _, size := range []int{1, 2, 3} {
			var back []string
			list, err := conversations("acc-tie", domain.DirectionNewest, "", size)
			for pages := 0; ; pages++ {
				require.NoError(t, err)
				require.Less(t, pages, 10, "the walk ends")
				back = append(back, idsOf(list)...)
				if !list.HasMore {
					break
				}
				list, err = conversations("acc-tie", domain.DirectionBefore, list.Conversations[len(list.Conversations)-1].ConversationID, size)
			}
			assert.Equal(t, want, back, "size %d walking back", size)

			forward := []string{"tie-a"} // the last of the list is where a forward walk starts
			list, err = conversations("acc-tie", domain.DirectionAfter, "tie-a", size)
			for pages := 0; ; pages++ {
				require.NoError(t, err)
				require.Less(t, pages, 10, "the walk ends")
				forward = append(idsOf(list), forward...)
				if !list.HasMore {
					break
				}
				list, err = conversations("acc-tie", domain.DirectionAfter, list.Conversations[0].ConversationID, size)
			}
			assert.Equal(t, want, forward, "size %d walking forward", size)
		}
	})

	t.Run("walkingTheBigAccountAtTheMaximumAndAtAnOddSizeVisitsEveryConversationOnce", func(t *testing.T) {
		for _, size := range []int{domain.ConversationListSize, 7} {
			seen := map[string]bool{}
			total := 0
			list, err := conversations("acc-many", domain.DirectionNewest, "", size)
			for pages := 0; ; pages++ {
				require.NoError(t, err)
				require.Less(t, pages, 30, "the walk ends")
				require.LessOrEqual(t, len(list.Conversations), size)
				for _, c := range list.Conversations {
					require.False(t, seen[c.ConversationID], "size %d: %s came twice", size, c.ConversationID)
					seen[c.ConversationID] = true
					total++
				}
				if !list.HasMore {
					break
				}
				list, err = conversations("acc-many", domain.DirectionBefore, list.Conversations[len(list.Conversations)-1].ConversationID, size)
			}
			assert.Equal(t, 120, total, "size %d", size)
		}
	})

}
