package history_test

import (
	"context"
	"fmt"
	"testing"

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

	t.Run("pages", func(t *testing.T) {
		tests := []struct {
			name        string
			query       domain.PageQuery
			wantSeqs    []int
			wantHasMore bool
			wantErr     error
		}{
			{name: "newestPage", query: domain.PageQuery{ConversationID: "big", Direction: domain.DirectionNewest}, wantSeqs: sequenceRange(151, 250), wantHasMore: true},
			{name: "beforeTheOldestOfTheNewestPage", query: domain.PageQuery{ConversationID: "big", Direction: domain.DirectionBefore, CursorSequence: 151}, wantSeqs: sequenceRange(51, 150), wantHasMore: true},
			{name: "beforeLeavesTheFirstFifty", query: domain.PageQuery{ConversationID: "big", Direction: domain.DirectionBefore, CursorSequence: 51}, wantSeqs: sequenceRange(1, 50)},
			{name: "beforeTheVeryFirstMessageIsEmpty", query: domain.PageQuery{ConversationID: "big", Direction: domain.DirectionBefore, CursorSequence: 1}, wantSeqs: []int{}},
			{name: "afterTheFirstHundred", query: domain.PageQuery{ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: 100}, wantSeqs: sequenceRange(101, 200), wantHasMore: true},
			{name: "afterLeavesTheLastFifty", query: domain.PageQuery{ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: 200}, wantSeqs: sequenceRange(201, 250)},
			{name: "afterTheLastMessageIsEmpty", query: domain.PageQuery{ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: 250}, wantSeqs: []int{}},
			{name: "aConversationOfExactlyOnePage", query: domain.PageQuery{ConversationID: "exact", Direction: domain.DirectionNewest}, wantSeqs: sequenceRange(1, 100)},
			{name: "gapsInTheSequenceAreFine", query: domain.PageQuery{ConversationID: "small", Direction: domain.DirectionNewest}, wantSeqs: []int{2, 5}},
			{name: "afterAMessageAcrossAGap", query: domain.PageQuery{ConversationID: "small", Direction: domain.DirectionAfter, CursorSequence: 2}, wantSeqs: []int{5}},
			{name: "beforeAMessageAcrossAGap", query: domain.PageQuery{ConversationID: "small", Direction: domain.DirectionBefore, CursorSequence: 5}, wantSeqs: []int{2}},
			{name: "unknownConversation", query: domain.PageQuery{ConversationID: "nope", Direction: domain.DirectionNewest}, wantErr: domain.ErrNotFound},
			{name: "cursorThatIsNoMessageOfTheConversation", query: domain.PageQuery{ConversationID: "big", Direction: domain.DirectionBefore, CursorSequence: 999}, wantErr: domain.ErrInvalidCursor},
			{name: "cursorThatDoesNotExist", query: domain.PageQuery{ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: 424242}, wantErr: domain.ErrInvalidCursor},
			{name: "cursorInAnUnknownConversation", query: domain.PageQuery{ConversationID: "nope", Direction: domain.DirectionAfter, CursorSequence: 1}, wantErr: domain.ErrNotFound},
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
		query := domain.PageQuery{ConversationID: "big", Direction: domain.DirectionNewest}
		for pages := 0; ; pages++ {
			require.Less(t, pages, 10, "the walk ends")
			page, err := repo.ListMessages(ctx, query)
			require.NoError(t, err)
			got = append(sequencesOf(page.Messages), got...)
			if !page.HasMore {
				break
			}
			query = domain.PageQuery{ConversationID: "big", Direction: domain.DirectionBefore, CursorSequence: page.Messages[0].Sequence}
		}

		assert.Equal(t, sequenceRange(1, 250), got)
	})

	t.Run("walkingForwardThroughAConversationVisitsEveryMessageOnce", func(t *testing.T) {
		got := sequenceRange(1, 1) // the first message is the starting cursor
		query := domain.PageQuery{ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: 1}
		for pages := 0; ; pages++ {
			require.Less(t, pages, 10, "the walk ends")
			page, err := repo.ListMessages(ctx, query)
			require.NoError(t, err)
			got = append(got, sequencesOf(page.Messages)...)
			if !page.HasMore {
				break
			}
			query = domain.PageQuery{ConversationID: "big", Direction: domain.DirectionAfter, CursorSequence: page.Messages[len(page.Messages)-1].Sequence}
		}

		assert.Equal(t, sequenceRange(1, 250), got)
	})

	t.Run("messageFieldsAreRead", func(t *testing.T) {
		page, err := repo.ListMessages(ctx, domain.PageQuery{ConversationID: "noaccount", Direction: domain.DirectionNewest})

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
				list, err := repo.ListConversations(ctx, tt.account)

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
		list, err := repo.ListConversations(ctx, "acc-1")

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

		list, err := repo.ListConversations(ctx, "acc-many")

		require.NoError(t, err)
		assert.Len(t, list.Conversations, domain.ConversationListSize)
		assert.True(t, list.HasMore)
		assert.Equal(t, "many-120", list.Conversations[0].ConversationID, fmt.Sprintf("newest first, got %s", list.Conversations[0].ConversationID))
	})
}
