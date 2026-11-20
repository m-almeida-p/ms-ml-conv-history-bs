package history_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql/repository"
)

// A conversation of the list says what it was about: its first message from the user, in the same statement as the list.
func TestConversationsCarryTheirFirstMessage(t *testing.T) {
	pool, _ := newHistoryDB(t)
	ctx := context.Background()
	const account = "acc-first"

	// say writes one message of a conversation: sequence, who wrote it and what.
	say := func(conversation string, sequence int, role, text string, truncated bool) {
		t.Helper()
		event := "UserMessageSent"
		if role == "appMaker" {
			event = "AgentMessageSent"
		}
		_, err := pool.Exec(ctx, `
			INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, truncated, event_name, id_yape_account, occurred_at)
			VALUES ($1, $2, $3, 'a', $4, $5, $6, $7, '2026-10-21T10:00:00Z'::timestamptz + $2::int * interval '1 second')`,
			conversation, sequence, role, text, truncated, event, account)
		require.NoError(t, err)
	}
	say("one", 1, "appUser", "quiero yapear", false)

	say("two", 1, "appUser", "hola", false)
	say("two", 2, "appMaker", "¿en qué te ayudo?", false)

	say("agent-first", 1, "appMaker", "bienvenido", false)
	say("agent-first", 2, "appUser", "mi primer mensaje", false)
	say("agent-first", 3, "appUser", "el segundo", false)

	say("agent-only", 1, "appMaker", "aviso", false)

	// a long conversation: its first message is still the first, among hundreds
	say("long", 1, "appUser", "el primero de todos", false)
	for s := 2; s <= 300; s++ {
		role := "appUser"
		if s%2 == 0 {
			role = "appMaker"
		}
		say("long", s, role, "otro", false)
	}

	say("cut", 1, "appUser", strings.Repeat("ñ", 300), false)
	say("emoji", 1, "appUser", strings.Repeat("🙂", 250), false)
	say("exact", 1, "appUser", strings.Repeat("x", domain.FirstMessageMaxChars), false)
	say("over-by-one", 1, "appUser", strings.Repeat("x", domain.FirstMessageMaxChars+1), false)
	say("agent-cut", 1, "appUser", "el agente lo cortó", true)
	say("markup", 1, "appUser", `<script>alert("x")</script> ' OR 1=1; --`, false)
	refreshSummaries(t, pool)

	list, err := repository.NewPostgresMessageRepository(pool).ListConversations(ctx,
		domain.ConversationQuery{IDYapeAccount: account, Direction: domain.DirectionNewest, Size: domain.ConversationListSize})
	require.NoError(t, err)
	byID := map[string]domain.ConversationSummary{}
	for _, c := range list.Conversations {
		byID[c.ConversationID] = c
	}
	require.Len(t, byID, 11)

	for name, tc := range map[string]struct {
		first     string
		truncated bool
	}{
		"one":         {"quiero yapear", false},
		"two":         {"hola", false},
		"agent-first": {"mi primer mensaje", false}, // the first message of the user, not of the conversation
		"agent-only":  {"", false},                  // nobody from the user said anything
		"long":        {"el primero de todos", false},
		"cut":         {strings.Repeat("ñ", domain.FirstMessageMaxChars), true},
		"emoji":       {strings.Repeat("🙂", domain.FirstMessageMaxChars), true}, // characters, not bytes, not half an emoji
		"exact":       {strings.Repeat("x", domain.FirstMessageMaxChars), false},
		"over-by-one": {strings.Repeat("x", domain.FirstMessageMaxChars), true},
		"agent-cut":   {"el agente lo cortó", true},
		"markup":      {`<script>alert("x")</script> ' OR 1=1; --`, false}, // data: never executed, never interpreted
	} {
		t.Run(name, func(t *testing.T) {
			got := byID[name]

			assert.Equal(t, tc.first, got.FirstMessage)
			assert.Equal(t, tc.truncated, got.FirstMessageTruncated)
		})
	}

	t.Run("the other fields of the list are what they were", func(t *testing.T) {
		assert.Equal(t, 300, byID["long"].MessageCount)
		assert.Equal(t, 2, byID["two"].MessageCount)
	})

	t.Run("a page beside a cursor carries them too", func(t *testing.T) {
		page := list.Conversations
		cursor := domain.ConversationCursor{LastOccurredAt: page[1].LastOccurredAt, ConversationID: page[1].ConversationID}
		older, err := repository.NewPostgresMessageRepository(pool).ListConversations(ctx,
			domain.ConversationQuery{IDYapeAccount: account, Direction: domain.DirectionBefore, Cursor: cursor, Size: 3})
		require.NoError(t, err)
		newer, err := repository.NewPostgresMessageRepository(pool).ListConversations(ctx,
			domain.ConversationQuery{IDYapeAccount: account, Direction: domain.DirectionAfter, Cursor: cursor, Size: 3})
		require.NoError(t, err)

		for _, c := range append(older.Conversations, newer.Conversations...) {
			assert.Equal(t, byID[c.ConversationID].FirstMessage, c.FirstMessage, c.ConversationID)
			assert.Equal(t, byID[c.ConversationID].FirstMessageTruncated, c.FirstMessageTruncated, c.ConversationID)
		}
		assert.NotEmpty(t, older.Conversations)
		assert.NotEmpty(t, newer.Conversations)
	})
}

// A conversation id is the agent's, and the agent does not check whose it is, so two accounts can end up with messages in one
// conversation. Each account reads what is its own and nothing of the other's: the messages, and the first message of its row.
func TestAConversationThatTwoAccountsWroteIntoIsReadByEachForItsOwn(t *testing.T) {
	pool, _ := newHistoryDB(t)
	ctx := context.Background()
	say := func(sequence int, role, text, account string) {
		t.Helper()
		_, err := pool.Exec(ctx, `
			INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
			VALUES ('shared', $1, $2, 'a', $3, 'UserMessageSent', $4, '2026-10-21T10:00:00Z'::timestamptz + $1::int * interval '1 second')`,
			sequence, role, text, account)
		require.NoError(t, err)
	}
	say(1, "appUser", "secreto de A", "acc-a")
	say(2, "appMaker", "respuesta para A", "acc-a")
	say(3, "appUser", "mensaje de B", "acc-b")
	say(4, "appMaker", "respuesta para B", "acc-b")
	refreshSummaries(t, pool)
	repo := repository.NewPostgresMessageRepository(pool)

	firstOfTheRow := map[string]string{"acc-a": "secreto de A", "acc-b": ""} // the first of the conversation is A's: B's row shows none, and not A's
	for account, want := range map[string][]string{"acc-a": {"secreto de A", "respuesta para A"}, "acc-b": {"mensaje de B", "respuesta para B"}} {
		page, err := repo.ListMessages(ctx, domain.PageQuery{IDYapeAccount: account, ConversationID: "shared", Direction: domain.DirectionNewest, Size: domain.PageSize})
		require.NoError(t, err, account)
		var got []string
		for _, m := range page.Messages {
			got = append(got, m.Text)
		}
		assert.Equal(t, want, got, "%s reads its own messages of the shared conversation, and no other", account)

		list, err := repo.ListConversations(ctx, domain.ConversationQuery{IDYapeAccount: account, Direction: domain.DirectionNewest, Size: domain.ConversationListSize})
		require.NoError(t, err, account)
		require.Len(t, list.Conversations, 1, account)
		assert.Equal(t, firstOfTheRow[account], list.Conversations[0].FirstMessage, "%s: the row never shows a first message of the other account", account)
	}
}
