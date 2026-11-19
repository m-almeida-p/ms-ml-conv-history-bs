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
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql/repository"
)

// attempt describes one P2P conversation as the agent writes it, with the times that matter to the
// statistics: the request opens the segment, the card comes cardAfter later and the person decides
// resolveAfter after the card.
type attempt struct {
	account      string
	conversation string
	start        time.Time
	// decision is approved, rejected or failed; empty leaves the card unresolved. It is ignored
	// when noCard is set.
	decision     string
	otp          bool
	noCard       bool
	failedTool   bool
	cardAfter    time.Duration
	resolveAfter time.Duration
}

// play writes the attempt as the consumer left it: every event in the inbox, and the chat messages (the user's and
// the agent's text) in the messages table. It is plain SQL, because the writing is done by the Camel consumers now.
func play(t *testing.T, pool *pgxpool.Pool, a attempt) {
	t.Helper()

	type step struct {
		name, actor, flow string
		at                time.Duration
		payload           map[string]any
	}
	ref := "ref-" + a.conversation
	cardAt := a.cardAfter
	steps := []step{
		{"ConversationStarted", "system", "", 0, map[string]any{"channel": "a2a"}},
		{"UserMessageSent", "user", "", 0, map[string]any{"text": "quiero yapear"}},
		{"AgentMessageSent", "agent", "p2p", 3 * time.Second, map[string]any{"text": "¿A quién y cuánto?"}},
		{"ContactConfirmed", "user", "p2p", 6 * time.Second, map[string]any{"contact_ref": "c"}},
		{"UserMessageSent", "user", "p2p", 8 * time.Second, map[string]any{"text": "@(Juan) 20 soles"}},
	}
	amountOutcome := "ok"
	if a.failedTool {
		amountOutcome = "failed"
	}
	tool := func(name, outcome string, at time.Duration) step {
		return step{"ToolActionSummarized", "agent", "p2p", at, map[string]any{"tool_name": name, "outcome": outcome}}
	}
	steps = append(steps, tool("validate_p2p_amount", amountOutcome, 9*time.Second))

	if a.noCard {
		steps = append(steps, step{"AgentMessageSent", "agent", "p2p", 10 * time.Second, map[string]any{"text": "El máximo es S/500"}})
	} else {
		steps = append(steps,
			tool("request_contact_from_client", "ok", 10*time.Second),
			tool("draft_transfer", "ok", 11*time.Second),
			step{"ConfirmationRequested", "agent", "p2p", cardAt, map[string]any{"confirmation_ref": ref, "hint": "p2p_transfer"}},
			tool("request_transfer_approval", "ok", cardAt+time.Second),
			step{"AgentMessageSent", "agent", "p2p", cardAt + 2*time.Second, map[string]any{"text": "Revisa tu yapeo"}},
		)
		if a.otp {
			steps = append(steps, step{"PaymentChallengeIssued", "system", "p2p", cardAt + 3*time.Second, map[string]any{"confirmation_ref": ref}})
		}
		if a.decision != "" {
			steps = append(steps, step{"ConfirmationResolved", "user", "p2p", cardAt + a.resolveAfter,
				map[string]any{"confirmation_ref": ref, "decision": a.decision}})
		}
	}

	ctx := context.Background()
	for i, s := range steps {
		payload, err := json.Marshal(s.payload)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, `
			INSERT INTO conversation_inbox (conversation_id, sequence, event_name, event_version, payload, metadata, correlation_id,
			                                id_yape_account, actor, flow, occurred_at, kafka_topic, kafka_partition, kafka_offset)
			VALUES ($1, $2, $3, 1, $4, '{}', 'corr', $5, $6, NULLIF($7, ''), $8, 'yape.event_store.conversation_events', 0, 0)`,
			a.conversation, i+1, s.name, payload, a.account, s.actor, s.flow, a.start.Add(s.at))
		require.NoError(t, err)

		role := map[string]string{"UserMessageSent": "appUser", "AgentMessageSent": "appMaker"}[s.name]
		if role == "" {
			continue
		}
		_, err = pool.Exec(ctx, `
			INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, flow, occurred_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), $9)`,
			a.conversation, i+1, role, s.actor, s.payload["text"], s.name, a.account, s.flow, a.start.Add(s.at))
		require.NoError(t, err)
	}
}

// TestInsights runs the statistics against a real Postgres: the SQL is only checked here, because
// the unit tests of the repository mock the pool.
func TestInsights(t *testing.T) {
	pool, _ := newHistoryDB(t)
	ctx := context.Background()
	insights := repository.NewPostgresInsightsRepository(pool)

	now := time.Now().UTC().Truncate(time.Second)
	recent := now.Add(-time.Hour)
	long := now.Add(-45 * 24 * time.Hour)
	query := func(account string, since time.Time) domain.InsightQuery {
		return domain.InsightQuery{IDYapeAccount: account, Flow: "p2p", Since: since}
	}

	// Account A: three paid or decided attempts without OTP or with it, one rejected, one left on
	// the card for over ten minutes, one the agent refused before any card, and one 45 days old.
	attempts := []attempt{
		{account: "acc-a", conversation: "conv-a-pay", start: recent, decision: "approved", cardAfter: 22 * time.Second, resolveAfter: 4 * time.Second},
		{account: "acc-a", conversation: "conv-a-otp", start: recent.Add(time.Minute), decision: "approved", otp: true, cardAfter: 20 * time.Second, resolveAfter: 18 * time.Second},
		{account: "acc-a", conversation: "conv-a-reject", start: recent.Add(2 * time.Minute), decision: "rejected", cardAfter: 22 * time.Second, resolveAfter: 4 * time.Second},
		{account: "acc-a", conversation: "conv-a-card", start: recent.Add(3 * time.Minute), cardAfter: 20 * time.Second},
		{account: "acc-a", conversation: "conv-a-limit", start: recent.Add(4 * time.Minute), noCard: true, failedTool: true},
		{account: "acc-a", conversation: "conv-a-old", start: long, decision: "approved", cardAfter: 60 * time.Second, resolveAfter: 60 * time.Second},
		// Account B must never show up in A's numbers.
		{account: "acc-b", conversation: "conv-b-pay", start: now.Add(-30 * time.Minute), decision: "approved", cardAfter: 40 * time.Second, resolveAfter: 2 * time.Second},
	}
	for _, a := range attempts {
		play(t, pool, a)
	}
	refreshSummaries(t, pool)

	t.Run("accountsAreListedMostRecentlyActiveFirstWithTheirCounts", func(t *testing.T) {
		got, err := insights.ListAccounts(ctx, domain.AccountQuery{Size: 10})

		require.NoError(t, err)
		require.Len(t, got, 2)
		// B's last message is at now-30min, A's at about now-1h+5min.
		assert.Equal(t, "acc-b", got[0].IDYapeAccount)
		assert.Equal(t, 1, got[0].Conversations)
		assert.Equal(t, "acc-a", got[1].IDYapeAccount)
		assert.Equal(t, 6, got[1].Conversations)
		// Every attempt leaves four chat messages: two from the user and two from the agent.
		assert.Equal(t, 6*4, got[1].Messages)
	})

	t.Run("accountsPageWithSizeAndOffset", func(t *testing.T) {
		first, err := insights.ListAccounts(ctx, domain.AccountQuery{Size: 1})
		require.NoError(t, err)
		second, err := insights.ListAccounts(ctx, domain.AccountQuery{Size: 1, Offset: 1})
		require.NoError(t, err)
		beyond, err := insights.ListAccounts(ctx, domain.AccountQuery{Size: 1, Offset: 2})
		require.NoError(t, err)

		require.Len(t, first, 1)
		require.Len(t, second, 1)
		assert.NotEqual(t, first[0].IDYapeAccount, second[0].IDYapeAccount)
		assert.Empty(t, beyond)
	})

	t.Run("outcomesOfTheWindowForOneAccount", func(t *testing.T) {
		got, err := insights.OutcomeCounts(ctx, query("acc-a", now.AddDate(0, 0, -30)))

		require.NoError(t, err)
		// The card nobody decided is the last segment of its conversation and older than ten
		// minutes by the time it is read, so it has expired; B's approval is not counted.
		assert.ElementsMatch(t, []domain.OutcomeCount{
			{Flow: "p2p", Outcome: "approved", Segments: 2},
			{Flow: "p2p", Outcome: "rejected", Segments: 1},
			{Flow: "p2p", Outcome: "expired", Segments: 1},
			{Flow: "p2p", Outcome: "none", Segments: 1},
		}, got)
	})

	t.Run("aWiderWindowReachesTheOlderAttempt", func(t *testing.T) {
		got, err := insights.OutcomeCounts(ctx, query("acc-a", now.AddDate(0, 0, -90)))

		require.NoError(t, err)
		approved := 0
		for _, c := range got {
			if c.Outcome == "approved" {
				approved = c.Segments
			}
		}
		assert.Equal(t, 3, approved)
	})

	t.Run("anotherFlowAndAnUnknownAccountHaveNothing", func(t *testing.T) {
		other, err := insights.OutcomeCounts(ctx, domain.InsightQuery{IDYapeAccount: "acc-a", Flow: "bill_payment", Since: now.AddDate(0, 0, -30)})
		require.NoError(t, err)
		unknown, err := insights.OutcomeCounts(ctx, query("acc-nobody", now.AddDate(0, 0, -30)))
		require.NoError(t, err)

		assert.Empty(t, other)
		assert.Empty(t, unknown)
	})

	t.Run("latencyIsSplitByWhetherAnOTPWasAsked", func(t *testing.T) {
		got, err := insights.LatencyGroups(ctx, query("acc-a", now.AddDate(0, 0, -30)))

		require.NoError(t, err)
		require.Len(t, got, 2)

		noOTP, withOTP := got[0], got[1]
		assert.False(t, noOTP.OTP)
		// pay, reject and the card nobody decided: three cards, two decisions. The requests took
		// 22, 22 and 20 seconds to reach the card; the two decisions took 4 seconds each.
		assert.Equal(t, 3, noOTP.Cards)
		assert.Equal(t, 2, noOTP.Resolved)
		require.NotNil(t, noOTP.AskToCard.Average)
		assert.InDelta(t, 64.0/3, *noOTP.AskToCard.Average, 0.001)
		assert.InDelta(t, 22.0, *noOTP.AskToCard.Median, 0.001)
		assert.InDelta(t, 4.0, *noOTP.CardToResolution.Average, 0.001)
		assert.InDelta(t, 4.0, *noOTP.CardToResolution.Median, 0.001)

		assert.True(t, withOTP.OTP)
		assert.Equal(t, 1, withOTP.Cards)
		assert.Equal(t, 1, withOTP.Resolved)
		assert.InDelta(t, 20.0, *withOTP.AskToCard.Average, 0.001)
		assert.InDelta(t, 18.0, *withOTP.CardToResolution.Average, 0.001)
	})

	t.Run("anAttemptThatNeverGotADecisionHasNoResolutionTime", func(t *testing.T) {
		only := attempt{account: "acc-c", conversation: "conv-c-card", start: recent, cardAfter: 25 * time.Second}
		play(t, pool, only)
		refreshSummaries(t, pool) // the statements reach the inbox through the summaries

		got, err := insights.LatencyGroups(ctx, query("acc-c", now.AddDate(0, 0, -30)))

		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, 1, got[0].Cards)
		assert.Equal(t, 0, got[0].Resolved)
		assert.InDelta(t, 25.0, *got[0].AskToCard.Average, 0.001)
		assert.Nil(t, got[0].CardToResolution.Average)
		assert.Nil(t, got[0].CardToResolution.Median)
	})

	t.Run("toolCallsAreCountedPerToolAndOutcomeInsideTheWindow", func(t *testing.T) {
		got, err := insights.ToolCounts(ctx, query("acc-a", now.AddDate(0, 0, -30)))

		require.NoError(t, err)
		counts := map[string]int{}
		for _, c := range got {
			counts[fmt.Sprintf("%s/%s", c.Tool, c.Outcome)] = c.Calls
		}
		// Five attempts in the window; the 45-day-old one and account B are left out. The limit
		// attempt failed the amount check and went no further.
		assert.Equal(t, map[string]int{
			"validate_p2p_amount/ok":         4,
			"validate_p2p_amount/failed":     1,
			"request_contact_from_client/ok": 4,
			"draft_transfer/ok":              4,
			"request_transfer_approval/ok":   4,
		}, counts)
	})

	t.Run("noStatementTouchesAnotherAccount", func(t *testing.T) {
		for _, account := range []string{"acc-a", "acc-b"} {
			since := now.AddDate(0, 0, -90)
			outcomes, err := insights.OutcomeCounts(ctx, query(account, since))
			require.NoError(t, err)
			tools, err := insights.ToolCounts(ctx, query(account, since))
			require.NoError(t, err)

			total := 0
			for _, c := range outcomes {
				total += c.Segments
			}
			want := map[string]int{"acc-a": 6, "acc-b": 1}[account]
			assert.Equal(t, want, total, account)
			assert.NotEmpty(t, tools, account)
		}
	})
}
