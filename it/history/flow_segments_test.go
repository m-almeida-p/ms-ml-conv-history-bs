package history_test

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fact is one row of the inbox as the view reads it.
type fact struct {
	name    string
	flow    string // "" is NULL
	ref     string // confirmation_ref of a payment fact
	outcome string // decision of a ConfirmationResolved
	bare    bool   // a ConfirmationResolved whose payload has no decision
	ago     time.Duration
}

// segment is one row of conversation_flow_segments.
type segment struct {
	Segment  int
	Flow     string
	From, To int
	Events   int
	Outcome  string
}

// TestFlowSegments: conversation_flow_segments cuts a conversation into segments. A
// segment is a stretch of one flow (or of none); a user message belongs to the flow
// that handled it; the guardrail and failure facts belong to the segment around them;
// a segment ends at its resolution, and its outcome is read from the payment facts.
func TestFlowSegments(t *testing.T) {
	agent := agentFact
	request := func(ref string) fact { return fact{name: "ConfirmationRequested", flow: "p2p", ref: ref} }
	resolved := func(ref, decision string) fact {
		return fact{name: "ConfirmationResolved", flow: "p2p", ref: ref, outcome: decision}
	}

	tests := []struct {
		name  string
		facts []fact
		want  []segment
	}{
		{
			name: "theMessageThatOpensAFlowJoinsItAndTheOneThatLeavesItJoinsTheNextRun",
			facts: []fact{
				{name: "ConversationStarted"}, user(""), agent(""), // 1-3 outside any flow
				user(""), agent("p2p"), user("p2p"), request("a"), resolved("a", "approved"), // 4-8 the flow
				user("p2p"), agent(""), user(""), // 9-11 back outside: 9 was written inside the flow
			},
			want: []segment{
				{1, "", 1, 3, 3, "none"},
				{2, "p2p", 4, 8, 5, "approved"},
				{3, "", 9, 11, 3, "none"},
			},
		},
		{
			name: "twoTransfersInARowAreTwoRuns",
			facts: []fact{
				user(""), agent("p2p"), user("p2p"), request("a"), resolved("a", "approved"), agent("p2p"),
				user("p2p"), agent("p2p"), request("b"), resolved("b", "failed"),
			},
			want: []segment{
				{1, "p2p", 1, 6, 6, "approved"},
				{2, "p2p", 7, 10, 4, "failed"},
			},
		},
		{
			name: "aCardLeftUnresolvedAndFollowedByAnotherRunWasAbandoned",
			facts: []fact{
				user(""), agent("p2p"), request("a"), user("p2p"), agent(""), user(""),
			},
			want: []segment{
				{1, "p2p", 1, 3, 3, "abandoned"},
				{2, "", 4, 6, 3, "none"},
			},
		},
		{
			name: "aFlowWithoutACardHasNoOutcome",
			facts: []fact{
				user(""), agent("p2p"), user("p2p"), agent("p2p"),
			},
			want: []segment{{1, "p2p", 1, 4, 4, "none"}},
		},
		{
			name: "aBlockedMessageInsideAFlowDoesNotSplitIt",
			facts: []fact{
				user(""), agent("p2p"),
				{name: "UserMessageBlocked", flow: "p2p"}, {name: "GuardrailResponseSent"}, // 3-4 belong to the segment around them
				user("p2p"), agent("p2p"),
			},
			want: []segment{{1, "p2p", 1, 6, 6, "none"}},
		},
		{
			name: "aRejectedCardIsARejectedRun",
			facts: []fact{
				user(""), agent("p2p"), request("a"), resolved("a", "rejected"),
			},
			want: []segment{{1, "p2p", 1, 4, 4, "rejected"}},
		},
		{
			name:  "aConversationOfOnlyItsStartIsOneRunOutsideAnyFlow",
			facts: []fact{{name: "ConversationStarted"}},
			want:  []segment{{1, "", 1, 1, 1, "none"}},
		},
		{
			name:  "aUserMessageWithNoFactAfterItKeepsTheFlowItWasWrittenIn",
			facts: []fact{user(""), agent("p2p"), user("p2p")},
			want:  []segment{{1, "p2p", 1, 3, 3, "none"}},
		},
		{
			name:  "aLastMessageOutsideAFlowAfterTheFlowIsItsOwnRun",
			facts: []fact{user(""), agent("p2p"), user("")},
			want:  []segment{{1, "p2p", 1, 2, 2, "none"}, {2, "", 3, 3, 1, "none"}},
		},
		{
			name: "theLastResolutionOfARunDecidesItsOutcome",
			facts: []fact{
				user(""), agent("p2p"), request("a"), resolved("a", "rejected"), resolved("a", "approved"),
			},
			want: []segment{{1, "p2p", 1, 5, 5, "approved"}},
		},
		{
			name: "aResolutionWithoutADecisionIsNotAnOutcome",
			facts: []fact{
				user(""), agent("p2p"), request("a"), {name: "ConfirmationResolved", flow: "p2p", ref: "a", bare: true},
			},
			want: []segment{{1, "p2p", 1, 4, 4, "open"}},
		},
		{
			name:  "consecutiveUserMessagesTakeTheFlowOfTheFactThatHandledThem",
			facts: []fact{user(""), user(""), agent("p2p"), user("p2p")},
			want:  []segment{{1, "p2p", 1, 4, 4, "none"}},
		},
		{
			name:  "aFailedTurnBetweenAMessageAndItsHandlerDoesNotSplitTheRun",
			facts: []fact{user(""), {name: "AgentResponseFailed"}, agent("p2p")},
			want:  []segment{{1, "p2p", 1, 3, 3, "none"}},
		},
		{
			name:  "theCloseOfAConversationJoinsTheRunBeforeIt",
			facts: []fact{user(""), agent("p2p"), {name: "ConversationClosed"}},
			want:  []segment{{1, "p2p", 1, 3, 3, "none"}},
		},
		{
			name:  "messagesAloneAreCutByTheFlowTheyWereWrittenIn",
			facts: []fact{user("p2p"), user("")},
			want:  []segment{{1, "p2p", 1, 1, 1, "none"}, {2, "", 2, 2, 1, "none"}},
		},
		{
			name: "aDecisionIsReportedAsTheAgentWroteIt",
			facts: []fact{
				user(""), agent("p2p"), request("a"), resolved("a", "APPROVED"),
			},
			want: []segment{{1, "p2p", 1, 4, 4, "APPROVED"}},
		},
		{
			name: "aGuardrailReplyAtTheEdgeOfAFlowJoinsTheRunBeforeIt",
			facts: []fact{
				user(""), agent("p2p"), request("a"), resolved("a", "approved"),
				user("p2p"), {name: "UserMessageBlocked", flow: "p2p"}, {name: "GuardrailResponseSent"},
			},
			want: []segment{{1, "p2p", 1, 4, 4, "approved"}, {2, "p2p", 5, 7, 3, "none"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newPool(t)
			createSchema(t, pool)

			insertFacts(t, pool, "conv_a", tt.facts)
			// A second conversation in the same table must not mix with the first.
			insertFacts(t, pool, "conv_b", []fact{{name: "ConversationStarted"}, user(""), agent("bill_payment")})

			assert.Equal(t, tt.want, segmentsOf(t, pool, "conv_a"))
			assert.Equal(t, []segment{{1, "", 1, 1, 1, "none"}, {2, "bill_payment", 2, 3, 2, "none"}}, segmentsOf(t, pool, "conv_b"),
				"another conversation is cut on its own")
		})
	}

	t.Run("anUnresolvedCardOfTheLastRunIsOpenThenExpired", func(t *testing.T) {
		pool := newPool(t)
		createSchema(t, pool)

		recent := []fact{user(""), agent("p2p"), request("a")}
		old := []fact{user(""), agent("p2p"), request("a")}
		for i := range old {
			old[i].ago = time.Hour
		}
		insertFacts(t, pool, "conv_recent", recent)
		insertFacts(t, pool, "conv_old", old)

		assert.Equal(t, "open", segmentsOf(t, pool, "conv_recent")[0].Outcome, "inside the approval's ten minutes")
		assert.Equal(t, "expired", segmentsOf(t, pool, "conv_old")[0].Outcome, "no resolution after the approval's ten minutes")
	})
}

// TestFlowSegmentsReadTheCardFromThePaymentFacts: the card reference is the first one
// the segment carries; only a ConfirmationRequested makes a segment "offer a card", while a
// challenge or a failed notice carries the reference without an outcome.
func TestFlowSegmentsReadTheCardFromThePaymentFacts(t *testing.T) {
	refOf := func(t *testing.T, pool *pgxpool.Pool, conversation string) (ref *string, segments int) {
		t.Helper()

		rows, err := pool.Query(context.Background(),
			`SELECT confirmation_ref FROM conversation_flow_segments WHERE conversation_id = $1 ORDER BY segment`, conversation)
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			require.NoError(t, rows.Scan(&ref))
			segments++
		}
		require.NoError(t, rows.Err())

		return ref, segments
	}
	card := func(name, ref string) fact { return fact{name: name, flow: "p2p", ref: ref} }

	tests := []struct {
		name        string
		facts       []fact
		wantRef     string
		wantOutcome string
	}{
		{"twoCardsInARunReportTheFirst", []fact{user(""), agentFact("p2p"), card("ConfirmationRequested", "first"), card("ConfirmationRequested", "second")}, "first", "open"},
		{"aChallengeAloneIsNotACard", []fact{user(""), agentFact("p2p"), card("PaymentChallengeIssued", "c1")}, "c1", "none"},
		{"aFailedNoticeAloneIsNotACard", []fact{user(""), agentFact("p2p"), card("AgentNotificationFailed", "c1")}, "c1", "none"},
		{"aRunWithoutPaymentFactsHasNoReference", []fact{user(""), agentFact("p2p")}, "", "none"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newPool(t)
			createSchema(t, pool)
			insertFacts(t, pool, "conv_a", tt.facts)

			ref, segments := refOf(t, pool, "conv_a")
			require.Equal(t, 1, segments)
			if tt.wantRef == "" {
				assert.Nil(t, ref)
			} else {
				require.NotNil(t, ref)
				assert.Equal(t, tt.wantRef, *ref)
			}
			assert.Equal(t, tt.wantOutcome, segmentsOf(t, pool, "conv_a")[0].Outcome)
		})
	}
}

// TestFlowSegmentsFollowTheSequenceNotThePosition: the inbox of a conversation can
// have holes (a skipped malformed event); the segments report the sequences it holds.
func TestFlowSegmentsFollowTheSequenceNotThePosition(t *testing.T) {
	pool := newPool(t)
	createSchema(t, pool)

	insertFacts(t, pool, "conv_a", []fact{user(""), agentFact("p2p"), user("p2p"), agentFact(""), user("")})
	_, err := pool.Exec(context.Background(),
		`UPDATE conversation_inbox SET sequence = sequence * 10, kafka_offset = kafka_offset + 1000 WHERE conversation_id = 'conv_a'`)
	require.NoError(t, err)

	assert.Equal(t, []segment{
		{1, "p2p", 10, 20, 2, "none"},
		{2, "", 30, 50, 3, "none"}, // 30 was written inside p2p and handled by the dispatcher
	}, segmentsOf(t, pool, "conv_a"))
}

// TestFlowSegmentsAcrossConversations: reading several conversations in one query
// gives each the segments it has alone, and a conversation that does not exist has none.
func TestFlowSegmentsAcrossConversations(t *testing.T) {
	pool := newPool(t)
	createSchema(t, pool)

	insertFacts(t, pool, "conv_a", []fact{user(""), agentFact("p2p"), {name: "ConfirmationRequested", flow: "p2p", ref: "a"}, user("p2p"), user("")})
	insertFacts(t, pool, "conv_b", []fact{user(""), agentFact("bill_payment"), user("bill_payment")})
	insertFacts(t, pool, "conv_c", []fact{user("")})

	t.Run("anUnknownConversationHasNoRuns", func(t *testing.T) {
		assert.Empty(t, segmentsOf(t, pool, "conv_missing"))
	})

	t.Run("severalConversationsInOneQueryAreEachCutOnTheirOwn", func(t *testing.T) {
		rows, err := pool.Query(context.Background(), `
			SELECT conversation_id, segment, coalesce(flow, ''), from_sequence, to_sequence, events, outcome
			FROM conversation_flow_segments
			WHERE conversation_id = ANY (ARRAY(SELECT unnest($1::text[])))
			ORDER BY conversation_id, segment`, []string{"conv_a", "conv_b", "conv_missing"})
		require.NoError(t, err)
		defer rows.Close()

		got := map[string][]segment{}
		for rows.Next() {
			var id string
			var r segment
			require.NoError(t, rows.Scan(&id, &r.Segment, &r.Flow, &r.From, &r.To, &r.Events, &r.Outcome))
			got[id] = append(got[id], r)
		}
		require.NoError(t, rows.Err())

		require.Len(t, got, 2, "a conversation outside the list or the table adds nothing")
		assert.Equal(t, segmentsOf(t, pool, "conv_a"), got["conv_a"])
		assert.Equal(t, segmentsOf(t, pool, "conv_b"), got["conv_b"])
	})
}

// TestFlowSegmentsApprovalLifetime: a card left unresolved is open for ten minutes
// after its last fact and expired afterwards.
func TestFlowSegmentsApprovalLifetime(t *testing.T) {
	for name, tt := range map[string]struct {
		ago  time.Duration
		want string
	}{
		"justNow":          {0, "open"},
		"nineMinutesAgo":   {9 * time.Minute, "open"},
		"elevenMinutesAgo": {11 * time.Minute, "expired"},
		"aDayAgo":          {24 * time.Hour, "expired"},
	} {
		t.Run(name, func(t *testing.T) {
			pool := newPool(t)
			createSchema(t, pool)

			insertFacts(t, pool, "conv_a", []fact{
				{name: "UserMessageSent", ago: tt.ago},
				{name: "AgentMessageSent", flow: "p2p", ago: tt.ago},
				{name: "ConfirmationRequested", flow: "p2p", ref: "a", ago: tt.ago},
			})

			assert.Equal(t, tt.want, segmentsOf(t, pool, "conv_a")[0].Outcome)
		})
	}
}

// TestFlowSegmentsReadOnlyTheConversationAsked: a query for one conversation reads
// that conversation's rows of the inbox and no others. The view is made of windows
// that all partition by conversation_id, so the filter reaches the primary key; a
// materialized step or a correlated lookup would read the whole table, and on a
// real inbox that is minutes.
func TestFlowSegmentsReadOnlyTheConversationAsked(t *testing.T) {
	pool := newPool(t)
	createSchema(t, pool)

	insertFacts(t, pool, "conv_asked", []fact{user(""), agentFact("p2p"), {name: "ConfirmationRequested", flow: "p2p", ref: "a"}})
	_, err := pool.Exec(context.Background(), `
		INSERT INTO conversation_inbox (conversation_id, sequence, event_name, event_version, payload, metadata, occurred_at, kafka_topic, kafka_partition, kafka_offset)
		SELECT 'conv_other_' || c, s, 'UserMessageSent', 1, '{}', '{}', now(), 't', 0, c * 100 + s
		FROM generate_series(1, 300) c, generate_series(1, 10) s`)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), `ANALYZE conversation_inbox`)
	require.NoError(t, err)

	rows, err := pool.Query(context.Background(),
		`EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) SELECT * FROM conversation_flow_segments WHERE conversation_id = 'conv_asked'`)
	require.NoError(t, err)
	defer rows.Close()

	actual := regexp.MustCompile(`actual rows=(\d+)`)
	var scans int
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		if !strings.Contains(line, "conversation_inbox") || !strings.Contains(line, "Scan") {
			continue
		}
		scans++
		match := actual.FindStringSubmatch(line)
		require.NotNil(t, match, line)
		read, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		assert.LessOrEqual(t, read, 10, "a scan of the inbox read other conversations: %s", strings.TrimSpace(line))
	}
	require.NoError(t, rows.Err())
	assert.Positive(t, scans, "the plan has no scan of the inbox")
}

func agentFact(flow string) fact { return fact{name: "AgentMessageSent", flow: flow} }

func user(flow string) fact { return fact{name: "UserMessageSent", flow: flow} }

// insertFacts stores the facts of one conversation in the inbox, sequence 1 upward.
func insertFacts(t *testing.T, pool *pgxpool.Pool, conversation string, facts []fact) {
	t.Helper()

	for i, f := range facts {
		payload := "{}"
		switch {
		case f.name == "ConfirmationResolved" && !f.bare:
			payload = fmt.Sprintf(`{"confirmation_ref": %q, "decision": %q}`, f.ref, f.outcome)
		case f.ref != "":
			payload = fmt.Sprintf(`{"confirmation_ref": %q}`, f.ref)
		}

		_, err := pool.Exec(context.Background(), `
			INSERT INTO conversation_inbox (conversation_id, sequence, event_name, event_version, payload, metadata, flow, occurred_at, kafka_topic, kafka_partition, kafka_offset)
			VALUES ($1, $2::int, $3, 1, $4::jsonb, '{}'::jsonb, NULLIF($5, ''), now() - ($6::bigint * interval '1 microsecond') + ($2::int * interval '1 second'), 't', 0, $2::int)`,
			conversation, i+1, f.name, payload, f.flow, f.ago.Microseconds())
		require.NoError(t, err)
	}
}

func segmentsOf(t *testing.T, pool *pgxpool.Pool, conversation string) []segment {
	t.Helper()

	rows, err := pool.Query(context.Background(), `
		SELECT segment, coalesce(flow, ''), from_sequence, to_sequence, events, outcome
		FROM conversation_flow_segments WHERE conversation_id = $1 ORDER BY segment`, conversation)
	require.NoError(t, err)
	defer rows.Close()

	var out []segment
	for rows.Next() {
		var r segment
		require.NoError(t, rows.Scan(&r.Segment, &r.Flow, &r.From, &r.To, &r.Events, &r.Outcome))
		out = append(out, r)
	}
	require.NoError(t, rows.Err())

	return out
}
