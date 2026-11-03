package mcp

import (
	"math"
	"time"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
)

// The types below are what a tool returns. They are shaped for a model to read: snake_case names,
// times in RFC 3339, durations in seconds rounded to a tenth, and no field the model has no use for.
// Every list is built non-nil, so it is [] and never null in the output schema, and every time is
// in UTC, whatever zone the store returned it in.

// accountJSON is one account of list_accounts.
type accountJSON struct {
	IDYapeAccount string    `json:"id_yape_account"`
	Conversations int       `json:"conversations"`
	Messages      int       `json:"messages"`
	FirstActivity time.Time `json:"first_activity"`
	LastActivity  time.Time `json:"last_activity"`
}

// accountsResult is the output of list_accounts.
type accountsResult struct {
	Accounts []accountJSON `json:"accounts"`
	Limit    int           `json:"limit"`
	Offset   int           `json:"offset"`
	HasMore  bool          `json:"has_more"`
}

func toAccountsResult(list domain.AccountList) accountsResult {
	out := accountsResult{Accounts: make([]accountJSON, 0, len(list.Accounts)), Limit: list.Size, Offset: list.Offset, HasMore: list.HasMore}
	for _, a := range list.Accounts {
		out.Accounts = append(out.Accounts, accountJSON{
			IDYapeAccount: a.IDYapeAccount, Conversations: a.Conversations, Messages: a.Messages,
			FirstActivity: a.FirstOccurredAt.UTC(), LastActivity: a.LastOccurredAt.UTC(),
		})
	}

	return out
}

// conversationJSON is one conversation of get_recent_conversations.
type conversationJSON struct {
	ConversationID string    `json:"conversation_id"`
	Messages       int       `json:"messages"`
	FirstMessageAt time.Time `json:"first_message_at"`
	LastMessageAt  time.Time `json:"last_message_at"`
}

// conversationsResult is the output of get_recent_conversations.
type conversationsResult struct {
	IDYapeAccount string             `json:"id_yape_account"`
	Conversations []conversationJSON `json:"conversations"`
	HasMore       bool               `json:"has_more"`
}

func toConversationsResult(account string, list domain.ConversationList) conversationsResult {
	out := conversationsResult{IDYapeAccount: account, Conversations: make([]conversationJSON, 0, len(list.Conversations)), HasMore: list.HasMore}
	for _, c := range list.Conversations {
		out.Conversations = append(out.Conversations, conversationJSON{
			ConversationID: c.ConversationID, Messages: c.MessageCount,
			FirstMessageAt: c.FirstOccurredAt.UTC(), LastMessageAt: c.LastOccurredAt.UTC(),
		})
	}

	return out
}

// messageJSON is one message of get_transcript.
type messageJSON struct {
	ID        string    `json:"id"`
	Role      string    `json:"role"`
	Text      string    `json:"text"`
	Flow      string    `json:"flow,omitempty"`
	At        time.Time `json:"at"`
	Truncated bool      `json:"truncated,omitzero"`
}

// transcriptResult is the output of get_transcript.
type transcriptResult struct {
	ConversationID string        `json:"conversation_id"`
	Messages       []messageJSON `json:"messages"`
	// HasOlder is true when earlier messages exist; pass OlderCursor as before to read them.
	HasOlder    bool   `json:"has_older"`
	OlderCursor string `json:"older_cursor,omitempty"`
}

func toTranscriptResult(page domain.MessagePage) transcriptResult {
	out := transcriptResult{ConversationID: page.ConversationID, Messages: make([]messageJSON, 0, len(page.Messages)), HasOlder: page.HasMore}
	for _, m := range page.Messages {
		out.Messages = append(out.Messages, messageJSON{
			ID: itoa(m.Sequence), Role: string(m.Role), Text: m.Text, Flow: m.Flow, At: m.OccurredAt.UTC(), Truncated: m.Truncated,
		})
	}
	if page.HasMore {
		out.OlderCursor = page.BeforeCursor
	}

	return out
}

// funnelResult is the output of get_activity_funnel.
type funnelResult struct {
	IDYapeAccount string    `json:"id_yape_account"`
	Flow          string    `json:"flow"`
	WindowDays    int       `json:"window_days"`
	Since         time.Time `json:"since"`
	// Attempts is every try of the flow in the window: one per request to start it.
	Attempts int `json:"attempts"`
	// CardsOffered is the attempts that reached the confirmation card.
	CardsOffered int `json:"cards_offered"`
	// Outcomes counts the attempts by how they ended: approved, rejected, failed, abandoned
	// (a card left unresolved when the person moved on), expired (a card left unresolved for over
	// ten minutes), open (an unresolved card still within ten minutes) and none (no card was offered).
	Outcomes map[string]int `json:"outcomes"`
	// ApprovalRate is approved over the cards whose fate is settled; absent while none is.
	ApprovalRate *float64 `json:"approval_rate,omitzero"`
}

func toFunnelResult(f domain.Funnel) funnelResult {
	return funnelResult{
		IDYapeAccount: f.IDYapeAccount, Flow: f.Flow, WindowDays: f.WindowDays, Since: f.Since.UTC(),
		Attempts: f.Attempts, CardsOffered: f.CardsOffered, Outcomes: f.Outcomes, ApprovalRate: round(f.ApprovalRate, 1000),
	}
}

// statJSON is an average and a median in seconds; a value is absent when there was nothing to measure.
type statJSON struct {
	Average *float64 `json:"average,omitzero"`
	Median  *float64 `json:"median,omitzero"`
}

// latencyGroupJSON measures the attempts that did, or did not, ask for an OTP.
type latencyGroupJSON struct {
	AskedOTP bool `json:"asked_otp"`
	// Cards is the attempts that reached the confirmation card, and Resolved the ones that were decided.
	Cards    int `json:"cards"`
	Resolved int `json:"resolved"`
	// AskToCardSeconds is the time from the request to the card: the agent's work.
	AskToCardSeconds statJSON `json:"ask_to_card_seconds"`
	// CardToResolutionSeconds is the time from the card to the decision: the person's time, OTP included.
	CardToResolutionSeconds statJSON `json:"card_to_resolution_seconds"`
}

// latencyResult is the output of get_latency_stats.
type latencyResult struct {
	IDYapeAccount string             `json:"id_yape_account"`
	Flow          string             `json:"flow"`
	WindowDays    int                `json:"window_days"`
	Since         time.Time          `json:"since"`
	Groups        []latencyGroupJSON `json:"groups"`
}

func toLatencyResult(l domain.Latency) latencyResult {
	out := latencyResult{
		IDYapeAccount: l.IDYapeAccount, Flow: l.Flow, WindowDays: l.WindowDays, Since: l.Since.UTC(),
		Groups: make([]latencyGroupJSON, 0, len(l.Groups)),
	}
	for _, g := range l.Groups {
		out.Groups = append(out.Groups, latencyGroupJSON{
			AskedOTP: g.OTP, Cards: g.Cards, Resolved: g.Resolved,
			AskToCardSeconds:        statJSON{Average: round(g.AskToCard.Average, 10), Median: round(g.AskToCard.Median, 10)},
			CardToResolutionSeconds: statJSON{Average: round(g.CardToResolution.Average, 10), Median: round(g.CardToResolution.Median, 10)},
		})
	}

	return out
}

// toolJSON is the record of one tool of the agent.
type toolJSON struct {
	Tool        string  `json:"tool"`
	Calls       int     `json:"calls"`
	Failed      int     `json:"failed"`
	FailureRate float64 `json:"failure_rate"`
}

// toolHealthResult is the output of get_tool_health.
type toolHealthResult struct {
	IDYapeAccount string     `json:"id_yape_account"`
	WindowDays    int        `json:"window_days"`
	Since         time.Time  `json:"since"`
	Tools         []toolJSON `json:"tools"`
}

func toToolHealthResult(h domain.ToolHealth) toolHealthResult {
	out := toolHealthResult{IDYapeAccount: h.IDYapeAccount, WindowDays: h.WindowDays, Since: h.Since.UTC(), Tools: make([]toolJSON, 0, len(h.Tools))}
	for _, t := range h.Tools {
		out.Tools = append(out.Tools, toolJSON{Tool: t.Tool, Calls: t.Calls, Failed: t.Failed, FailureRate: *round(&t.FailureRate, 1000)})
	}

	return out
}

// round rounds *v to the nearest 1/scale (10 gives a tenth, 1000 a thousandth). A nil stays nil.
func round(v *float64, scale float64) *float64 {
	if v == nil {
		return nil
	}

	return new(math.Round(*v*scale) / scale)
}
