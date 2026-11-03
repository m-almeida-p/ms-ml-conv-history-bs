package domain

import "time"

// DefaultWindowDays is how far back an insight looks when the caller does not say.
const DefaultWindowDays = 30

// MaxWindowDays is the longest window an insight may look back.
const MaxWindowDays = 365

// DefaultInsightFlow is the flow an insight reads when the caller names none.
const DefaultInsightFlow = "p2p"

// AccountListSize is how many accounts a page holds when the caller does not ask for fewer. It
// is also the most a caller can ask for.
const AccountListSize = 100

// Outcomes of a flow segment, as conversation_flow_segments reports them.
const (
	OutcomeApproved  = "approved"
	OutcomeRejected  = "rejected"
	OutcomeFailed    = "failed"
	OutcomeAbandoned = "abandoned"
	OutcomeExpired   = "expired"
	OutcomeOpen      = "open"
	OutcomeNone      = "none"
)

// ToolOutcomeOK is the outcome a ToolActionSummarized event reports for a tool that worked.
const ToolOutcomeOK = "ok"

// AccountActivity describes what the history holds for one account.
type AccountActivity struct {
	IDYapeAccount   string
	Conversations   int
	Messages        int
	FirstOccurredAt time.Time
	LastOccurredAt  time.Time
}

// AccountQuery asks for a page of the accounts that have chat messages, the most recently active
// first. Size is how many to return, from 1 to AccountListSize; Offset is how many to skip.
type AccountQuery struct {
	Size   int
	Offset int
}

// AccountList is one page of accounts.
type AccountList struct {
	Size     int
	Offset   int
	Accounts []AccountActivity
	// HasMore is true when more accounts exist after this page.
	HasMore bool
}

// InsightQuery scopes a statistic to one account, one flow and the time since which segments began.
type InsightQuery struct {
	IDYapeAccount string
	Flow          string
	Since         time.Time
}

// OutcomeCount is how many segments of a flow ended in an outcome.
type OutcomeCount struct {
	Flow     string
	Outcome  string
	Segments int
}

// Funnel counts the segments of one flow of an account by how they ended. A segment is one
// attempt: it opens with the user's request and ends at the payment decision, or when the user
// moves on.
type Funnel struct {
	IDYapeAccount string
	Flow          string
	WindowDays    int
	Since         time.Time
	// Attempts is every segment of the flow in the window.
	Attempts int
	// CardsOffered is the attempts that reached the confirmation card: every attempt but the
	// ones with the outcome none.
	CardsOffered int
	// Outcomes counts the attempts by outcome. Every outcome is present, with 0 when none.
	Outcomes map[string]int
	// ApprovalRate is approved divided by the cards whose fate is settled (everything but open).
	// It is nil while no card is settled.
	ApprovalRate *float64
}

// Stat is an average and a median, in seconds. A nil value means there was nothing to measure.
type Stat struct {
	Average *float64
	Median  *float64
}

// LatencyGroup measures the attempts that did, or did not, ask the user for an OTP.
type LatencyGroup struct {
	OTP bool
	// Cards is the attempts that reached the confirmation card.
	Cards int
	// Resolved is the attempts whose card was approved, rejected or failed.
	Resolved int
	// AskToCard is the time from the user's request to the card: the agent's work.
	AskToCard Stat
	// CardToResolution is the time from the card to the user's decision: the person's time.
	CardToResolution Stat
}

// Latency measures how long the attempts of a flow take, split by whether they needed an OTP.
type Latency struct {
	IDYapeAccount string
	Flow          string
	WindowDays    int
	Since         time.Time
	Groups        []LatencyGroup
}

// ToolCount is how many calls of a tool the agent made with an outcome.
type ToolCount struct {
	Tool    string
	Outcome string
	Calls   int
}

// ToolHealthEntry is the record of one tool of the agent.
type ToolHealthEntry struct {
	Tool        string
	Calls       int
	Failed      int
	FailureRate float64
}

// ToolHealth lists the tools the agent called for an account, the most called first.
type ToolHealth struct {
	IDYapeAccount string
	WindowDays    int
	Since         time.Time
	Tools         []ToolHealthEntry
}
