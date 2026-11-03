package service

import (
	"context"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
)

//go:generate mockery --name Insights --filename insights.go

// AccountsRequest asks for a page of accounts. Size and Offset are as a caller sends them: zero
// means not sent, and the page holds domain.AccountListSize accounts from the start.
type AccountsRequest struct {
	Size   int
	Offset int
}

// InsightRequest scopes a statistic. Days is how far back to look (zero means
// domain.DefaultWindowDays) and Flow which flow to read (empty means domain.DefaultInsightFlow).
type InsightRequest struct {
	IDYapeAccount string
	Flow          string
	Days          int
}

// Insights is what a caller can learn about the conversations of the accounts. It answers with
// counts and times, never with message text.
type Insights interface {
	// Accounts returns one page of the accounts that have chat messages. Errors wrap
	// domain.ErrInvalidRequest (a size outside 1 to domain.AccountListSize, a negative offset).
	Accounts(ctx context.Context, req AccountsRequest) (domain.AccountList, error)

	// Funnel counts the attempts of a flow by how they ended. Errors wrap
	// domain.ErrInvalidRequest (a malformed account or flow, a window outside 1 to
	// domain.MaxWindowDays).
	Funnel(ctx context.Context, req InsightRequest) (domain.Funnel, error)

	// Latency measures how long the attempts of a flow take. Errors are as for Funnel.
	Latency(ctx context.Context, req InsightRequest) (domain.Latency, error)

	// ToolHealth lists how often each tool of the agent failed. The flow of the request is
	// ignored. Errors are as for Funnel.
	ToolHealth(ctx context.Context, req InsightRequest) (domain.ToolHealth, error)
}
