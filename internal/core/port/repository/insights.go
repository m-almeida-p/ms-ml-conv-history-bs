package repository

import (
	"context"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
)

//go:generate mockery --name InsightsRepository --filename insights.go

// InsightsRepository reads statistics about the conversations of an account. Every read is
// scoped to the account it is given, and none of them changes anything.
type InsightsRepository interface {
	// ListAccounts returns up to query.Size accounts that have chat messages, the most recently
	// active first, starting query.Offset accounts in. It does not say whether more exist.
	ListAccounts(ctx context.Context, query domain.AccountQuery) ([]domain.AccountActivity, error)

	// OutcomeCounts returns how many segments of the flow began since the given time, by
	// outcome. An account with none gets an empty slice, not an error.
	OutcomeCounts(ctx context.Context, query domain.InsightQuery) ([]domain.OutcomeCount, error)

	// LatencyGroups measures the segments of the flow that began since the given time and reached
	// the confirmation card, one group for the ones that asked for an OTP and one for the ones
	// that did not, the group without an OTP first. A group with no such segment is left out.
	LatencyGroups(ctx context.Context, query domain.InsightQuery) ([]domain.LatencyGroup, error)

	// ToolCounts returns how many calls of each tool the agent made for the account since the
	// given time, by outcome. The flow of the query is ignored.
	ToolCounts(ctx context.Context, query domain.InsightQuery) ([]domain.ToolCount, error)
}
