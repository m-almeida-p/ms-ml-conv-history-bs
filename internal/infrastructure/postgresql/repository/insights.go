package repository

import (
	"context"
	_ "embed"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql"
)

var (
	//go:embed SQL/list_accounts.sql
	sqlListAccounts string
	//go:embed SQL/insight_outcome_counts.sql
	sqlInsightOutcomeCounts string
	//go:embed SQL/insight_latency.sql
	sqlInsightLatency string
	//go:embed SQL/insight_tool_counts.sql
	sqlInsightToolCounts string
)

var _ repository.InsightsRepository = (*PostgresInsightsRepository)(nil)

// PostgresInsightsRepository is the Postgres InsightsRepository. It only reads, and every
// statement it runs is scoped to the account it is given.
type PostgresInsightsRepository struct {
	db postgresql.DB
}

// NewPostgresInsightsRepository returns a statistics reader over db.
func NewPostgresInsightsRepository(db postgresql.DB) *PostgresInsightsRepository {
	return &PostgresInsightsRepository{db: db}
}

// ListAccounts implements repository.InsightsRepository. It reads conversation_summaries, so an
// account with chat messages is listed and one with only tool events is not, as in the API.
func (r *PostgresInsightsRepository) ListAccounts(ctx context.Context, q domain.AccountQuery) ([]domain.AccountActivity, error) {
	rows, err := r.db.Query(ctx, sqlListAccounts, q.Size, q.Offset)
	if err != nil {
		return nil, dbError("list accounts", err)
	}
	defer rows.Close()

	var accounts []domain.AccountActivity
	for rows.Next() {
		var a domain.AccountActivity
		if err := rows.Scan(&a.IDYapeAccount, &a.Conversations, &a.Messages, &a.FirstOccurredAt, &a.LastOccurredAt); err != nil {
			return nil, dbError("scan account", err)
		}
		accounts = append(accounts, a)
	}
	if err := rows.Err(); err != nil {
		return nil, dbError("list accounts", err)
	}

	return accounts, nil
}

// OutcomeCounts implements repository.InsightsRepository.
func (r *PostgresInsightsRepository) OutcomeCounts(ctx context.Context, q domain.InsightQuery) ([]domain.OutcomeCount, error) {
	rows, err := r.db.Query(ctx, sqlInsightOutcomeCounts, q.IDYapeAccount, q.Flow, q.Since)
	if err != nil {
		return nil, dbError("count outcomes", err)
	}
	defer rows.Close()

	var counts []domain.OutcomeCount
	for rows.Next() {
		var c domain.OutcomeCount
		if err := rows.Scan(&c.Flow, &c.Outcome, &c.Segments); err != nil {
			return nil, dbError("scan outcome count", err)
		}
		counts = append(counts, c)
	}
	if err := rows.Err(); err != nil {
		return nil, dbError("count outcomes", err)
	}

	return counts, nil
}

// LatencyGroups implements repository.InsightsRepository.
func (r *PostgresInsightsRepository) LatencyGroups(ctx context.Context, q domain.InsightQuery) ([]domain.LatencyGroup, error) {
	rows, err := r.db.Query(ctx, sqlInsightLatency, q.IDYapeAccount, q.Flow, q.Since)
	if err != nil {
		return nil, dbError("measure latency", err)
	}
	defer rows.Close()

	var groups []domain.LatencyGroup
	for rows.Next() {
		var g domain.LatencyGroup
		if err := rows.Scan(&g.OTP, &g.Cards, &g.Resolved,
			&g.AskToCard.Average, &g.AskToCard.Median,
			&g.CardToResolution.Average, &g.CardToResolution.Median); err != nil {
			return nil, dbError("scan latency", err)
		}
		groups = append(groups, g)
	}
	if err := rows.Err(); err != nil {
		return nil, dbError("measure latency", err)
	}

	return groups, nil
}

// ToolCounts implements repository.InsightsRepository. The flow of the query is ignored: a tool
// summary carries the flow of its agent, but the health of a tool does not depend on it.
func (r *PostgresInsightsRepository) ToolCounts(ctx context.Context, q domain.InsightQuery) ([]domain.ToolCount, error) {
	rows, err := r.db.Query(ctx, sqlInsightToolCounts, q.IDYapeAccount, q.Since)
	if err != nil {
		return nil, dbError("count tool calls", err)
	}
	defer rows.Close()

	var counts []domain.ToolCount
	for rows.Next() {
		var c domain.ToolCount
		if err := rows.Scan(&c.Tool, &c.Outcome, &c.Calls); err != nil {
			return nil, dbError("scan tool count", err)
		}
		counts = append(counts, c)
	}
	if err := rows.Err(); err != nil {
		return nil, dbError("count tool calls", err)
	}

	return counts, nil
}
