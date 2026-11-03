// Package insights answers questions about the conversations of an account with numbers: how
// attempts ended, how long they took, which tools of the agent failed. It checks a request,
// applies the defaults and turns the rows the store returns into the figures a caller reads.
// It never returns message text.
package insights

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"time"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
)

var _ service.Insights = (*Service)(nil)

// outcomes lists every outcome a funnel reports, so a funnel always carries all of them.
var outcomes = []string{
	domain.OutcomeApproved,
	domain.OutcomeRejected,
	domain.OutcomeFailed,
	domain.OutcomeAbandoned,
	domain.OutcomeExpired,
	domain.OutcomeOpen,
	domain.OutcomeNone,
}

// Service implements service.Insights over an insights repository.
type Service struct {
	repo repository.InsightsRepository
	now  func() time.Time
}

// NewService returns the insights service.
func NewService(repo repository.InsightsRepository) *Service {
	return &Service{repo: repo, now: time.Now}
}

// Accounts implements service.Insights. It reads one account more than the page holds, which
// tells whether more exist without a second query.
func (s *Service) Accounts(ctx context.Context, req service.AccountsRequest) (domain.AccountList, error) {
	size := cmp.Or(req.Size, domain.AccountListSize)
	if size < 1 || size > domain.AccountListSize {
		return domain.AccountList{}, domain.InvalidRequest("size must be from 1 to %d", domain.AccountListSize)
	}
	if req.Offset < 0 {
		return domain.AccountList{}, domain.InvalidRequest("offset must not be negative")
	}

	accounts, err := s.repo.ListAccounts(ctx, domain.AccountQuery{Size: size + 1, Offset: req.Offset})
	if err != nil {
		return domain.AccountList{}, err
	}

	hasMore := len(accounts) > size
	if hasMore {
		accounts = accounts[:size]
	}
	if accounts == nil {
		accounts = []domain.AccountActivity{}
	}

	return domain.AccountList{Size: size, Offset: req.Offset, Accounts: accounts, HasMore: hasMore}, nil
}

// Funnel implements service.Insights.
func (s *Service) Funnel(ctx context.Context, req service.InsightRequest) (domain.Funnel, error) {
	query, days, err := s.scope(req)
	if err != nil {
		return domain.Funnel{}, err
	}

	counts, err := s.repo.OutcomeCounts(ctx, query)
	if err != nil {
		return domain.Funnel{}, err
	}

	funnel := domain.Funnel{
		IDYapeAccount: query.IDYapeAccount,
		Flow:          query.Flow,
		WindowDays:    days,
		Since:         query.Since,
		Outcomes:      make(map[string]int, len(outcomes)),
	}
	for _, outcome := range outcomes {
		funnel.Outcomes[outcome] = 0
	}
	for _, count := range counts {
		funnel.Outcomes[count.Outcome] += count.Segments
		funnel.Attempts += count.Segments
	}
	funnel.CardsOffered = funnel.Attempts - funnel.Outcomes[domain.OutcomeNone]

	settled := funnel.Outcomes[domain.OutcomeApproved] + funnel.Outcomes[domain.OutcomeRejected] +
		funnel.Outcomes[domain.OutcomeFailed] + funnel.Outcomes[domain.OutcomeAbandoned] +
		funnel.Outcomes[domain.OutcomeExpired]
	if settled > 0 {
		funnel.ApprovalRate = new(float64(funnel.Outcomes[domain.OutcomeApproved]) / float64(settled))
	}

	return funnel, nil
}

// Latency implements service.Insights.
func (s *Service) Latency(ctx context.Context, req service.InsightRequest) (domain.Latency, error) {
	query, days, err := s.scope(req)
	if err != nil {
		return domain.Latency{}, err
	}

	groups, err := s.repo.LatencyGroups(ctx, query)
	if err != nil {
		return domain.Latency{}, err
	}
	if groups == nil {
		groups = []domain.LatencyGroup{}
	}

	return domain.Latency{
		IDYapeAccount: query.IDYapeAccount,
		Flow:          query.Flow,
		WindowDays:    days,
		Since:         query.Since,
		Groups:        groups,
	}, nil
}

// ToolHealth implements service.Insights.
func (s *Service) ToolHealth(ctx context.Context, req service.InsightRequest) (domain.ToolHealth, error) {
	query, days, err := s.scope(req)
	if err != nil {
		return domain.ToolHealth{}, err
	}

	counts, err := s.repo.ToolCounts(ctx, query)
	if err != nil {
		return domain.ToolHealth{}, err
	}

	byTool := make(map[string]*domain.ToolHealthEntry)
	for _, count := range counts {
		entry, found := byTool[count.Tool]
		if !found {
			entry = &domain.ToolHealthEntry{Tool: count.Tool}
			byTool[count.Tool] = entry
		}
		entry.Calls += count.Calls
		if count.Outcome != domain.ToolOutcomeOK {
			entry.Failed += count.Calls
		}
	}

	tools := make([]domain.ToolHealthEntry, 0, len(byTool))
	for _, entry := range byTool {
		entry.FailureRate = float64(entry.Failed) / float64(entry.Calls)
		tools = append(tools, *entry)
	}
	slices.SortFunc(tools, func(a, b domain.ToolHealthEntry) int {
		return cmp.Or(cmp.Compare(b.Calls, a.Calls), strings.Compare(a.Tool, b.Tool))
	})

	return domain.ToolHealth{
		IDYapeAccount: query.IDYapeAccount,
		WindowDays:    days,
		Since:         query.Since,
		Tools:         tools,
	}, nil
}

// scope checks a request and turns it into the query the store reads, with the window in days.
func (s *Service) scope(req service.InsightRequest) (domain.InsightQuery, int, error) {
	if !domain.ValidIdentifier(req.IDYapeAccount) {
		return domain.InsightQuery{}, 0, domain.InvalidRequest("id_yape_account is missing or malformed")
	}

	flow := cmp.Or(req.Flow, domain.DefaultInsightFlow)
	if !domain.ValidFlow(flow) {
		return domain.InsightQuery{}, 0, domain.InvalidRequest("flow is malformed")
	}

	days := cmp.Or(req.Days, domain.DefaultWindowDays)
	if days < 1 || days > domain.MaxWindowDays {
		return domain.InsightQuery{}, 0, domain.InvalidRequest("days must be from 1 to %d", domain.MaxWindowDays)
	}

	return domain.InsightQuery{
		IDYapeAccount: req.IDYapeAccount,
		Flow:          flow,
		Since:         s.now().UTC().AddDate(0, 0, -days),
	}, days, nil
}
