package insights

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/repository/mocks"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
)

var fixedNow = time.Date(2026, time.November, 3, 12, 0, 0, 0, time.UTC)

func newService(repo *mocks.InsightsRepository) *Service {
	s := NewService(repo)
	s.now = func() time.Time { return fixedNow }

	return s
}

func accounts(ids ...string) []domain.AccountActivity {
	out := make([]domain.AccountActivity, 0, len(ids))
	for _, id := range ids {
		out = append(out, domain.AccountActivity{IDYapeAccount: id, Conversations: 1, Messages: 4})
	}

	return out
}

func TestAccounts(t *testing.T) {
	storeDown := errors.New("connection reset")

	tests := []struct {
		name      string
		req       service.AccountsRequest
		wantQuery *domain.AccountQuery // nil when the store must not be called
		rows      []domain.AccountActivity
		repoErr   error
		want      domain.AccountList
		wantErr   error
	}{
		{
			name:      "defaultsToAFullPage",
			req:       service.AccountsRequest{},
			wantQuery: &domain.AccountQuery{Size: domain.AccountListSize + 1},
			rows:      accounts("a", "b"),
			want:      domain.AccountList{Size: domain.AccountListSize, Accounts: accounts("a", "b")},
		},
		{
			name:      "readsOneMoreToKnowThereIsMore",
			req:       service.AccountsRequest{Size: 2, Offset: 4},
			wantQuery: &domain.AccountQuery{Size: 3, Offset: 4},
			rows:      accounts("a", "b", "c"),
			want:      domain.AccountList{Size: 2, Offset: 4, Accounts: accounts("a", "b"), HasMore: true},
		},
		{
			name:      "noAccountsIsAnEmptyListNotAnError",
			req:       service.AccountsRequest{Size: 5},
			wantQuery: &domain.AccountQuery{Size: 6},
			rows:      nil,
			want:      domain.AccountList{Size: 5, Accounts: []domain.AccountActivity{}},
		},
		{name: "sizeOverTheMaximum", req: service.AccountsRequest{Size: domain.AccountListSize + 1}, wantErr: domain.ErrInvalidRequest},
		{name: "negativeSize", req: service.AccountsRequest{Size: -1}, wantErr: domain.ErrInvalidRequest},
		{name: "negativeOffset", req: service.AccountsRequest{Offset: -1}, wantErr: domain.ErrInvalidRequest},
		{
			name:      "storeErrorPassesThrough",
			req:       service.AccountsRequest{},
			wantQuery: &domain.AccountQuery{Size: domain.AccountListSize + 1},
			repoErr:   storeDown,
			wantErr:   storeDown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := mocks.NewInsightsRepository(t)
			if tt.wantQuery != nil {
				repo.On("ListAccounts", mock.Anything, *tt.wantQuery).Return(tt.rows, tt.repoErr)
			}

			got, err := newService(repo).Accounts(t.Context(), tt.req)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFunnel(t *testing.T) {
	storeDown := errors.New("connection reset")
	since := fixedNow.AddDate(0, 0, -30)

	zero := func() map[string]int {
		return map[string]int{
			domain.OutcomeApproved: 0, domain.OutcomeRejected: 0, domain.OutcomeFailed: 0,
			domain.OutcomeAbandoned: 0, domain.OutcomeExpired: 0, domain.OutcomeOpen: 0, domain.OutcomeNone: 0,
		}
	}
	with := func(counts map[string]int) map[string]int {
		out := zero()
		for outcome, n := range counts {
			out[outcome] = n
		}

		return out
	}

	tests := []struct {
		name    string
		req     service.InsightRequest
		counts  []domain.OutcomeCount
		repoErr error
		want    domain.Funnel
		wantErr error
	}{
		{
			name: "countsAttemptsCardsAndTheApprovalRate",
			req:  service.InsightRequest{IDYapeAccount: "acc-1"},
			counts: []domain.OutcomeCount{
				{Flow: "p2p", Outcome: domain.OutcomeApproved, Segments: 3},
				{Flow: "p2p", Outcome: domain.OutcomeRejected, Segments: 1},
				{Flow: "p2p", Outcome: domain.OutcomeExpired, Segments: 2},
				{Flow: "p2p", Outcome: domain.OutcomeOpen, Segments: 5},
				{Flow: "p2p", Outcome: domain.OutcomeNone, Segments: 1},
			},
			want: domain.Funnel{
				IDYapeAccount: "acc-1", Flow: "p2p", WindowDays: 30, Since: since,
				Attempts: 12, CardsOffered: 11,
				Outcomes: with(map[string]int{
					domain.OutcomeApproved: 3, domain.OutcomeRejected: 1, domain.OutcomeExpired: 2,
					domain.OutcomeOpen: 5, domain.OutcomeNone: 1,
				}),
				// open cards are not settled: 3 approved of 3+1+2.
				ApprovalRate: new(0.5),
			},
		},
		{
			name:   "noSegmentsHasNoApprovalRate",
			req:    service.InsightRequest{IDYapeAccount: "acc-1", Flow: "bill_payment", Days: 7},
			counts: nil,
			want: domain.Funnel{
				IDYapeAccount: "acc-1", Flow: "bill_payment", WindowDays: 7, Since: fixedNow.AddDate(0, 0, -7),
				Outcomes: zero(),
			},
		},
		{
			name:   "onlyOpenCardsHaveNoApprovalRate",
			req:    service.InsightRequest{IDYapeAccount: "acc-1"},
			counts: []domain.OutcomeCount{{Flow: "p2p", Outcome: domain.OutcomeOpen, Segments: 2}},
			want: domain.Funnel{
				IDYapeAccount: "acc-1", Flow: "p2p", WindowDays: 30, Since: since,
				Attempts: 2, CardsOffered: 2, Outcomes: with(map[string]int{domain.OutcomeOpen: 2}),
			},
		},
		{
			name:   "anOutcomeTheServiceDoesNotKnowIsStillCounted",
			req:    service.InsightRequest{IDYapeAccount: "acc-1"},
			counts: []domain.OutcomeCount{{Flow: "p2p", Outcome: "cancelled", Segments: 1}},
			want: domain.Funnel{
				IDYapeAccount: "acc-1", Flow: "p2p", WindowDays: 30, Since: since,
				Attempts: 1, CardsOffered: 1, Outcomes: with(map[string]int{"cancelled": 1}),
			},
		},
		{name: "missingAccount", req: service.InsightRequest{}, wantErr: domain.ErrInvalidRequest},
		{name: "malformedAccount", req: service.InsightRequest{IDYapeAccount: "a b"}, wantErr: domain.ErrInvalidRequest},
		{name: "malformedFlow", req: service.InsightRequest{IDYapeAccount: "acc-1", Flow: "P2P;drop"}, wantErr: domain.ErrInvalidRequest},
		{name: "windowTooShort", req: service.InsightRequest{IDYapeAccount: "acc-1", Days: -1}, wantErr: domain.ErrInvalidRequest},
		{name: "windowTooLong", req: service.InsightRequest{IDYapeAccount: "acc-1", Days: domain.MaxWindowDays + 1}, wantErr: domain.ErrInvalidRequest},
		{name: "storeErrorPassesThrough", req: service.InsightRequest{IDYapeAccount: "acc-1"}, repoErr: storeDown, wantErr: storeDown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := mocks.NewInsightsRepository(t)
			if tt.counts != nil || tt.repoErr != nil || tt.wantErr == nil {
				repo.On("OutcomeCounts", mock.Anything, mock.Anything).Return(tt.counts, tt.repoErr)
			}

			got, err := newService(repo).Funnel(t.Context(), tt.req)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFunnelAsksTheStoreForTheAccountFlowAndWindow(t *testing.T) {
	repo := mocks.NewInsightsRepository(t)
	repo.On("OutcomeCounts", mock.Anything, domain.InsightQuery{
		IDYapeAccount: "acc-9", Flow: "bill_payment", Since: fixedNow.AddDate(0, 0, -90),
	}).Return(nil, nil)

	_, err := newService(repo).Funnel(t.Context(), service.InsightRequest{IDYapeAccount: "acc-9", Flow: "bill_payment", Days: 90})

	require.NoError(t, err)
}

func TestLatency(t *testing.T) {
	storeDown := errors.New("connection reset")
	group := domain.LatencyGroup{
		OTP: true, Cards: 2, Resolved: 2,
		AskToCard:        domain.Stat{Average: new(22.0), Median: new(21.5)},
		CardToResolution: domain.Stat{Average: new(18.0), Median: new(18.0)},
	}

	tests := []struct {
		name    string
		req     service.InsightRequest
		groups  []domain.LatencyGroup
		repoErr error
		want    domain.Latency
		wantErr error
	}{
		{
			name:   "passesTheGroupsThrough",
			req:    service.InsightRequest{IDYapeAccount: "acc-1", Days: 14},
			groups: []domain.LatencyGroup{group},
			want: domain.Latency{
				IDYapeAccount: "acc-1", Flow: "p2p", WindowDays: 14, Since: fixedNow.AddDate(0, 0, -14),
				Groups: []domain.LatencyGroup{group},
			},
		},
		{
			name: "noAttemptsIsAnEmptyListNotNull",
			req:  service.InsightRequest{IDYapeAccount: "acc-1"},
			want: domain.Latency{
				IDYapeAccount: "acc-1", Flow: "p2p", WindowDays: 30, Since: fixedNow.AddDate(0, 0, -30),
				Groups: []domain.LatencyGroup{},
			},
		},
		{name: "invalidRequest", req: service.InsightRequest{IDYapeAccount: ""}, wantErr: domain.ErrInvalidRequest},
		{name: "storeErrorPassesThrough", req: service.InsightRequest{IDYapeAccount: "acc-1"}, repoErr: storeDown, wantErr: storeDown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := mocks.NewInsightsRepository(t)
			if tt.groups != nil || tt.repoErr != nil || tt.wantErr == nil {
				repo.On("LatencyGroups", mock.Anything, mock.Anything).Return(tt.groups, tt.repoErr)
			}

			got, err := newService(repo).Latency(t.Context(), tt.req)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestToolHealth(t *testing.T) {
	storeDown := errors.New("connection reset")

	tests := []struct {
		name    string
		req     service.InsightRequest
		counts  []domain.ToolCount
		repoErr error
		want    domain.ToolHealth
		wantErr error
	}{
		{
			name: "addsOutcomesPerToolAndSortsByCalls",
			req:  service.InsightRequest{IDYapeAccount: "acc-1"},
			counts: []domain.ToolCount{
				{Tool: "draft_transfer", Outcome: "ok", Calls: 6},
				{Tool: "validate_p2p_amount", Outcome: "ok", Calls: 6},
				{Tool: "validate_p2p_amount", Outcome: "failed", Calls: 2},
				{Tool: "get_last_payment_card_status", Outcome: "ok", Calls: 4},
			},
			want: domain.ToolHealth{
				IDYapeAccount: "acc-1", WindowDays: 30, Since: fixedNow.AddDate(0, 0, -30),
				Tools: []domain.ToolHealthEntry{
					{Tool: "validate_p2p_amount", Calls: 8, Failed: 2, FailureRate: 0.25},
					{Tool: "draft_transfer", Calls: 6},
					{Tool: "get_last_payment_card_status", Calls: 4},
				},
			},
		},
		{
			name:   "anyOutcomeButOkIsAFailure",
			req:    service.InsightRequest{IDYapeAccount: "acc-1", Days: 1},
			counts: []domain.ToolCount{{Tool: "t", Outcome: "timeout", Calls: 1}},
			want: domain.ToolHealth{
				IDYapeAccount: "acc-1", WindowDays: 1, Since: fixedNow.AddDate(0, 0, -1),
				Tools: []domain.ToolHealthEntry{{Tool: "t", Calls: 1, Failed: 1, FailureRate: 1}},
			},
		},
		{
			name: "noCallsIsAnEmptyListNotNull",
			req:  service.InsightRequest{IDYapeAccount: "acc-1"},
			want: domain.ToolHealth{
				IDYapeAccount: "acc-1", WindowDays: 30, Since: fixedNow.AddDate(0, 0, -30), Tools: []domain.ToolHealthEntry{},
			},
		},
		{name: "invalidRequest", req: service.InsightRequest{IDYapeAccount: "bad id"}, wantErr: domain.ErrInvalidRequest},
		{name: "storeErrorPassesThrough", req: service.InsightRequest{IDYapeAccount: "acc-1"}, repoErr: storeDown, wantErr: storeDown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := mocks.NewInsightsRepository(t)
			if tt.counts != nil || tt.repoErr != nil || tt.wantErr == nil {
				repo.On("ToolCounts", mock.Anything, mock.Anything).Return(tt.counts, tt.repoErr)
			}

			got, err := newService(repo).ToolHealth(t.Context(), tt.req)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestToolHealthTiesSortByName(t *testing.T) {
	repo := mocks.NewInsightsRepository(t)
	repo.On("ToolCounts", mock.Anything, mock.Anything).Return([]domain.ToolCount{
		{Tool: "b", Outcome: "ok", Calls: 2},
		{Tool: "a", Outcome: "ok", Calls: 2},
	}, nil)

	got, err := newService(repo).ToolHealth(context.Background(), service.InsightRequest{IDYapeAccount: "acc-1"})

	require.NoError(t, err)
	require.Len(t, got.Tools, 2)
	assert.Equal(t, "a", got.Tools[0].Tool)
}

func TestNewServiceUsesTheClock(t *testing.T) {
	s := NewService(mocks.NewInsightsRepository(t))

	assert.WithinDuration(t, time.Now(), s.now(), time.Second)
}
