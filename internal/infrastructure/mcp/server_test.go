package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sdkmcp "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service/mocks"
)

var at = time.Date(2026, time.November, 3, 20, 6, 7, 0, time.UTC)

// recorder is a Logger that keeps what it is told. The server calls it from its own goroutines.
type recorder struct {
	mu    sync.Mutex
	lines []string
}

func (r *recorder) Infof(msg string, v ...any)  { r.add("INFO " + fmt.Sprintf(msg, v...)) }
func (r *recorder) Errorf(msg string, v ...any) { r.add("ERROR " + fmt.Sprintf(msg, v...)) }

func (r *recorder) add(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, line)
}

func (r *recorder) text() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return strings.Join(r.lines, "\n")
}

// rig is a connected client with the doubles behind the server.
type rig struct {
	cs       *sdkmcp.ClientSession
	history  *mocks.History
	insights *mocks.Insights
	log      *recorder
}

func newRig(t *testing.T) *rig {
	t.Helper()

	r := &rig{history: mocks.NewHistory(t), insights: mocks.NewInsights(t), log: &recorder{}}
	server := NewServer(Deps{History: r.history, Insights: r.insights, Log: r.log})

	clientTransport, serverTransport := sdkmcp.NewInMemoryTransports()
	ss, err := server.Connect(t.Context(), serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })

	cs, err := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "test", Version: "v0"}, nil).Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	r.cs = cs

	return r
}

func (r *rig) call(t *testing.T, tool string, args map[string]any) *sdkmcp.CallToolResult {
	t.Helper()

	res, err := r.cs.CallTool(t.Context(), &sdkmcp.CallToolParams{Name: tool, Arguments: args})
	require.NoError(t, err)

	return res
}

// raw is the structured content as the model receives it, as JSON.
func raw(t *testing.T, res *sdkmcp.CallToolResult) string {
	t.Helper()
	require.False(t, res.IsError, "unexpected tool error: %v", res.Content)

	b, err := json.Marshal(res.StructuredContent)
	require.NoError(t, err)

	return string(b)
}

func errorText(t *testing.T, res *sdkmcp.CallToolResult) string {
	t.Helper()
	require.True(t, res.IsError, "expected a tool error, got %v", res.StructuredContent)
	require.NotEmpty(t, res.Content)
	text, ok := res.Content[0].(*sdkmcp.TextContent)
	require.True(t, ok)

	return text.Text
}

func decode[T any](t *testing.T, res *sdkmcp.CallToolResult) T {
	t.Helper()

	var out T
	require.NoError(t, json.Unmarshal([]byte(raw(t, res)), &out))

	return out
}

func TestToolsAreReadOnlyAndDescribed(t *testing.T) {
	r := newRig(t)

	listed, err := r.cs.ListTools(t.Context(), nil)

	require.NoError(t, err)
	names := make([]string, 0, len(listed.Tools))
	for _, tool := range listed.Tools {
		names = append(names, tool.Name)
		assert.NotEmpty(t, tool.Description, tool.Name)
		require.NotNil(t, tool.Annotations, tool.Name)
		assert.True(t, tool.Annotations.ReadOnlyHint, tool.Name)
		require.NotNil(t, tool.Annotations.OpenWorldHint, tool.Name)
		assert.False(t, *tool.Annotations.OpenWorldHint, tool.Name)
		assert.NotNil(t, tool.OutputSchema, "%s has an output schema", tool.Name)
	}
	assert.ElementsMatch(t, []string{
		"list_accounts", "get_recent_conversations", "get_transcript",
		"get_activity_funnel", "get_latency_stats", "get_tool_health",
	}, names)
}

func TestTheAccountIsAnArgumentTheModelMustGive(t *testing.T) {
	r := newRig(t)

	listed, err := r.cs.ListTools(t.Context(), nil)
	require.NoError(t, err)

	required := map[string]bool{}
	for _, tool := range listed.Tools {
		schema, err := json.Marshal(tool.InputSchema)
		require.NoError(t, err)
		required[tool.Name] = strings.Contains(string(schema), `"required":["id_yape_account"`)
	}
	assert.Equal(t, map[string]bool{
		"list_accounts": false, "get_recent_conversations": true, "get_transcript": true,
		"get_activity_funnel": true, "get_latency_stats": true, "get_tool_health": true,
	}, required)
}

func TestTheServerSaysHowToUseIt(t *testing.T) {
	r := newRig(t)

	init := r.cs.InitializeResult()

	require.NotNil(t, init)
	assert.Equal(t, serverName, init.ServerInfo.Name)
	assert.Contains(t, init.Instructions, "list_accounts")
	assert.Contains(t, init.Instructions, "untrusted")
}

func TestListAccounts(t *testing.T) {
	t.Run("returnsThePageWithDefaultsAndRFC3339Times", func(t *testing.T) {
		r := newRig(t)
		r.insights.On("Accounts", mock.Anything, service.AccountsRequest{Size: 20}).Return(domain.AccountList{
			Size: 20, HasMore: true,
			Accounts: []domain.AccountActivity{{IDYapeAccount: "acc-1", Conversations: 2, Messages: 9, FirstOccurredAt: at, LastOccurredAt: at.Add(time.Hour)}},
		}, nil)

		out := decode[accountsResult](t, r.call(t, "list_accounts", nil))

		assert.Equal(t, accountsResult{
			Accounts: []accountJSON{{IDYapeAccount: "acc-1", Conversations: 2, Messages: 9, FirstActivity: at, LastActivity: at.Add(time.Hour)}},
			Limit:    20, HasMore: true,
		}, out)
	})

	t.Run("passesTheLimitAndOffsetOn", func(t *testing.T) {
		r := newRig(t)
		r.insights.On("Accounts", mock.Anything, service.AccountsRequest{Size: 5, Offset: 10}).Return(domain.AccountList{Size: 5, Offset: 10}, nil)

		out := decode[accountsResult](t, r.call(t, "list_accounts", map[string]any{"limit": 5, "offset": 10}))

		assert.Equal(t, 10, out.Offset)
	})

	t.Run("noAccountsIsAnEmptyListNotNull", func(t *testing.T) {
		r := newRig(t)
		r.insights.On("Accounts", mock.Anything, mock.Anything).Return(domain.AccountList{}, nil)

		assert.Contains(t, raw(t, r.call(t, "list_accounts", nil)), `"accounts":[]`)
	})
}

// TestEveryTimeIsInUTC: the store hands back times in the zone of its session, and a model that
// compares an offset of -05:00 with a Z gets it wrong.
func TestEveryTimeIsInUTC(t *testing.T) {
	bogota := time.FixedZone("-05", -5*60*60)
	local := at.In(bogota)
	r := newRig(t)
	r.insights.On("Accounts", mock.Anything, mock.Anything).Return(domain.AccountList{
		Accounts: []domain.AccountActivity{{IDYapeAccount: "acc-1", FirstOccurredAt: local, LastOccurredAt: local}},
	}, nil)
	r.insights.On("Funnel", mock.Anything, mock.Anything).Return(domain.Funnel{Since: local, Outcomes: map[string]int{}}, nil)
	r.insights.On("Latency", mock.Anything, mock.Anything).Return(domain.Latency{Since: local}, nil)
	r.insights.On("ToolHealth", mock.Anything, mock.Anything).Return(domain.ToolHealth{Since: local}, nil)
	r.history.On("Conversations", mock.Anything, mock.Anything).Return(domain.ConversationList{
		Conversations: []domain.ConversationSummary{{ConversationID: "conv_v1_a", FirstOccurredAt: local, LastOccurredAt: local}},
	}, nil)
	r.history.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{
		Messages: []domain.Message{{Sequence: 2, Role: domain.RoleUser, Text: "x", OccurredAt: local}},
	}, nil)

	for tool, args := range map[string]map[string]any{
		"list_accounts":            nil,
		"get_recent_conversations": {"id_yape_account": "acc-1"},
		"get_transcript":           {"id_yape_account": "acc-1", "conversation_id": "conv_v1_a"},
		"get_activity_funnel":      {"id_yape_account": "acc-1"},
		"get_latency_stats":        {"id_yape_account": "acc-1"},
		"get_tool_health":          {"id_yape_account": "acc-1"},
	} {
		out := raw(t, r.call(t, tool, args))

		assert.NotContains(t, out, "-05:00", tool)
		assert.Contains(t, out, "2026-11-03T20:06:07Z", tool)
	}
}

func TestGetRecentConversations(t *testing.T) {
	r := newRig(t)
	r.history.On("Conversations", mock.Anything, mock.MatchedBy(func(req service.ConversationsRequest) bool {
		return req.IDYapeAccount == "acc-1" && req.Size != nil && *req.Size == "10" && req.Before == nil && req.After == nil
	})).Return(domain.ConversationList{
		HasMore: true,
		Conversations: []domain.ConversationSummary{
			{ConversationID: "conv_v1_a", MessageCount: 6, FirstOccurredAt: at, LastOccurredAt: at.Add(time.Minute)},
		},
	}, nil)

	out := decode[conversationsResult](t, r.call(t, "get_recent_conversations", map[string]any{"id_yape_account": "acc-1"}))

	assert.Equal(t, conversationsResult{
		IDYapeAccount: "acc-1", HasMore: true,
		Conversations: []conversationJSON{{ConversationID: "conv_v1_a", Messages: 6, FirstMessageAt: at, LastMessageAt: at.Add(time.Minute)}},
	}, out)
}

func TestGetRecentConversationsWithAGivenLimitAndNoConversations(t *testing.T) {
	r := newRig(t)
	r.history.On("Conversations", mock.Anything, mock.MatchedBy(func(req service.ConversationsRequest) bool {
		return req.Size != nil && *req.Size == "3"
	})).Return(domain.ConversationList{}, nil)

	res := r.call(t, "get_recent_conversations", map[string]any{"id_yape_account": "acc-1", "limit": 3})

	assert.Contains(t, raw(t, res), `"conversations":[]`)
}

func TestGetTranscript(t *testing.T) {
	t.Run("readsTheNewestPage", func(t *testing.T) {
		r := newRig(t)
		r.history.On("Messages", mock.Anything, mock.MatchedBy(func(req service.MessagesRequest) bool {
			return req.ConversationID == "conv_v1_a" && req.Before == nil && req.Size != nil && *req.Size == "30"
		})).Return(domain.MessagePage{
			ConversationID: "conv_v1_a", HasMore: true, BeforeCursor: "5",
			Messages: []domain.Message{
				{Sequence: 5, Role: domain.RoleUser, Text: "@(Juan Perez) 20 soles", Flow: "p2p", OccurredAt: at},
				{Sequence: 11, Role: domain.RoleMaker, Text: "Revisa tu yapeo", Truncated: true, OccurredAt: at.Add(time.Second)},
			},
		}, nil)

		res := r.call(t, "get_transcript", map[string]any{"id_yape_account": "acc-1", "conversation_id": "conv_v1_a"})
		out := decode[transcriptResult](t, res)

		assert.Equal(t, transcriptResult{
			ConversationID: "conv_v1_a", HasOlder: true, OlderCursor: "5",
			Messages: []messageJSON{
				{ID: "5", Role: "appUser", Text: "@(Juan Perez) 20 soles", Flow: "p2p", At: at},
				{ID: "11", Role: "appMaker", Text: "Revisa tu yapeo", At: at.Add(time.Second), Truncated: true},
			},
		}, out)
		// A false truncated and an empty flow are left out, so a message stays short.
		assert.NotContains(t, strings.Split(raw(t, res), `"id":"11"`)[0], `"truncated":false`)
	})

	t.Run("passesBeforeAndTheLimitOn", func(t *testing.T) {
		r := newRig(t)
		r.history.On("Messages", mock.Anything, mock.MatchedBy(func(req service.MessagesRequest) bool {
			return req.Before != nil && *req.Before == "5" && req.Size != nil && *req.Size == "2"
		})).Return(domain.MessagePage{ConversationID: "conv_v1_a", Messages: []domain.Message{{Sequence: 3, Role: domain.RoleMaker, Text: "hola"}}}, nil)

		out := decode[transcriptResult](t, r.call(t, "get_transcript", map[string]any{"id_yape_account": "acc-1", "conversation_id": "conv_v1_a", "before": "5", "limit": 2}))

		assert.False(t, out.HasOlder)
		assert.Empty(t, out.OlderCursor, "no older messages, no cursor")
	})

	t.Run("noMessagesIsAnEmptyListNotNull", func(t *testing.T) {
		r := newRig(t)
		r.history.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{ConversationID: "conv_v1_a"}, nil)

		assert.Contains(t, raw(t, r.call(t, "get_transcript", map[string]any{"id_yape_account": "acc-1", "conversation_id": "conv_v1_a"})), `"messages":[]`)
	})
}

func TestGetActivityFunnel(t *testing.T) {
	r := newRig(t)
	r.insights.On("Funnel", mock.Anything, service.InsightRequest{IDYapeAccount: "acc-1", Flow: "p2p", Days: 7}).Return(domain.Funnel{
		IDYapeAccount: "acc-1", Flow: "p2p", WindowDays: 7, Since: at, Attempts: 7, CardsOffered: 6,
		Outcomes:     map[string]int{"approved": 3, "rejected": 1, "expired": 2, "none": 1},
		ApprovalRate: new(0.5),
	}, nil)

	out := decode[funnelResult](t, r.call(t, "get_activity_funnel", map[string]any{"id_yape_account": "acc-1", "flow": "p2p", "days": 7}))

	assert.Equal(t, 7, out.Attempts)
	assert.Equal(t, 6, out.CardsOffered)
	assert.Equal(t, map[string]int{"approved": 3, "rejected": 1, "expired": 2, "none": 1}, out.Outcomes)
	require.NotNil(t, out.ApprovalRate)
	assert.InDelta(t, 0.5, *out.ApprovalRate, 0.0001)
}

func TestTheApprovalRateIsAbsentWhileNoCardIsSettled(t *testing.T) {
	r := newRig(t)
	r.insights.On("Funnel", mock.Anything, mock.Anything).Return(domain.Funnel{IDYapeAccount: "acc-1", Flow: "p2p", Outcomes: map[string]int{"open": 1}}, nil)

	assert.NotContains(t, raw(t, r.call(t, "get_activity_funnel", map[string]any{"id_yape_account": "acc-1"})), "approval_rate")
}

func TestApprovalRateIsRoundedToAThousandth(t *testing.T) {
	r := newRig(t)
	r.insights.On("Funnel", mock.Anything, mock.Anything).Return(domain.Funnel{
		IDYapeAccount: "acc-1", Flow: "p2p", Outcomes: map[string]int{}, ApprovalRate: new(2.0 / 3.0),
	}, nil)

	out := decode[funnelResult](t, r.call(t, "get_activity_funnel", map[string]any{"id_yape_account": "acc-1"}))

	require.NotNil(t, out.ApprovalRate)
	assert.Equal(t, 0.667, *out.ApprovalRate)
}

func TestGetLatencyStats(t *testing.T) {
	r := newRig(t)
	r.insights.On("Latency", mock.Anything, service.InsightRequest{IDYapeAccount: "acc-1"}).Return(domain.Latency{
		IDYapeAccount: "acc-1", Flow: "p2p", WindowDays: 30, Since: at,
		Groups: []domain.LatencyGroup{
			{
				OTP: false, Cards: 3, Resolved: 2,
				AskToCard:        domain.Stat{Average: new(21.3333), Median: new(22.0)},
				CardToResolution: domain.Stat{Average: new(4.04), Median: new(4.0)},
			},
			{OTP: true, Cards: 1, AskToCard: domain.Stat{Average: new(20.0), Median: new(20.0)}},
		},
	}, nil)

	res := r.call(t, "get_latency_stats", map[string]any{"id_yape_account": "acc-1"})
	out := decode[latencyResult](t, res)

	require.Len(t, out.Groups, 2)
	first, second := out.Groups[0], out.Groups[1]
	assert.False(t, first.AskedOTP)
	assert.Equal(t, 3, first.Cards)
	assert.Equal(t, 2, first.Resolved)
	assert.Equal(t, 21.3, *first.AskToCardSeconds.Average, "rounded to a tenth of a second")
	assert.Equal(t, 22.0, *first.AskToCardSeconds.Median)
	assert.Equal(t, 4.0, *first.CardToResolutionSeconds.Average)
	assert.True(t, second.AskedOTP)
	assert.Nil(t, second.CardToResolutionSeconds.Average, "nothing was decided, so there is nothing to average")
}

func TestNoAttemptsIsAnEmptyGroupListNotNull(t *testing.T) {
	r := newRig(t)
	r.insights.On("Latency", mock.Anything, mock.Anything).Return(domain.Latency{IDYapeAccount: "acc-1", Flow: "p2p"}, nil)

	assert.Contains(t, raw(t, r.call(t, "get_latency_stats", map[string]any{"id_yape_account": "acc-1"})), `"groups":[]`)
}

func TestGetToolHealth(t *testing.T) {
	r := newRig(t)
	r.insights.On("ToolHealth", mock.Anything, service.InsightRequest{IDYapeAccount: "acc-1", Days: 14}).Return(domain.ToolHealth{
		IDYapeAccount: "acc-1", WindowDays: 14, Since: at,
		Tools: []domain.ToolHealthEntry{
			{Tool: "validate_p2p_amount", Calls: 3, Failed: 1, FailureRate: 1.0 / 3.0},
			{Tool: "draft_transfer", Calls: 2},
		},
	}, nil)

	out := decode[toolHealthResult](t, r.call(t, "get_tool_health", map[string]any{"id_yape_account": "acc-1", "days": 14}))

	assert.Equal(t, []toolJSON{
		{Tool: "validate_p2p_amount", Calls: 3, Failed: 1, FailureRate: 0.333},
		{Tool: "draft_transfer", Calls: 2},
	}, out.Tools)
}

func TestNoToolCallsIsAnEmptyListNotNull(t *testing.T) {
	r := newRig(t)
	r.insights.On("ToolHealth", mock.Anything, mock.Anything).Return(domain.ToolHealth{IDYapeAccount: "acc-1"}, nil)

	assert.Contains(t, raw(t, r.call(t, "get_tool_health", map[string]any{"id_yape_account": "acc-1"})), `"tools":[]`)
}

// TestErrorsAreWordedForTheModel: a mistake of the model says what to fix; a failure of the store
// says only to try again, and the cause goes to the log, not to the model.
func TestErrorsAreWordedForTheModel(t *testing.T) {
	storeDown := errors.New("dial tcp 10.0.0.5:5432: connection refused")

	tests := []struct {
		name       string
		err        error
		want       string
		wantLogged string
	}{
		{name: "invalidRequestSaysWhatToFix", err: domain.InvalidRequest("days must be from 1 to 365"), want: "days must be from 1 to 365"},
		{name: "invalidRequestWithoutWords", err: fmt.Errorf("wrapped: %w", domain.ErrInvalidRequest), want: "the request is invalid"},
		{name: "invalidRequestThroughContext", err: fmt.Errorf("history: %w", domain.InvalidRequest("size must be from 1 to 100")), want: "size must be from 1 to 100"},
		{name: "invalidCursor", err: domain.ErrInvalidCursor, want: "before is not the id of a message of this conversation"},
		{name: "notFound", err: domain.ErrNotFound, want: "not found: the conversation has no chat messages"},
		{name: "deadline", err: context.DeadlineExceeded, want: "the query took too long; try a shorter window"},
		{name: "anythingElseIsHiddenAndLogged", err: storeDown, want: "the history is unavailable; try again", wantLogged: "connection refused"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRig(t)
			r.insights.On("ToolHealth", mock.Anything, mock.Anything).Return(domain.ToolHealth{}, tt.err)

			text := errorText(t, r.call(t, "get_tool_health", map[string]any{"id_yape_account": "acc-1"}))

			assert.Equal(t, tt.want, text)
			if tt.wantLogged != "" {
				assert.Contains(t, r.log.text(), "ERROR tool get_tool_health")
				assert.Contains(t, r.log.text(), tt.wantLogged)
				assert.NotContains(t, text, tt.wantLogged, "the cause stays out of the model's view")
			} else {
				assert.NotContains(t, r.log.text(), "ERROR")
			}
		})
	}
}

func TestEveryToolReportsAStoreFailureTheSameWay(t *testing.T) {
	down := errors.New("down")
	account := map[string]any{"id_yape_account": "acc-1"}
	tests := []struct {
		tool    string
		args    map[string]any
		arrange func(r *rig)
	}{
		{"list_accounts", nil, func(r *rig) {
			r.insights.On("Accounts", mock.Anything, mock.Anything).Return(domain.AccountList{}, down)
		}},
		{"get_recent_conversations", account, func(r *rig) {
			r.history.On("Conversations", mock.Anything, mock.Anything).Return(domain.ConversationList{}, down)
		}},
		{"get_transcript", map[string]any{"id_yape_account": "acc-1", "conversation_id": "conv_v1_a"}, func(r *rig) {
			r.history.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{}, down)
		}},
		{"get_activity_funnel", account, func(r *rig) {
			r.insights.On("Funnel", mock.Anything, mock.Anything).Return(domain.Funnel{}, down)
		}},
		{"get_latency_stats", account, func(r *rig) {
			r.insights.On("Latency", mock.Anything, mock.Anything).Return(domain.Latency{}, down)
		}},
		{"get_tool_health", account, func(r *rig) {
			r.insights.On("ToolHealth", mock.Anything, mock.Anything).Return(domain.ToolHealth{}, down)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.tool, func(t *testing.T) {
			r := newRig(t)
			tt.arrange(r)

			res := r.call(t, tt.tool, tt.args)

			assert.Equal(t, "the history is unavailable; try again", errorText(t, res))
		})
	}
}

// TestAnArgumentTheToolDoesNotDeclareIsRefused: the input schema is closed, so a model that sends
// the account to a tool that has none, or invents a field, is told so instead of being ignored.
func TestAnArgumentTheToolDoesNotDeclareIsRefused(t *testing.T) {
	r := newRig(t)

	res := r.call(t, "list_accounts", map[string]any{"id_yape_account": "acc-1"})

	assert.Contains(t, errorText(t, res), "unexpected additional properties")
}

// TestTheAccountIsRequired: a statistic without an account is refused before the service is asked.
func TestTheAccountIsRequired(t *testing.T) {
	r := newRig(t)

	res := r.call(t, "get_activity_funnel", map[string]any{"days": 7})

	assert.Contains(t, errorText(t, res), "id_yape_account")
}

// TestLogsNameTheToolAndNeverTheArguments: no account, conversation or message text reaches a log.
func TestLogsNameTheToolAndNeverTheArguments(t *testing.T) {
	r := newRig(t)
	r.history.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{
		ConversationID: "conv_v1_secret", Messages: []domain.Message{{Sequence: 2, Role: domain.RoleUser, Text: "mi clave es 1234"}},
	}, nil)
	r.insights.On("Funnel", mock.Anything, mock.Anything).Return(domain.Funnel{}, domain.ErrNotFound)

	r.call(t, "get_transcript", map[string]any{"id_yape_account": "acc-1", "conversation_id": "conv_v1_secret"})
	r.call(t, "get_activity_funnel", map[string]any{"id_yape_account": "acc-secret"})

	logged := r.log.text()
	assert.Contains(t, logged, "tool get_transcript ok in")
	assert.Contains(t, logged, "tool get_activity_funnel failed in")
	for _, secret := range []string{"conv_v1_secret", "acc-secret", "mi clave"} {
		assert.NotContains(t, logged, secret)
	}
}

func TestHealth(t *testing.T) {
	tests := []struct {
		name   string
		ping   Pinger
		status int
		body   string
	}{
		{name: "databaseAnswers", ping: func(context.Context) error { return nil }, status: http.StatusOK, body: `{"status":"ok"}`},
		{name: "databaseDown", ping: func(context.Context) error { return errors.New("down") }, status: http.StatusServiceUnavailable, body: `{"status":"unavailable"}`},
		{name: "noPingerIsOk", ping: nil, status: http.StatusOK, body: `{"status":"ok"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := NewServer(Deps{History: mocks.NewHistory(t), Insights: mocks.NewInsights(t), Log: &recorder{}})
			rec := httptest.NewRecorder()

			NewHandler(server, tt.ping).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

			assert.Equal(t, tt.status, rec.Code)
			assert.JSONEq(t, tt.body, rec.Body.String())
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		})
	}
}

func TestHealthOnlyAnswersGet(t *testing.T) {
	server := NewServer(Deps{History: mocks.NewHistory(t), Insights: mocks.NewInsights(t), Log: &recorder{}})
	rec := httptest.NewRecorder()

	NewHandler(server, nil).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/health", nil))

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// TestOverStreamableHTTP: a client reaches the tools through the HTTP endpoint, with no session kept.
func TestOverStreamableHTTP(t *testing.T) {
	history, insights := mocks.NewHistory(t), mocks.NewInsights(t)
	insights.On("Accounts", mock.Anything, mock.Anything).Return(domain.AccountList{
		Size: 20, Accounts: []domain.AccountActivity{{IDYapeAccount: "acc-1", Conversations: 1, Messages: 4, FirstOccurredAt: at, LastOccurredAt: at}},
	}, nil)
	httpServer := httptest.NewServer(NewHandler(NewServer(Deps{History: history, Insights: insights, Log: &recorder{}}), nil))
	t.Cleanup(httpServer.Close)

	client := sdkmcp.NewClient(&sdkmcp.Implementation{Name: "test", Version: "v0"}, nil)
	cs, err := client.Connect(t.Context(), &sdkmcp.StreamableClientTransport{Endpoint: httpServer.URL + mcpPath}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	res, err := cs.CallTool(t.Context(), &sdkmcp.CallToolParams{Name: "list_accounts"})

	require.NoError(t, err)
	assert.Contains(t, raw(t, res), `"id_yape_account":"acc-1"`)
}

func TestNewHTTPServerBoundsSlowClients(t *testing.T) {
	h := http.NewServeMux()

	s := NewHTTPServer(":3200", h)

	assert.Equal(t, ":3200", s.Addr)
	assert.Same(t, h, s.Handler)
	assert.Positive(t, s.ReadHeaderTimeout)
	assert.Positive(t, s.IdleTimeout)
}

func TestRound(t *testing.T) {
	assert.Nil(t, round(nil, 10))
	assert.Equal(t, 1.2, *round(new(1.23456), 10))
	assert.Equal(t, 0.667, *round(new(0.66666), 1000))
}

// A conversation is read for an account: the tool takes it, passes it on, and a conversation of another account is not found.
func TestGetTranscript_TheAccount(t *testing.T) {
	t.Run("itIsPassedOn", func(t *testing.T) {
		r := newRig(t)
		r.history.On("Messages", mock.Anything, service.MessagesRequest{
			IDYapeAccount: "acc-9", ConversationID: "conv_v1_a", Size: new("30"),
		}).Return(domain.MessagePage{ConversationID: "conv_v1_a"}, nil).Once()

		r.call(t, "get_transcript", map[string]any{"id_yape_account": "acc-9", "conversation_id": "conv_v1_a"})
	})

	t.Run("aConversationOfAnotherAccountIsNotFound", func(t *testing.T) {
		r := newRig(t)
		r.history.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{}, domain.ErrNotFound)

		res := r.call(t, "get_transcript", map[string]any{"id_yape_account": "acc-2", "conversation_id": "conv_v1_a"})

		require.True(t, res.IsError)
		text := res.Content[0].(*sdkmcp.TextContent).Text
		assert.Contains(t, text, "not found")
		assert.NotContains(t, text, "acc-2", "the answer does not echo the account")
	})

	t.Run("withoutItTheToolIsNotCalled", func(t *testing.T) {
		r := newRig(t) // no call of the service is expected

		res := r.call(t, "get_transcript", map[string]any{"conversation_id": "conv_v1_a"})

		assert.True(t, res.IsError)
	})
}
