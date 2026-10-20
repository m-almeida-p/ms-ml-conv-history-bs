package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service/mocks"
)

const specPath = "../../../docs/ms-ml-conv-history-bs-openapi.yml"

// schema is the part of a Swagger 2.0 definition the contract checks read.
type schema struct {
	Ref        string            `yaml:"$ref"`
	Type       string            `yaml:"type"`
	Properties map[string]schema `yaml:"properties"`
	Items      *schema           `yaml:"items"`
	Enum       []any             `yaml:"enum"`
}

type spec struct {
	Paths       map[string]map[string]any `yaml:"paths"`
	Definitions map[string]schema         `yaml:"definitions"`
}

func loadSpec(t *testing.T) spec {
	t.Helper()

	data, err := os.ReadFile(specPath)
	require.NoError(t, err, "run make openapi")

	var s spec
	require.NoError(t, yaml.Unmarshal(data, &s))

	return s
}

// documentedRoutes lists "METHOD /path" as the server writes it (:param, not {param}).
var braces = regexp.MustCompile(`\{([^}]+)\}`)

func documentedRoutes(s spec) []string {
	var routes []string
	for path, methods := range s.Paths {
		echoPath := braces.ReplaceAllString(path, ":$1")
		for method := range methods {
			routes = append(routes, strings.ToUpper(method)+" "+echoPath)
		}
	}
	sort.Strings(routes)

	return routes
}

// servedRoutes lists "METHOD /path" for what the router really serves, leaving out
// the pseudo routes Echo adds for its own errors.
func servedRoutes(e *echo.Echo) []string {
	verbs := []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}
	var routes []string
	for _, r := range e.Routes() {
		if slices.Contains(verbs, r.Method) {
			routes = append(routes, r.Method+" "+r.Path)
		}
	}
	sort.Strings(routes)

	return routes
}

// missingFrom returns what is in a and not in b.
func missingFrom(a, b []string) []string {
	var out []string
	for _, x := range a {
		if !slices.Contains(b, x) {
			out = append(out, x)
		}
	}

	return out
}

// violations compares a decoded JSON value with its schema and lists every
// difference: a key the schema does not document, a wrong type, a value outside an enum.
func violations(s spec, def schema, value any, path string) []string {
	if def.Ref != "" {
		def = s.Definitions[strings.TrimPrefix(def.Ref, "#/definitions/")]
	}

	switch v := value.(type) {
	case map[string]any:
		if def.Type != "object" {
			return []string{path + ": an object where the spec says " + def.Type}
		}
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var out []string
		for _, k := range keys {
			property, ok := def.Properties[k]
			if !ok {
				out = append(out, path+"."+k+": not in the spec")
				continue
			}
			out = append(out, violations(s, property, v[k], path+"."+k)...)
		}

		return out
	case []any:
		if def.Type != "array" || def.Items == nil {
			return []string{path + ": an array where the spec says " + def.Type}
		}
		var out []string
		for i, item := range v {
			out = append(out, violations(s, *def.Items, item, path+"[]")...)
			_ = i
		}

		return out
	case string:
		if def.Type != "string" {
			return []string{path + ": a string where the spec says " + def.Type}
		}
		if len(def.Enum) > 0 && !slices.Contains(def.Enum, any(v)) {
			return []string{path + ": " + v + " is not one of the documented values"}
		}
	case float64:
		if def.Type != "number" && def.Type != "integer" {
			return []string{path + ": a number where the spec says " + def.Type}
		}
	case bool:
		if def.Type != "boolean" {
			return []string{path + ": a boolean where the spec says " + def.Type}
		}
	}

	return nil
}

func TestOpenAPIDocumentsExactlyTheRoutesServed(t *testing.T) {
	s := loadSpec(t)
	e := NewServer(mocks.NewHistory(t), &recordingLog{}, nil)

	documented, served := documentedRoutes(s), servedRoutes(e)

	assert.Empty(t, missingFrom(served, documented), "served but not documented: add the swag annotations and run make openapi")
	assert.Empty(t, missingFrom(documented, served), "documented but not served: run make openapi")
	assert.NotEmpty(t, served)
}

func TestRepliesConformToTheOpenAPISpec(t *testing.T) {
	s := loadSpec(t)
	at := time.Date(2026, 10, 20, 14, 7, 23, 792_000_000, time.UTC)
	message := func(sequence int, role domain.Role, event string, truncated bool) domain.Message {
		return domain.Message{ConversationID: "conv_v1_a", Sequence: sequence, Role: role, AuthorID: "acc-1", Type: "text",
			Text: "hola", Truncated: truncated, SourceEvent: event, ReceivedAt: at}
	}

	tests := []struct {
		name       string
		target     string
		expect     func(h *mocks.History)
		ping       Pinger
		wantStatus int
		definition string
	}{
		{
			name:   "newestPageLinksBack",
			target: "/v1/conversations/conv_v1_a/messages",
			expect: func(h *mocks.History) {
				h.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{
					ConversationID: "conv_v1_a", Direction: domain.DirectionNewest, HasMore: true, BeforeCursor: "1", AfterCursor: "2",
					Messages: []domain.Message{message(1, domain.RoleUser, "UserMessageSent", false), message(2, domain.RoleMaker, "GuardrailResponseSent", true)},
				}, nil).Once()
			},
			wantStatus: http.StatusOK, definition: "handler.messagesResponse",
		},
		{
			name:   "afterPageLinksForward",
			target: "/v1/conversations/conv_v1_a/messages?page[after]=1",
			expect: func(h *mocks.History) {
				h.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{
					ConversationID: "conv_v1_a", Direction: domain.DirectionAfter, HasMore: true, BeforeCursor: "2", AfterCursor: "2",
					Messages: []domain.Message{message(2, domain.RoleMaker, "AgentMessageSent", false)},
				}, nil).Once()
			},
			wantStatus: http.StatusOK, definition: "handler.messagesResponse",
		},
		{
			name:   "emptyPage",
			target: "/v1/conversations/conv_v1_a/messages?page[after]=9",
			expect: func(h *mocks.History) {
				h.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{ConversationID: "conv_v1_a", Direction: domain.DirectionAfter}, nil).Once()
			},
			wantStatus: http.StatusOK, definition: "handler.messagesResponse",
		},
		{
			name:   "conversationList",
			target: "/v1/conversations?idYapeAccount=acc-1",
			expect: func(h *mocks.History) {
				h.On("Conversations", mock.Anything, "acc-1").Return(domain.ConversationList{HasMore: true, Conversations: []domain.ConversationSummary{
					{ConversationID: "conv_v1_a", MessageCount: 5, FirstReceivedAt: at, LastReceivedAt: at},
				}}, nil).Once()
			},
			wantStatus: http.StatusOK, definition: "handler.conversationsResponse",
		},
		{
			name:   "invalidCursor",
			target: "/v1/conversations/conv_v1_a/messages?page[before]=abc",
			expect: func(h *mocks.History) {
				h.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{}, domain.ErrInvalidCursor).Once()
			},
			wantStatus: http.StatusBadRequest, definition: "handler.errorResponse",
		},
		{
			name:   "invalidRequest",
			target: "/v1/conversations?idYapeAccount=",
			expect: func(h *mocks.History) {
				h.On("Conversations", mock.Anything, "").Return(domain.ConversationList{}, domain.ErrInvalidRequest).Once()
			},
			wantStatus: http.StatusBadRequest, definition: "handler.errorResponse",
		},
		{
			name:   "notFound",
			target: "/v1/conversations/conv_v1_zzz/messages",
			expect: func(h *mocks.History) {
				h.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{}, domain.ErrNotFound).Once()
			},
			wantStatus: http.StatusNotFound, definition: "handler.errorResponse",
		},
		{
			name:   "serverError",
			target: "/v1/conversations/conv_v1_a/messages",
			expect: func(h *mocks.History) {
				h.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{}, errors.New("boom")).Once()
			},
			wantStatus: http.StatusInternalServerError, definition: "handler.errorResponse",
		},
		{name: "unknownRoute", target: "/nothing", expect: func(*mocks.History) {}, wantStatus: http.StatusNotFound, definition: "handler.errorResponse"},
		{name: "healthy", target: "/health", expect: func(*mocks.History) {}, wantStatus: http.StatusOK, definition: "api.healthResponse"},
		{
			name: "unhealthy", target: "/health", expect: func(*mocks.History) {}, ping: func(context.Context) error { return errors.New("down") },
			wantStatus: http.StatusServiceUnavailable, definition: "api.healthResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := mocks.NewHistory(t)
			tt.expect(history)

			rec := get(NewServer(history, &recordingLog{}, tt.ping), tt.target)

			require.Equal(t, tt.wantStatus, rec.Code)
			var body any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			def, ok := s.Definitions[tt.definition]
			require.True(t, ok, "the spec has no definition %s", tt.definition)
			assert.Empty(t, violations(s, def, body, "$"), "the reply does not match the spec")
		})
	}
}

func TestViolationsFindsWhatTheSpecDoesNotSay(t *testing.T) {
	s := spec{Definitions: map[string]schema{
		"item": {Type: "object", Properties: map[string]schema{
			"name": {Type: "string", Enum: []any{"a", "b"}},
			"n":    {Type: "integer"},
			"ok":   {Type: "boolean"},
		}},
		"list": {Type: "object", Properties: map[string]schema{"items": {Type: "array", Items: &schema{Ref: "#/definitions/item"}}}},
	}}

	tests := []struct {
		name  string
		def   string
		json  string
		wants []string
	}{
		{name: "conforming", def: "list", json: `{"items":[{"name":"a","n":1,"ok":true}]}`},
		{name: "extraKey", def: "item", json: `{"name":"a","surprise":1}`, wants: []string{"$.surprise: not in the spec"}},
		{name: "extraKeyInsideAnArray", def: "list", json: `{"items":[{"zzz":1}]}`, wants: []string{"$.items[].zzz: not in the spec"}},
		{name: "valueOutsideTheEnum", def: "item", json: `{"name":"c"}`, wants: []string{"$.name: c is not one of the documented values"}},
		{name: "stringWhereANumberIsSaid", def: "item", json: `{"n":"x"}`, wants: []string{"$.n: a string where the spec says integer"}},
		{name: "numberWhereAStringIsSaid", def: "item", json: `{"name":3}`, wants: []string{"$.name: a number where the spec says string"}},
		{name: "booleanWhereAStringIsSaid", def: "item", json: `{"name":true}`, wants: []string{"$.name: a boolean where the spec says string"}},
		{name: "objectWhereAnArrayIsSaid", def: "list", json: `{"items":{"a":1}}`, wants: []string{"$.items: an object where the spec says array"}},
		{name: "arrayWhereAnObjectIsSaid", def: "item", json: `[1]`, wants: []string{"$: an array where the spec says object"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var value any
			require.NoError(t, json.Unmarshal([]byte(tt.json), &value))

			got := violations(s, s.Definitions[tt.def], value, "$")

			assert.Equal(t, tt.wants, got)
		})
	}
}

func TestDocumentedRoutesTurnBracesIntoEchoParameters(t *testing.T) {
	s := spec{Paths: map[string]map[string]any{
		"/v1/conversations/{conversationId}/messages": {"get": nil},
		"/health":      {"get": nil, "head": nil},
		"/a/{x}/b/{y}": {"post": nil},
	}}

	assert.Equal(t, []string{
		"GET /health", "GET /v1/conversations/:conversationId/messages", "HEAD /health", "POST /a/:x/b/:y",
	}, documentedRoutes(s))
}
