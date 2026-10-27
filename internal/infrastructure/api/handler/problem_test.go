package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service/mocks"
)

// The tests below pin the rules of RFC 9457 for every kind of error the API answers, whichever
// route produces it.

// problemCases returns one reply for each kind of error: a handler error of each class, and the
// errors Echo raises itself (an unknown route, a method the route does not accept).
func problemCases(t *testing.T) map[string]*httptest.ResponseRecorder {
	t.Helper()

	reply := func(err error, target string) *httptest.ResponseRecorder {
		history := mocks.NewHistory(t)
		history.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{}, err).Maybe()
		history.On("Conversations", mock.Anything, mock.Anything).Return(domain.ConversationList{}, err).Maybe()

		return serve(t, history, &recordingLog{}, target)
	}
	messages := "/v1/conversations/conv_v1_a/messages"
	list := "/v1/conversations?idYapeAccount=acc-1"

	// A method the route does not accept, which Echo answers itself.
	wrongMethod := func() *httptest.ResponseRecorder {
		e := echo.New()
		e.HTTPErrorHandler = ErrorHandler(&recordingLog{})
		New(mocks.NewHistory(t), &recordingLog{}).Register(e)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, list, nil))

		return rec
	}

	return map[string]*httptest.ResponseRecorder{
		"invalidRequest":        reply(domain.InvalidRequest("page[size] must be a whole number from 1 to 100"), messages),
		"invalidCursorMessages": reply(domain.ErrInvalidCursor, messages),
		"invalidCursorList":     reply(domain.ErrInvalidCursor, list),
		"conversationNotFound":  reply(domain.ErrNotFound, messages),
		"serverError":           reply(errors.New("connection reset: password=hunter2"), messages),
		"repeatedParameter":     reply(nil, messages+"?page[size]=1&page[size]=2"),
		"unknownRoute":          reply(nil, "/v1/nothing"),
		"wrongMethod":           wrongMethod(),
	}
}

func decodeProblem(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

	return body
}

func TestEveryErrorIsServedAsProblemJSON(t *testing.T) {
	for name, rec := range problemCases(t) {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"), "the exact media type, with no parameter")
		})
	}
}

func TestTheStatusInTheBodyIsTheStatusOfTheResponse(t *testing.T) {
	for name, rec := range problemCases(t) {
		t.Run(name, func(t *testing.T) {
			body := decodeProblem(t, rec)

			assert.InDelta(t, float64(rec.Code), body["status"], 0, "RFC 9457: the same status code as the HTTP response")
			assert.GreaterOrEqual(t, rec.Code, 400)
		})
	}
}

func TestAProblemHasTheFourMembersAndNothingElse(t *testing.T) {
	for name, rec := range problemCases(t) {
		t.Run(name, func(t *testing.T) {
			body := decodeProblem(t, rec)

			keys := make([]string, 0, len(body))
			for k := range body {
				keys = append(keys, k)
			}
			assert.ElementsMatch(t, []string{"type", "title", "status", "detail"}, keys)
			for _, k := range []string{"type", "title", "detail"} {
				assert.IsType(t, "", body[k], k)
				assert.NotEmpty(t, body[k], k)
			}
		})
	}
}

func TestTheTypeIsAnAbsoluteURIAndAboutBlankMeansTheStatusAlone(t *testing.T) {
	for name, rec := range problemCases(t) {
		t.Run(name, func(t *testing.T) {
			body := decodeProblem(t, rec)
			problemType := body["type"].(string)

			if problemType == "about:blank" {
				assert.Equal(t, http.StatusText(rec.Code), body["title"], "RFC 9457: with about:blank the title is the HTTP status phrase")
				return
			}
			assert.True(t, strings.HasPrefix(problemType, "tag:yape.com.pe,2026:conversation-history:"), problemType)
		})
	}
}

func TestEachKindOfErrorHasItsOwnTypeAndAStableTitle(t *testing.T) {
	cases := problemCases(t)

	for name, want := range map[string][2]string{
		"invalidRequest":        {typeInvalidRequest, "Invalid request"},
		"invalidCursorMessages": {typeInvalidCursor, "Invalid cursor"},
		"invalidCursorList":     {typeInvalidCursor, "Invalid cursor"},
		"conversationNotFound":  {typeConversationNotFound, "Conversation not found"},
		"serverError":           {typeBlank, "Internal Server Error"},
		"repeatedParameter":     {typeInvalidRequest, "Invalid request"},
		"unknownRoute":          {typeBlank, "Not Found"},
		"wrongMethod":           {typeBlank, "Method Not Allowed"},
	} {
		body := decodeProblem(t, cases[name])

		assert.Equal(t, want[0], body["type"], name)
		assert.Equal(t, want[1], body["title"], name+": the title does not change from one occurrence to the next")
	}
}

func TestTheSameTypeKeepsOneTitleWhateverTheDetail(t *testing.T) {
	titles := map[string]string{}
	for name, rec := range problemCases(t) {
		body := decodeProblem(t, rec)
		problemType := body["type"].(string)
		if problemType == typeBlank {
			continue
		}
		if title, seen := titles[problemType]; seen {
			assert.Equal(t, title, body["title"], name)
		}
		titles[problemType] = body["title"].(string)
	}
}

func TestAnErrorNeverRepeatsWhatTheClientSent(t *testing.T) {
	const secret = "SECRET-VALUE-123"
	history := mocks.NewHistory(t)
	history.On("Messages", mock.Anything, mock.Anything).
		Return(domain.MessagePage{}, domain.InvalidRequest("the conversation id is malformed")).Maybe()
	history.On("Conversations", mock.Anything, mock.Anything).Return(domain.ConversationList{}, domain.ErrInvalidCursor).Maybe()

	for _, target := range []string{
		"/v1/conversations/" + secret + "/messages",
		"/v1/conversations/conv_v1_a/messages?page[before]=" + secret,
		"/v1/conversations/conv_v1_a/messages?page[size]=" + secret,
		"/v1/conversations?idYapeAccount=" + secret + "&page[after]=" + secret,
		"/v1/" + secret,
	} {
		rec := serve(t, history, &recordingLog{}, target)

		assert.NotContains(t, rec.Body.String(), secret, target)
		assert.NotContains(t, rec.Header().Get("Location"), secret, target)
	}
}

func TestAServerErrorHidesItsCause(t *testing.T) {
	rec := problemCases(t)["serverError"]

	assert.NotContains(t, rec.Body.String(), "hunter2")
	assert.NotContains(t, rec.Body.String(), "connection reset")
}

func TestABlankProblemFallsBackWhenTheStatusHasNoPhrase(t *testing.T) {
	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), rec)

	require.NoError(t, writeBlankProblem(c, 599, "x"))

	assert.Contains(t, rec.Body.String(), `"title":"Error"`)
	assert.Contains(t, rec.Body.String(), `"status":599`)
}

func TestTheDetailOfAnInvalidRequestComesFromTheTypedErrorWhateverIsWrappedAroundIt(t *testing.T) {
	for name, tc := range map[string]struct {
		err        error
		wantDetail string
	}{
		"typed":                {domain.InvalidRequest("page[size] must be a whole number from 1 to 100"), "page[size] must be a whole number from 1 to 100"},
		"typedWithContext":     {fmt.Errorf("list messages: %w", domain.InvalidRequest("both cursors")), "both cursors"},
		"typedWithTwoContexts": {fmt.Errorf("a: %w", fmt.Errorf("b: %w", domain.InvalidRequest("the account is malformed"))), "the account is malformed"},
		"theBareSentinel":      {domain.ErrInvalidRequest, "The request is invalid."},
		"theSentinelWithWords": {fmt.Errorf("%w: internal words that are not for the client", domain.ErrInvalidRequest), "The request is invalid."},
		"theSentinelInContext": {fmt.Errorf("repository: %w", domain.ErrInvalidRequest), "The request is invalid."},
	} {
		t.Run(name, func(t *testing.T) {
			history := mocks.NewHistory(t)
			history.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{}, tc.err).Once()

			rec := serve(t, history, &recordingLog{}, "/v1/conversations/conv_v1_a/messages")

			require.Equal(t, http.StatusBadRequest, rec.Code)
			body := decodeProblem(t, rec)
			assert.Equal(t, typeInvalidRequest, body["type"])
			assert.Equal(t, tc.wantDetail, body["detail"])
			assert.NotContains(t, rec.Body.String(), "list messages", "context added around an error is not for the client")
			assert.NotContains(t, rec.Body.String(), "repository", "context added around an error is not for the client")
			assert.NotContains(t, rec.Body.String(), "internal words", "an error without a typed detail is not shown")
		})
	}
}
