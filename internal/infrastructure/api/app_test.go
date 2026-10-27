package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service/mocks"
)

type recordingLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLog) add(level, msg string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, level+" "+fmt.Sprintf(msg, v...))
}

func (l *recordingLog) Infof(msg string, v ...any)  { l.add("info", msg, v...) }
func (l *recordingLog) Errorf(msg string, v ...any) { l.add("error", msg, v...) }
func (l *recordingLog) text() string                { return strings.Join(l.lines, "\n") }

func get(e *echo.Echo, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))

	return rec
}

func TestHealth(t *testing.T) {
	tests := []struct {
		name       string
		ping       Pinger
		wantStatus int
		wantBody   string
	}{
		{name: "databaseAnswers", ping: func(context.Context) error { return nil }, wantStatus: http.StatusOK, wantBody: `{"status":"ok"}`},
		{name: "noDatabaseCheck", ping: nil, wantStatus: http.StatusOK, wantBody: `{"status":"ok"}`},
		{name: "databaseDown", ping: func(context.Context) error { return errors.New("connection refused") }, wantStatus: http.StatusServiceUnavailable, wantBody: `{"status":"unavailable"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewServer(mocks.NewHistory(t), &recordingLog{}, tt.ping)

			rec := get(e, "/health")

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.JSONEq(t, tt.wantBody, rec.Body.String())
		})
	}
}

func TestServerRoutes(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		expect     func(h *mocks.History)
		wantStatus int
	}{
		{
			name:   "messagesRoute",
			target: "/v1/conversations/conv_v1_a/messages",
			expect: func(h *mocks.History) {
				h.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{ConversationID: "conv_v1_a"}, nil).Once()
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "conversationsRoute",
			target: "/v1/conversations?idYapeAccount=acc-1",
			expect: func(h *mocks.History) {
				h.On("Conversations", mock.Anything, service.ConversationsRequest{IDYapeAccount: "acc-1"}).Return(domain.ConversationList{}, nil).Once()
			},
			wantStatus: http.StatusOK,
		},
		{name: "unknownRoute", target: "/v2/anything", expect: func(*mocks.History) {}, wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := mocks.NewHistory(t)
			tt.expect(history)

			rec := get(NewServer(history, &recordingLog{}, nil), tt.target)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRequestLogHasTheRouteNotTheValues(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		wantLine   string
		wantStatus int
	}{
		{name: "routePatternWithoutTheQuery", target: "/v1/conversations?idYapeAccount=acc-secret-1", wantLine: "info GET /v1/conversations 200", wantStatus: http.StatusOK},
		{name: "pathParameterIsNotLogged", target: "/v1/conversations/conv_secret/messages?page[before]=555", wantLine: "info GET /v1/conversations/:conversationId/messages 200", wantStatus: http.StatusOK},
		{name: "unmatchedRoute", target: "/nothing/acc-secret-1", wantLine: "info GET unmatched 404", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			history := mocks.NewHistory(t)
			history.On("Conversations", mock.Anything, mock.Anything).Return(domain.ConversationList{}, nil).Maybe()
			history.On("Messages", mock.Anything, mock.Anything).Return(domain.MessagePage{}, nil).Maybe()
			log := &recordingLog{}

			rec := get(NewServer(history, log, nil), tt.target)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, log.text(), tt.wantLine)
			for _, secret := range []string{"acc-secret-1", "conv_secret", "555"} {
				assert.NotContains(t, log.text(), secret)
			}
		})
	}
}

func TestPanicIsAnsweredAndLogged(t *testing.T) {
	log := &recordingLog{}
	e := NewServer(mocks.NewHistory(t), log, nil)
	e.GET("/boom", func(echo.Context) error { panic("something broke") })

	rec := get(e, "/boom")

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), `"title":"Internal Server Error"`)
	assert.NotContains(t, rec.Body.String(), "something broke")
	assert.Contains(t, log.text(), "error GET /boom failed: something broke")
	assert.Contains(t, log.text(), "info GET /boom 500")
}

func TestARequestThatRunsPastItsDeadlineIsCancelledAndAnswered503(t *testing.T) {
	history := mocks.NewHistory(t)
	history.On("Messages", mock.Anything, mock.Anything).Return(
		func(ctx context.Context, _ service.MessagesRequest) (domain.MessagePage, error) {
			<-ctx.Done() // a slow query: it ends only when the server cancels it

			return domain.MessagePage{}, ctx.Err()
		}).Once()
	log := &recordingLog{}
	e := newServer(history, log, nil, 30*time.Millisecond)

	started := time.Now()
	rec := get(e, "/v1/conversations/conv_v1_a/messages")

	assert.Less(t, time.Since(started), 2*time.Second, "the request did not wait for the query to finish on its own")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "application/problem+json", rec.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"type":"about:blank","title":"Service Unavailable","status":503,"detail":"The request took too long. Try again."}`, rec.Body.String())
	assert.Contains(t, log.text(), "deadline exceeded")
}

func TestARequestInsideItsDeadlineIsUntouched(t *testing.T) {
	history := mocks.NewHistory(t)
	history.On("Conversations", mock.Anything, service.ConversationsRequest{IDYapeAccount: "acc-1"}).Return(domain.ConversationList{Conversations: []domain.ConversationSummary{}}, nil).Once()
	e := newServer(history, &recordingLog{}, nil, time.Second)

	rec := get(e, "/v1/conversations?idYapeAccount=acc-1")

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestTheRequestDeadlineIsLongEnoughForARealQueryAndShortEnoughToBound(t *testing.T) {
	assert.GreaterOrEqual(t, requestTimeout, 5*time.Second, "the heaviest account measured takes milliseconds, so this is generous")
	assert.LessOrEqual(t, requestTimeout, time.Minute, "a request must not hold a connection for long")
}
