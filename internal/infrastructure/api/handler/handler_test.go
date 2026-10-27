package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service/mocks"
)

type recordingLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLog) Errorf(msg string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(msg, v...))
}

func (l *recordingLog) text() string { return strings.Join(l.lines, "\n") }

// serve sends a GET through the real router and error handler.
func serve(t *testing.T, history service.History, log Logger, target string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	e.HTTPErrorHandler = ErrorHandler(log)
	New(history, log).Register(e)

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))

	return rec
}

func TestErrorHandler(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		target     string
		wantStatus int
		wantJSON   string
	}{
		{
			name: "unknownRoute", method: http.MethodGet, target: "/v1/nothing", wantStatus: http.StatusNotFound,
			wantJSON: problemJSON(404, "", "Not Found", "There is nothing at this path."),
		},
		{
			name: "wrongMethod", method: http.MethodPost, target: "/v1/conversations", wantStatus: http.StatusMethodNotAllowed,
			wantJSON: problemJSON(405, "", "Method Not Allowed", "This path does not accept this method."),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := echo.New()
			e.HTTPErrorHandler = ErrorHandler(&recordingLog{})
			New(mocks.NewHistory(t), &recordingLog{}).Register(e)
			rec := httptest.NewRecorder()

			e.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.target, nil))

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, problemContentType, contentType(rec))
			assert.JSONEq(t, tt.wantJSON, rec.Body.String())
		})
	}
}

func TestErrorHandlerForOtherErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantJSON   string
		wantLogged bool
	}{
		{name: "badRequestFromEcho", err: echo.NewHTTPError(http.StatusBadRequest), wantStatus: http.StatusBadRequest,
			wantJSON: problemJSON(400, "invalid-request", "Invalid request", "The request is invalid.")},
		{name: "otherClientErrorUsesItsStatusPhraseAndSaysNothingIsWrongHere", err: echo.NewHTTPError(http.StatusTeapot), wantStatus: http.StatusTeapot,
			wantJSON: problemJSON(418, "", "I'm a teapot", "The request could not be processed.")},
		{name: "unsupportedMediaType", err: echo.NewHTTPError(http.StatusUnsupportedMediaType), wantStatus: http.StatusUnsupportedMediaType,
			wantJSON: problemJSON(415, "", "Unsupported Media Type", "The request could not be processed.")},
		{name: "tooManyRequests", err: echo.NewHTTPError(http.StatusTooManyRequests), wantStatus: http.StatusTooManyRequests,
			wantJSON: problemJSON(429, "", "Too Many Requests", "The request could not be processed.")},
		{name: "serverErrorFromEchoIsLogged", err: echo.NewHTTPError(http.StatusBadGateway, "upstream boom"), wantStatus: http.StatusBadGateway,
			wantJSON: problemJSON(502, "", "Bad Gateway", "Something went wrong. Try again."), wantLogged: true},
		{name: "plainError", err: fmt.Errorf("boom"), wantStatus: http.StatusInternalServerError,
			wantJSON: problemJSON(500, "", "Internal Server Error", "Something went wrong. Try again."), wantLogged: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &recordingLog{}
			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), rec)

			ErrorHandler(log)(tt.err, c)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, problemContentType, contentType(rec))
			assert.JSONEq(t, tt.wantJSON, rec.Body.String())
			assert.Equal(t, tt.wantLogged, strings.Contains(log.text(), "boom"))
		})
	}
}

func TestErrorHandlerSkipsAnAnsweredRequest(t *testing.T) {
	e := echo.New()
	rec := httptest.NewRecorder()
	c := e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), rec)
	require.NoError(t, c.String(http.StatusOK, "already answered"))

	ErrorHandler(&recordingLog{})(echo.NewHTTPError(http.StatusNotFound), c)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "already answered", rec.Body.String())
}

// problemJSON is the body of a problem details reply. typeName is the short name of one of the
// service's problem types, or "" for about:blank.
func problemJSON(status int, typeName, title, detail string) string {
	problemType := typeBlank
	if typeName != "" {
		problemType = problemTypeBase + typeName
	}
	body, err := json.Marshal(map[string]any{"type": problemType, "title": title, "status": status, "detail": detail})
	if err != nil {
		panic(err)
	}

	return string(body)
}

// contentType is the exact media type of a reply.
func contentType(rec *httptest.ResponseRecorder) string {
	return rec.Header().Get("Content-Type")
}

// wantContentType is the media type a reply with this status has: problem details for an error.
func wantContentType(status int) string {
	if status >= http.StatusBadRequest {
		return problemContentType
	}

	return "application/json"
}
