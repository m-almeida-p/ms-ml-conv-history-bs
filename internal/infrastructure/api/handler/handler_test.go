package handler

import (
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
			wantJSON: `{"error":{"code":"not_found","description":"There is nothing at this path."}}`,
		},
		{
			name: "wrongMethod", method: http.MethodPost, target: "/v1/conversations", wantStatus: http.StatusMethodNotAllowed,
			wantJSON: `{"error":{"code":"method_not_allowed","description":"This path does not accept this method."}}`,
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
			assert.JSONEq(t, tt.wantJSON, rec.Body.String())
		})
	}
}

func TestErrorHandlerForOtherErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
		wantLogged bool
	}{
		{name: "badRequestFromEcho", err: echo.NewHTTPError(http.StatusBadRequest), wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "otherHTTPError", err: echo.NewHTTPError(http.StatusTeapot), wantStatus: http.StatusTeapot, wantCode: "internal_error"},
		{name: "plainError", err: fmt.Errorf("boom"), wantStatus: http.StatusInternalServerError, wantCode: "internal_error", wantLogged: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &recordingLog{}
			e := echo.New()
			rec := httptest.NewRecorder()
			c := e.NewContext(httptest.NewRequest(http.MethodGet, "/", nil), rec)

			ErrorHandler(log)(tt.err, c)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), `"code":"`+tt.wantCode+`"`)
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
