// Package api builds the HTTP server of the history service.
package api

import (
	"context"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/api/handler"
)

// healthTimeout bounds the database check of /health.
const healthTimeout = 2 * time.Second

// requestTimeout bounds the work of one request. The conversation list groups an account's whole
// history, so without a bound a heavy account could hold a connection for as long as the query runs.
// A request that runs past it is answered 503 and its query is cancelled.
const requestTimeout = 10 * time.Second

// Logger is what the server reports to.
type Logger interface {
	Infof(msg string, v ...any)
	Errorf(msg string, v ...any)
}

// healthResponse is the body of /health.
type healthResponse struct {
	Status string `json:"status" enums:"ok,unavailable" example:"ok"`
}

// Pinger checks that the database answers.
type Pinger func(ctx context.Context) error

// NewServer returns the router: the two history routes and /health. It has no
// authentication (PoC only).
func NewServer(history service.History, log Logger, ping Pinger) *echo.Echo {
	return newServer(history, log, ping, requestTimeout)
}

// newServer is NewServer with the request timeout given, so a test can use a short one.
func newServer(history service.History, log Logger, ping Pinger, timeout time.Duration) *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.HTTPErrorHandler = handler.ErrorHandler(log)

	// The request log is outermost, so it sees the status of a recovered panic too.
	e.Use(requestLog(log))
	e.Use(middleware.RecoverWithConfig(middleware.RecoverConfig{
		// The error handler logs the cause; this hook only keeps Echo from printing
		// the stack to its own logger.
		LogErrorFunc: func(_ echo.Context, err error, _ []byte) error { return err },
	}))

	e.Use(middleware.ContextTimeoutWithConfig(middleware.ContextTimeoutConfig{Timeout: timeout}))

	handler.New(history, log).Register(e)
	e.GET("/health", health(ping))

	return e
}

// requestLog logs one line per request: method, route pattern, status and time.
// It logs the route and not the URL, because the query holds an account id.
func requestLog(log Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()

			err := next(c)
			if err != nil {
				// Run the error handler now so the status is known.
				c.Error(err)
				err = nil
			}

			log.Infof("%s %s %d %s", c.Request().Method, routeOf(c), c.Response().Status, time.Since(start).Round(time.Millisecond))

			return err
		}
	}
}

func routeOf(c echo.Context) string {
	if path := c.Path(); path != "" {
		return path
	}

	return "unmatched"
}

// health answers GET /health.
//
// @Summary Health check
// @Description Returns 200 when the database answers and 503 when it does not.
// @Tags Health
// @Produce json
// @Success 200 {object} healthResponse
// @Failure 503 {object} healthResponse
// @Router /health [get]
func health(ping Pinger) echo.HandlerFunc {
	return func(c echo.Context) error {
		if ping != nil {
			ctx, cancel := context.WithTimeout(c.Request().Context(), healthTimeout)
			defer cancel()

			if err := ping(ctx); err != nil {
				return c.JSON(http.StatusServiceUnavailable, healthResponse{Status: "unavailable"})
			}
		}

		return c.JSON(http.StatusOK, healthResponse{Status: "ok"})
	}
}
