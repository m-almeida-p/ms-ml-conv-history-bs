package handler

import (
	"context"
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
)

// Logger is what the handlers report to. They log the route and the error class,
// never message text, cursors or account ids.
type Logger interface {
	Errorf(msg string, v ...any)
}

// Handler serves the history routes.
type Handler struct {
	history service.History
	log     Logger
}

// New returns the handlers over a history service.
func New(history service.History, log Logger) *Handler {
	return &Handler{history: history, log: log}
}

// Register adds the routes under /v1.
func (h *Handler) Register(e *echo.Echo) {
	v1 := e.Group("/v1")
	v1.GET("/conversations", h.Conversations)
	v1.GET("/conversations/:conversationId/messages", h.Messages)
}

// fail answers an error with the status and problem its kind calls for.
func (h *Handler) fail(c echo.Context, err error) error {
	return h.failWithCursor(c, err, detailMessagesCursor)
}

// failWithCursor is fail with the wording of an invalid cursor for what the route pages.
func (h *Handler) failWithCursor(c echo.Context, err error, cursorDetail string) error {
	switch {
	case errors.Is(err, domain.ErrInvalidCursor):
		return writeProblem(c, http.StatusBadRequest, typeInvalidCursor, "Invalid cursor", cursorDetail)
	case errors.Is(err, domain.ErrInvalidRequest):
		return writeProblem(c, http.StatusBadRequest, typeInvalidRequest, "Invalid request", invalidRequestDetail(err))
	case errors.Is(err, domain.ErrNotFound):
		return writeProblem(c, http.StatusNotFound, typeConversationNotFound, "Conversation not found", detailNoMessages)
	case errors.Is(err, context.DeadlineExceeded):
		// The request ran past its deadline: the server is busy, and trying again may work.
		h.log.Errorf("%s %s failed: %v", c.Request().Method, c.Path(), err)

		return writeBlankProblem(c, http.StatusServiceUnavailable, detailTimeout)
	default:
		h.log.Errorf("%s %s failed: %v", c.Request().Method, c.Path(), err)

		return writeBlankProblem(c, http.StatusInternalServerError, detailServerError)
	}
}

// invalidRequestDetail is what to tell the client about an invalid request. An
// *domain.InvalidRequestError carries words that name no value, found through any context wrapped
// around it. Any other error that matches ErrInvalidRequest carries none, and its text is not
// written for the client, so the client gets a fixed sentence.
func invalidRequestDetail(err error) string {
	var invalid *domain.InvalidRequestError
	if errors.As(err, &invalid) {
		return invalid.Detail
	}

	return detailInvalidRequest
}

// ErrorHandler answers the errors Echo raises itself (unknown route, wrong method,
// panics) as problem details too, like the handlers.
func ErrorHandler(log Logger) echo.HTTPErrorHandler {
	return func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}

		var httpErr *echo.HTTPError
		logged := false
		if !errors.As(err, &httpErr) {
			log.Errorf("%s %s failed: %v", c.Request().Method, c.Path(), err)
			httpErr, logged = echo.NewHTTPError(http.StatusInternalServerError), true
		}

		switch {
		case httpErr.Code == http.StatusNotFound:
			_ = writeBlankProblem(c, httpErr.Code, detailNothingHere)
		case httpErr.Code == http.StatusMethodNotAllowed:
			_ = writeBlankProblem(c, httpErr.Code, detailMethodNotAllowed)
		case httpErr.Code == http.StatusBadRequest:
			_ = writeProblem(c, httpErr.Code, typeInvalidRequest, "Invalid request", detailInvalidRequest)
		case httpErr.Code >= http.StatusInternalServerError:
			if !logged {
				log.Errorf("%s %s failed: %v", c.Request().Method, c.Path(), httpErr)
			}
			_ = writeBlankProblem(c, httpErr.Code, detailServerError)
		default:
			// Any other client error: the status says what it is, and nothing is wrong on our side.
			_ = writeBlankProblem(c, httpErr.Code, detailClientError)
		}
	}
}
