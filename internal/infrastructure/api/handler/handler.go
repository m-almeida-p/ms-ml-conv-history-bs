package handler

import (
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

// fail answers an error with the status and body its kind calls for.
func (h *Handler) fail(c echo.Context, err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalidCursor):
		return c.JSON(http.StatusBadRequest, errorBody400("invalid_cursor", "page[before] or page[after] is not the _id of a message in this conversation."))
	case errors.Is(err, domain.ErrInvalidRequest):
		// The service words these without the input, so the text is safe to return.
		return c.JSON(http.StatusBadRequest, errorBody400("invalid_request", err.Error()))
	case errors.Is(err, domain.ErrNotFound):
		return c.JSON(http.StatusNotFound, errorResponse{Error: errorBody{Code: "not_found", Description: "No chat messages were found for this conversation."}})
	default:
		h.log.Errorf("%s %s failed: %v", c.Request().Method, c.Path(), err)

		return c.JSON(http.StatusInternalServerError, errorResponse{Error: errorBody{Code: "internal_error", Description: "Something went wrong. Try again."}})
	}
}

func errorBody400(code, description string) errorResponse {
	return errorResponse{Error: errorBody{Code: code, Description: description}}
}

// ErrorHandler answers the errors Echo raises itself (unknown route, wrong method,
// panics) in the same body as the handlers.
func ErrorHandler(log Logger) echo.HTTPErrorHandler {
	return func(err error, c echo.Context) {
		if c.Response().Committed {
			return
		}

		var httpErr *echo.HTTPError
		if !errors.As(err, &httpErr) {
			log.Errorf("%s %s failed: %v", c.Request().Method, c.Path(), err)
			httpErr = echo.NewHTTPError(http.StatusInternalServerError)
		}

		code, description := "internal_error", "Something went wrong. Try again."
		switch httpErr.Code {
		case http.StatusNotFound:
			code, description = "not_found", "There is nothing at this path."
		case http.StatusMethodNotAllowed:
			code, description = "method_not_allowed", "This path does not accept this method."
		case http.StatusBadRequest:
			code, description = "invalid_request", "The request is malformed."
		}

		_ = c.JSON(httpErr.Code, errorResponse{Error: errorBody{Code: code, Description: description}})
	}
}
