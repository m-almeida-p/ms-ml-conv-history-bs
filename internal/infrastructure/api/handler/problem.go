package handler

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// Errors are answered as RFC 9457 problem details: a JSON object served as
// application/problem+json. A client tells problems apart by `type`, never by `detail`.
//
// A problem type has a stable URI, the primary identifier of the problem. The types are tag URIs
// (RFC 4151), absolute and valid without any website; RFC 9457 allows a type that cannot be
// dereferenced. What each type means, and the status it goes with, is in docs/design.md. A
// problem that means nothing beyond its HTTP status uses the default type, about:blank, and its
// title is the status phrase.
const (
	problemContentType = "application/problem+json"

	problemTypeBase = "tag:yape.com.pe,2026:conversation-history:"

	typeBlank                = "about:blank"
	typeInvalidRequest       = problemTypeBase + "invalid-request"
	typeInvalidCursor        = problemTypeBase + "invalid-cursor"
	typeConversationNotFound = problemTypeBase + "conversation-not-found"
)

// The details of the problems that have one fixed wording. None repeats what the client sent:
// they say what is wrong, not with which value.
const (
	detailMessagesCursor      = "page[before] or page[after] is not the _id of a message in this conversation."
	detailConversationsCursor = "page[before] or page[after] is not a cursor from the conversation list."
	detailNoMessages          = "No chat messages were found for this conversation."
	detailNothingHere         = "There is nothing at this path."
	detailMethodNotAllowed    = "This path does not accept this method."
	detailInvalidRequest      = "The request is invalid."
	detailClientError         = "The request could not be processed."
	detailTimeout             = "The request took too long. Try again."
	detailServerError         = "Something went wrong. Try again."
)

// writeProblem answers with a problem details object. The status in the body is the status of
// the response, as the RFC requires.
func writeProblem(c echo.Context, status int, problemType, title, detail string) error {
	c.Response().Header().Set(echo.HeaderContentType, problemContentType)

	return c.JSON(status, problemResponse{Type: problemType, Title: title, Status: status, Detail: detail})
}

// writeBlankProblem answers with a problem that has no meaning beyond its status: type
// about:blank, titled with the status phrase.
func writeBlankProblem(c echo.Context, status int, detail string) error {
	title := http.StatusText(status)
	if title == "" {
		title = "Error"
	}

	return writeProblem(c, status, typeBlank, title, detail)
}
