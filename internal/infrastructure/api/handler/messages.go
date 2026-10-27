package handler

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
)

// Messages handles GET /v1/conversations/{conversationId}/messages, with the
// cursors page[before] and page[after] (at most one) and the page size page[size].
// The body holds the messages only; the way to the next page is the Link header.
//
// @Summary List the messages of a conversation
// @Description Returns up to 100 chat messages, oldest first. Without a cursor it returns the newest page. page[before] returns the messages just older than the given message, page[after] the ones just newer. Send at most one of them, once, with a message _id of this conversation. page[size] (1 to 100, default 100) says how many. The body holds only the conversation and its messages; more pages are announced in the Link header: rel="prev" (older messages) on the newest and before pages, rel="next" (newer messages) on after pages, present only when there are more. The last page has no Link header. A page past the last message is an empty array.
// @Tags Messages
// @Param conversationId path string true "Conversation id (letters, digits, . _ : -, up to 128 characters)"
// @Param page[before] query string false "Message _id: read the page of messages older than it"
// @Param page[after] query string false "Message _id: read the page of messages newer than it"
// @Param page[size] query int false "How many messages the page holds, from 1 to 100 (default 100); the Link header repeats it when it is not the default" minimum(1) maximum(100) default(100)
// @Produce json
// @Produce application/problem+json
// @Success 200 {object} messagesResponse
// @Header 200 {string} Link "Only when more messages exist in the direction read: rel=prev (older messages) on newest and before pages, rel=next (newer messages) on after pages. The target is this route with page[before] or page[after] set to the _id of the first or last message, and page[size] repeated when it is not 100."
// @Failure 400 {object} problemResponse "Problem details (RFC 9457). Type invalid-request: malformed id, both cursors, a parameter sent twice, a page[size] outside 1 to 100. Type invalid-cursor: empty, not a plain decimal _id, or not a message of this conversation"
// @Failure 404 {object} problemResponse "Problem details (RFC 9457), type conversation-not-found: the conversation has no chat messages"
// @Failure 500 {object} problemResponse "Problem details (RFC 9457), type about:blank"
// @Router /v1/conversations/{conversationId}/messages [get]
func (h *Handler) Messages(c echo.Context) error {
	before, err := queryOnce(c, "page[before]")
	if err != nil {
		return h.fail(c, err)
	}
	after, err := queryOnce(c, "page[after]")
	if err != nil {
		return h.fail(c, err)
	}

	size, err := queryOnce(c, "page[size]")
	if err != nil {
		return h.fail(c, err)
	}

	page, err := h.history.Messages(c.Request().Context(), service.MessagesRequest{
		ConversationID: c.Param("conversationId"),
		Before:         before,
		After:          after,
		Size:           size,
	})
	if err != nil {
		return h.fail(c, err)
	}

	if link := continuation(page); link != "" {
		c.Response().Header().Set("Link", link)
	}

	return c.JSON(http.StatusOK, toMessagesResponse(page))
}

// continuation is the Link header that continues the page in the direction it was
// read: rel="next" after a page read forward, rel="prev" after one read backward.
// It is empty on the last page. The page size is repeated when it is not the default,
// so following the link keeps the size the client chose.
func continuation(page domain.MessagePage) string {
	if !page.HasMore {
		return ""
	}

	target := "/v1/conversations/" + page.ConversationID + "/messages?page[before]=" + page.BeforeCursor
	rel := "prev"
	if page.Direction == domain.DirectionAfter {
		target = "/v1/conversations/" + page.ConversationID + "/messages?page[after]=" + page.AfterCursor
		rel = "next"
	}
	if page.Size != domain.PageSize {
		target += "&page[size]=" + itoa(int64(page.Size))
	}

	return "<" + target + `>; rel="` + rel + `"`
}

func toMessagesResponse(page domain.MessagePage) messagesResponse {
	messages := make([]messageResponse, 0, len(page.Messages))
	for _, m := range page.Messages {
		messages = append(messages, messageResponse{
			ID:       itoa(int64(m.Sequence)),
			Type:     m.Type,
			Role:     string(m.Role),
			AuthorID: m.AuthorID,
			Text:     m.Text,
			Received: unixSeconds(m.OccurredAt),
			Source:   sourceResponse{Type: "api"},
			Metadata: messageMetadata{Sequence: m.Sequence, EventName: m.EventName, Truncated: m.Truncated, Flow: m.Flow},
		})
	}

	return messagesResponse{Conversation: conversationRef{ID: page.ConversationID}, Messages: messages}
}

// queryOnce returns the one value of a query parameter, nil when it is absent. A
// parameter that is present but empty comes back as an empty string, which the
// service rejects as a cursor, and one sent twice is ambiguous, so it is refused.
func queryOnce(c echo.Context, key string) (*string, error) {
	values, present := c.QueryParams()[key]
	if !present {
		return nil, nil
	}
	if len(values) > 1 {
		return nil, domain.InvalidRequest("%s is given more than once", key)
	}

	return &values[0], nil
}
