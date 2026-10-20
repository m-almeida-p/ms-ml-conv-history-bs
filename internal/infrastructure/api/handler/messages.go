package handler

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
)

// Messages handles GET /v1/conversations/{conversationId}/messages, with the
// cursors page[before] and page[after] (at most one).
//
// @Summary List the messages of a conversation
// @Description Returns up to 100 chat messages, oldest first. Without a cursor it returns the newest page. page[before] returns the 100 messages just older than the given message, page[after] the 100 just newer. Send at most one of them, once, with a message _id of this conversation. hasMore and the link follow the direction of travel: links.prev for the newest and before pages when older messages exist, links.next for after pages when newer ones exist. A page past the last message is an empty array.
// @Tags Messages
// @Param conversationId path string true "Conversation id (letters, digits, . _ : -, up to 128 characters)"
// @Param page[before] query string false "Message _id: read the page of messages older than it"
// @Param page[after] query string false "Message _id: read the page of messages newer than it"
// @Produce json
// @Success 200 {object} messagesResponse
// @Failure 400 {object} errorResponse "invalid_request (malformed id, both cursors, a cursor sent twice) or invalid_cursor (empty, not a plain decimal _id, or not a message of this conversation)"
// @Failure 404 {object} errorResponse "not_found: the conversation has no chat messages"
// @Failure 500 {object} errorResponse "internal_error"
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

	page, err := h.history.Messages(c.Request().Context(), service.MessagesRequest{
		ConversationID: c.Param("conversationId"),
		Before:         before,
		After:          after,
	})
	if err != nil {
		return h.fail(c, err)
	}

	return c.JSON(http.StatusOK, toMessagesResponse(page))
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
			Received: unixSeconds(m.ReceivedAt),
			Source:   sourceResponse{Type: "api"},
			Metadata: messageMetadata{Sequence: m.Sequence, SourceEvent: m.SourceEvent, Truncated: m.Truncated},
		})
	}

	response := messagesResponse{
		Conversation: conversationRef{ID: page.ConversationID},
		Messages:     messages,
		Meta:         messagesMeta{HasMore: page.HasMore, BeforeCursor: page.BeforeCursor, AfterCursor: page.AfterCursor},
	}

	if page.HasMore {
		base := "/v1/conversations/" + page.ConversationID + "/messages"
		if page.Direction == domain.DirectionAfter {
			response.Links.Next = base + "?page[after]=" + page.AfterCursor
		} else {
			response.Links.Prev = base + "?page[before]=" + page.BeforeCursor
		}
	}

	return response
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
		return nil, fmt.Errorf("%w: %s is given more than once", domain.ErrInvalidRequest, key)
	}

	return &values[0], nil
}
