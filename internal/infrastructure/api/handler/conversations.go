package handler

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
)

// Conversations handles GET /v1/conversations?idYapeAccount=, a page of the account's
// conversations with chat messages, the most recent first, with the cursors page[before]
// and page[after] (at most one) and the page size page[size]. The body holds the
// conversations only; the way to another page is the Link header.
//
// @Summary List the conversations of an account
// @Description Returns up to 100 conversations of the account that have chat messages, the most recent first, with their message count and the times of their first and last message. Without a cursor it returns the most recent page. page[before] returns the conversations just older than the given one (less recently active), page[after] the ones just more recent. Send at most one of them, once, with a conversation _id of this account. page[size] (1 to 100, default 100) says how many. The body holds only the conversations; more pages are announced in the Link header: rel="prev" (older conversations) on the most recent and before pages, rel="next" (more recent ones) on after pages, present only when there are more. The last page has no Link header. A page past the last conversation is an empty array.
// @Tags Conversations
// @Param idYapeAccount query string true "Customer account (letters, digits, . _ : -, up to 128 characters), sent once"
// @Param page[before] query string false "Conversation _id: read the page of conversations older than it"
// @Param page[after] query string false "Conversation _id: read the page of conversations more recent than it"
// @Param page[size] query int false "How many conversations the page holds, from 1 to 100 (default 100); the Link header repeats it when it is not the default" minimum(1) maximum(100) default(100)
// @Produce json
// @Success 200 {object} conversationsResponse
// @Header 200 {string} Link "Only when more conversations exist in the direction read: rel=prev (older) on most recent and before pages, rel=next (more recent) on after pages. The target is this route with idYapeAccount and page[before] or page[after] set to the _id of the oldest or most recent conversation of the page, and page[size] repeated when it is not 100."
// @Failure 400 {object} errorResponse "invalid_request (account missing or malformed, both cursors, a parameter sent twice, a page[size] outside 1 to 100) or invalid_cursor (empty, malformed, or not a conversation of this account)"
// @Failure 500 {object} errorResponse "internal_error"
// @Router /v1/conversations [get]
func (h *Handler) Conversations(c echo.Context) error {
	account, err := queryOnce(c, "idYapeAccount")
	if err != nil {
		return h.fail(c, err)
	}
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

	var idYapeAccount string
	if account != nil {
		idYapeAccount = *account
	}

	list, err := h.history.Conversations(c.Request().Context(), service.ConversationsRequest{
		IDYapeAccount: idYapeAccount,
		Before:        before,
		After:         after,
		Size:          size,
	})
	if err != nil {
		return h.failConversations(c, err)
	}

	if link := conversationsContinuation(idYapeAccount, list); link != "" {
		c.Response().Header().Set("Link", link)
	}

	return c.JSON(http.StatusOK, toConversationsResponse(list))
}

// failConversations words an invalid cursor for conversations and leaves every other error to fail.
func (h *Handler) failConversations(c echo.Context, err error) error {
	if errors.Is(err, domain.ErrInvalidCursor) {
		return c.JSON(http.StatusBadRequest, errorBody400("invalid_cursor", "page[before] or page[after] is not the _id of a conversation of this account."))
	}

	return h.fail(c, err)
}

// conversationsContinuation is the Link header that continues the page in the direction it
// was read: rel="next" after a page read forward, rel="prev" after one read backward. The
// cursor is the oldest conversation of the page for prev and the most recent for next. It
// is empty on the last page. The page size is repeated when it is not the default, so
// following the link keeps the size the client chose. The account and the ids hold only
// letters, digits and . _ : -, which the service checked, so nothing needs escaping.
func conversationsContinuation(idYapeAccount string, list domain.ConversationList) string {
	if !list.HasMore {
		return ""
	}

	target := "/v1/conversations?idYapeAccount=" + idYapeAccount + "&page[before]=" + list.BeforeCursor
	rel := "prev"
	if list.Direction == domain.DirectionAfter {
		target = "/v1/conversations?idYapeAccount=" + idYapeAccount + "&page[after]=" + list.AfterCursor
		rel = "next"
	}
	if list.Size != domain.ConversationListSize {
		target += "&page[size]=" + itoa(int64(list.Size))
	}

	return "<" + target + `>; rel="` + rel + `"`
}

func toConversationsResponse(list domain.ConversationList) conversationsResponse {
	conversations := make([]conversationResponse, 0, len(list.Conversations))
	for _, conv := range list.Conversations {
		conversations = append(conversations, conversationResponse{
			ID:            conv.ConversationID,
			MessageCount:  conv.MessageCount,
			FirstReceived: unixSeconds(conv.FirstOccurredAt),
			LastReceived:  unixSeconds(conv.LastOccurredAt),
		})
	}

	return conversationsResponse{Conversations: conversations}
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
