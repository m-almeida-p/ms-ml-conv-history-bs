package handler

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/domain"
)

// Conversations handles GET /v1/conversations?idYapeAccount=, the account's
// conversations with chat messages, the most recent first.
//
// @Summary List the conversations of an account
// @Description Returns the account's latest 100 conversations that have chat messages, the most recent first, with their message count and the times of their first and last message. There is no paging: hasMore says there are more than 100.
// @Tags Conversations
// @Param idYapeAccount query string true "Customer account (letters, digits, . _ : -, up to 128 characters), sent once"
// @Produce json
// @Success 200 {object} conversationsResponse
// @Failure 400 {object} errorResponse "invalid_request: the account is missing, malformed or sent twice"
// @Failure 500 {object} errorResponse "internal_error"
// @Router /v1/conversations [get]
func (h *Handler) Conversations(c echo.Context) error {
	account, err := queryOnce(c, "idYapeAccount")
	if err != nil {
		return h.fail(c, err)
	}

	var idYapeAccount string
	if account != nil {
		idYapeAccount = *account
	}

	list, err := h.history.Conversations(c.Request().Context(), idYapeAccount)
	if err != nil {
		return h.fail(c, err)
	}

	return c.JSON(http.StatusOK, toConversationsResponse(list))
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

	return conversationsResponse{Conversations: conversations, Meta: conversationsMeta{HasMore: list.HasMore}}
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
