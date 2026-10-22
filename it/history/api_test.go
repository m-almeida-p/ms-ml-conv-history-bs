package history_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// freePort asks the system for a port nobody holds.
func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

type apiMessages struct {
	Conversation struct {
		ID string `json:"_id"`
	} `json:"conversation"`
	Messages []struct {
		ID       string  `json:"_id"`
		Role     string  `json:"role"`
		AuthorID string  `json:"authorId"`
		Text     string  `json:"text"`
		Received float64 `json:"received"`
		Metadata struct {
			Sequence int `json:"sequence"`
		} `json:"metadata"`
	} `json:"messages"`
	Meta struct {
		HasMore      bool   `json:"hasMore"`
		BeforeCursor string `json:"beforeCursor"`
		AfterCursor  string `json:"afterCursor"`
	} `json:"meta"`
	Links struct {
		Prev string `json:"prev"`
		Next string `json:"next"`
	} `json:"links"`
}

// TestAPI runs the real cmd/api binary on a database with a long conversation
// and reads it over HTTP, following the links the API gives.
func TestAPI(t *testing.T) {
	pool, dsn := newHistoryDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
		SELECT 'conv_big', s, CASE WHEN s % 2 = 1 THEN 'appUser' ELSE 'appMaker' END,
		       CASE WHEN s % 2 = 1 THEN 'acc-api' ELSE 'ms-ml-genai-agent-bs' END, 'message ' || s, 'UserMessageSent', 'acc-api',
		       '2026-10-20T10:00:00Z'::timestamptz + s * interval '1 second'
		FROM generate_series(1, 230) s`)
	require.NoError(t, err)

	port := freePort(t)
	proc := startBinary(t, buildBinary(t, "api"), []string{"POSTGRES_DSN=" + dsn, fmt.Sprintf("SERVER_PORT=%d", port)})
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	client := http.Client{Timeout: 10 * time.Second}
	get := func(path string) (int, []byte) {
		t.Helper()

		resp, err := client.Get(base + path)
		require.NoError(t, err)
		defer resp.Body.Close()
		var body []byte
		buf := make([]byte, 1<<20)
		n, _ := resp.Body.Read(buf)
		body = append(body, buf[:n]...)

		return resp.StatusCode, body
	}

	eventually(t, 30*time.Second, "the API to answer /health", func() bool {
		resp, err := client.Get(base + "/health")
		if err != nil {
			return false
		}
		defer resp.Body.Close()

		return resp.StatusCode == http.StatusOK
	})

	t.Run("followingPrevLinksWalksBackThroughTheWholeConversation", func(t *testing.T) {
		var texts []string
		path := "/v1/conversations/conv_big/messages"
		for pages := 0; path != ""; pages++ {
			require.Less(t, pages, 5, "the walk ends")
			status, body := get(path)
			require.Equal(t, http.StatusOK, status)
			var page apiMessages
			require.NoError(t, json.Unmarshal(body, &page))

			for i := len(page.Messages) - 1; i >= 0; i-- {
				texts = append([]string{page.Messages[i].Text}, texts...)
			}
			if pages == 0 {
				assert.Len(t, page.Messages, 100)
				assert.Equal(t, "message 131", page.Messages[0].Text, "the newest page ends with the last message")
				assert.True(t, page.Meta.HasMore)
			}
			path = page.Links.Prev
		}

		require.Len(t, texts, 230)
		for i, text := range texts {
			assert.Equal(t, fmt.Sprintf("message %d", i+1), text)
		}
	})

	t.Run("messagesCarryTheSmoochFields", func(t *testing.T) {
		status, body := get("/v1/conversations/conv_big/messages?page[after]=228")

		require.Equal(t, http.StatusOK, status)
		var page apiMessages
		require.NoError(t, json.Unmarshal(body, &page))
		require.Len(t, page.Messages, 2)
		assert.Equal(t, "229", page.Messages[0].ID)
		assert.Equal(t, "appUser", page.Messages[0].Role)
		assert.Equal(t, "acc-api", page.Messages[0].AuthorID)
		assert.Equal(t, "appMaker", page.Messages[1].Role)
		assert.Equal(t, "ms-ml-genai-agent-bs", page.Messages[1].AuthorID)
		assert.Equal(t, 229, page.Messages[0].Metadata.Sequence)
		assert.InDelta(t, 1792490629.0, page.Messages[0].Received, 0.001)
		assert.False(t, page.Meta.HasMore)
		assert.Empty(t, page.Links.Prev+page.Links.Next)
	})

	t.Run("conversationsOfAnAccount", func(t *testing.T) {
		status, body := get("/v1/conversations?idYapeAccount=acc-api")

		require.Equal(t, http.StatusOK, status)
		var list struct {
			Conversations []struct {
				ID           string `json:"_id"`
				MessageCount int    `json:"messageCount"`
			} `json:"conversations"`
		}
		require.NoError(t, json.Unmarshal(body, &list))
		require.Len(t, list.Conversations, 1)
		assert.Equal(t, "conv_big", list.Conversations[0].ID)
		assert.Equal(t, 230, list.Conversations[0].MessageCount)
	})

	t.Run("errorsHaveTheirStatus", func(t *testing.T) {
		tests := []struct {
			name       string
			path       string
			wantStatus int
			wantCode   string
		}{
			{name: "bothCursors", path: "/v1/conversations/conv_big/messages?page[before]=100&page[after]=100", wantStatus: 400, wantCode: "invalid_request"},
			{name: "cursorNotANumber", path: "/v1/conversations/conv_big/messages?page[before]=abc", wantStatus: 400, wantCode: "invalid_cursor"},
			{name: "cursorOfNoMessage", path: "/v1/conversations/conv_big/messages?page[after]=99999", wantStatus: 400, wantCode: "invalid_cursor"},
			{name: "unknownConversation", path: "/v1/conversations/conv_nope/messages", wantStatus: 404, wantCode: "not_found"},
			{name: "malformedConversationID", path: "/v1/conversations/bad%20id/messages", wantStatus: 400, wantCode: "invalid_request"},
			{name: "missingAccount", path: "/v1/conversations", wantStatus: 400, wantCode: "invalid_request"},
			{name: "emptyCursor", path: "/v1/conversations/conv_big/messages?page[before]=", wantStatus: 400, wantCode: "invalid_cursor"},
			{name: "emptyCursorBesideARealOne", path: "/v1/conversations/conv_big/messages?page[before]=&page[after]=100", wantStatus: 400, wantCode: "invalid_request"},
			{name: "repeatedCursor", path: "/v1/conversations/conv_big/messages?page[before]=100&page[before]=101", wantStatus: 400, wantCode: "invalid_request"},
			{name: "cursorWithASign", path: "/v1/conversations/conv_big/messages?page[after]=%2B100", wantStatus: 400, wantCode: "invalid_cursor"},
			{name: "unknownRoute", path: "/v1/other", wantStatus: 404, wantCode: "not_found"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				status, body := get(tt.path)

				assert.Equal(t, tt.wantStatus, status)
				var e struct {
					Error struct{ Code string } `json:"error"`
				}
				require.NoError(t, json.Unmarshal(body, &e))
				assert.Equal(t, tt.wantCode, e.Error.Code)
			})
		}
	})

	t.Run("anyTextComesBackExactly", func(t *testing.T) {
		texts := []string{
			"hola 😀 👨‍👩‍👧 🇵🇪",
			"ñandú, canción, ¿qué tal?",
			"مرحبا بالعالم",
			"日本語のテキスト",
			"e\u0301 (combining accent) and \u200b zero width",
			"line one\nline two\r\n\ttabbed",
			"<script>alert(1)</script> & \"quotes\" 'single' \\ backslash",
			"separators \u2028 and \u2029 inside",
			"{\"looks\": \"like json\"} ' OR 1=1 --",
			"",
			strings.Repeat("x", 900_000), // close to Kafka's default 1 MB record limit
		}
		for i, text := range texts {
			_, err := pool.Exec(ctx, `INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
				VALUES ('conv_text', $1, 'appUser', 'a', $2, 'UserMessageSent', 'acc-text', now())`, i+1, text)
			require.NoError(t, err)
		}

		status, body := get("/v1/conversations/conv_text/messages")

		require.Equal(t, http.StatusOK, status)
		var page apiMessages
		require.NoError(t, json.Unmarshal(body, &page))
		require.Len(t, page.Messages, len(texts))
		for i, m := range page.Messages {
			// A plain comparison: a diff of a 900 000 character text is not readable.
			assert.True(t, m.Text == texts[i], "message %d came back changed (length %d, want %d)", i+1, len(m.Text), len(texts[i]))
		}
		assert.Equal(t, 900_000, len(page.Messages[len(texts)-1].Text), "a long text is not cut")
	})

	t.Run("healthFollowsTheDatabase", func(t *testing.T) {
		status, body := get("/health")

		assert.Equal(t, http.StatusOK, status)
		assert.JSONEq(t, `{"status":"ok"}`, string(body))
	})

	proc.stop(t)
	assert.NotContains(t, proc.out.String(), "acc-api", "the log has no account id")
	assert.NotContains(t, proc.out.String(), "message 1", "the log has no message text")
	assert.Contains(t, proc.out.String(), "GET /v1/conversations/:conversationId/messages 200")
}
