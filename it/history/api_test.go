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
}

// linkRel finds the target of one relation in a Link header such as
// `</v1/...?page[before]=131>; rel="prev"`. It is empty when the header has no such relation.
func linkRel(header, rel string) string {
	for _, part := range strings.Split(header, ",") {
		target, params, ok := strings.Cut(strings.TrimSpace(part), ">; rel=")
		if ok && params == `"`+rel+`"` {
			return strings.TrimPrefix(target, "<")
		}
	}

	return ""
}

// TestAPI runs the real cmd/api binary on a database with a long conversation
// and reads it over HTTP, following the Link header the API gives.
func TestAPI(t *testing.T) {
	pool, dsn := newHistoryDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
		SELECT 'conv_big', s, CASE WHEN s % 2 = 1 THEN 'appUser' ELSE 'appMaker' END,
		       CASE WHEN s % 2 = 1 THEN 'acc-api' ELSE 'ms-ml-genai-agent-bs' END, 'message ' || s, 'UserMessageSent', 'acc-api',
		       '2026-10-20T10:00:00Z'::timestamptz + s * interval '1 second'
		FROM generate_series(1, 230) s;
		INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
		VALUES ('conv_gaps', 2, 'appUser', 'acc-gaps', 'one', 'UserMessageSent', 'acc-gaps', '2026-10-20T11:00:00Z'),
		       ('conv_gaps', 5, 'appMaker', 'ms-ml-genai-agent-bs', 'two', 'AgentMessageSent', 'acc-gaps', '2026-10-20T11:00:01Z'),
		       ('conv_gaps', 9, 'appUser', 'acc-gaps', 'three', 'UserMessageSent', 'acc-gaps', '2026-10-20T11:00:02Z');
		INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, event_name, id_yape_account, occurred_at)
		SELECT 'list_' || c, 1, 'appUser', 'acc-list', 'hola', 'UserMessageSent', 'acc-list', t::timestamptz
		FROM (VALUES ('1', '2026-10-21T10:00:00Z'), ('2', '2026-10-21T11:00:00Z'), ('3', '2026-10-21T12:00:00Z'),
		             ('4', '2026-10-21T13:00:00Z'), ('5', '2026-10-21T14:00:00Z'),
		             ('tie_a', '2026-10-21T15:00:00Z'), ('tie_b', '2026-10-21T15:00:00Z')) v(c, t)`)
	require.NoError(t, err)

	port := freePort(t)
	proc := startBinary(t, buildBinary(t, "api"), []string{"POSTGRES_DSN=" + dsn, fmt.Sprintf("SERVER_PORT=%d", port)})
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	client := http.Client{Timeout: 10 * time.Second}
	getWithHeader := func(path string) (int, []byte, http.Header) {
		t.Helper()

		resp, err := client.Get(base + path)
		require.NoError(t, err)
		defer resp.Body.Close()
		var body []byte
		buf := make([]byte, 1<<20)
		n, _ := resp.Body.Read(buf)
		body = append(body, buf[:n]...)

		return resp.StatusCode, body, resp.Header
	}
	get := func(path string) (int, []byte) {
		t.Helper()

		status, body, _ := getWithHeader(path)

		return status, body
	}

	eventually(t, 30*time.Second, "the API to answer /health", func() bool {
		resp, err := client.Get(base + "/health")
		if err != nil {
			return false
		}
		defer resp.Body.Close()

		return resp.StatusCode == http.StatusOK
	})

	// walkBack follows rel="prev" from the newest page until the header stops, and returns
	// the texts oldest first together with the number of messages of every page.
	walkBack := func(t *testing.T, path string) ([]string, []int) {
		t.Helper()

		var texts []string
		var sizes []int
		for pages := 0; path != ""; pages++ {
			require.Less(t, pages, 30, "the walk ends")
			status, body, header := getWithHeader(path)
			require.Equal(t, http.StatusOK, status)
			var page apiMessages
			require.NoError(t, json.Unmarshal(body, &page))
			assert.NotContains(t, string(body), `"meta"`, "the body holds no paging fields")
			assert.NotContains(t, string(body), `"links"`, "the body holds no paging fields")

			for i := len(page.Messages) - 1; i >= 0; i-- {
				texts = append([]string{page.Messages[i].Text}, texts...)
			}
			sizes = append(sizes, len(page.Messages))
			assert.Empty(t, linkRel(header.Get("Link"), "next"), "a page read backwards never offers next")
			path = linkRel(header.Get("Link"), "prev")
		}

		return texts, sizes
	}

	t.Run("followingPrevLinksWalksBackThroughTheWholeConversation", func(t *testing.T) {
		texts, sizes := walkBack(t, "/v1/conversations/conv_big/messages")

		assert.Equal(t, []int{100, 100, 30}, sizes)
		require.Len(t, texts, 230)
		for i, text := range texts {
			assert.Equal(t, fmt.Sprintf("message %d", i+1), text)
		}
	})

	t.Run("theNewestPageSaysThereIsMoreOnlyInTheHeader", func(t *testing.T) {
		_, body, header := getWithHeader("/v1/conversations/conv_big/messages")

		assert.Equal(t, `</v1/conversations/conv_big/messages?page[before]=131>; rel="prev"`, header.Get("Link"))
		var page apiMessages
		require.NoError(t, json.Unmarshal(body, &page))
		assert.Len(t, page.Messages, 100)
		assert.Equal(t, "message 131", page.Messages[0].Text, "the newest page ends with the last message")
	})

	t.Run("aChosenPageSizeSurvivesEveryLinkOfTheWalk", func(t *testing.T) {
		texts, sizes := walkBack(t, "/v1/conversations/conv_big/messages?page[size]=40")

		assert.Equal(t, []int{40, 40, 40, 40, 40, 30}, sizes)
		require.Len(t, texts, 230)
		for i, text := range texts {
			assert.Equal(t, fmt.Sprintf("message %d", i+1), text)
		}
	})

	t.Run("aSizeThatDividesTheConversationExactlyLeavesNoEmptyLastPage", func(t *testing.T) {
		for _, size := range []int{10, 23, 46} {
			texts, sizes := walkBack(t, fmt.Sprintf("/v1/conversations/conv_big/messages?page[size]=%d", size))

			want := make([]int, 230/size)
			for i := range want {
				want[i] = size
			}
			assert.Equal(t, want, sizes, "size %d: every page is full and the last one has no Link, so there is no empty page after it", size)
			assert.Len(t, texts, 230)
		}
	})

	t.Run("aSizeJustAboveTheConversationReturnsItAllWithoutALink", func(t *testing.T) {
		_, body, header := getWithHeader("/v1/conversations/conv_gaps/messages?page[size]=4")

		var page apiMessages
		require.NoError(t, json.Unmarshal(body, &page))
		assert.Len(t, page.Messages, 3)
		assert.Empty(t, header.Values("Link"))
	})

	t.Run("aSizeOfOneWalksAConversationWhoseSequenceHasGaps", func(t *testing.T) {
		texts, sizes := walkBack(t, "/v1/conversations/conv_gaps/messages?page[size]=1")

		assert.Equal(t, []int{1, 1, 1}, sizes)
		assert.Equal(t, []string{"one", "two", "three"}, texts)
	})

	t.Run("anExplicitSizeOfOneHundredIsTheDefaultSoTheLinkDoesNotRepeatIt", func(t *testing.T) {
		_, _, header := getWithHeader("/v1/conversations/conv_big/messages?page[size]=100")

		assert.Equal(t, `</v1/conversations/conv_big/messages?page[before]=131>; rel="prev"`, header.Get("Link"))
	})

	t.Run("aSizeOfNinetyNineRepeatsItInTheLink", func(t *testing.T) {
		_, body, header := getWithHeader("/v1/conversations/conv_big/messages?page[size]=99")

		var page apiMessages
		require.NoError(t, json.Unmarshal(body, &page))
		assert.Len(t, page.Messages, 99)
		assert.Equal(t, `</v1/conversations/conv_big/messages?page[before]=132&page[size]=99>; rel="prev"`, header.Get("Link"))
	})

	t.Run("theSizeAppliesToTheOtherDirectionAndEncodedBracketsToo", func(t *testing.T) {
		status, body, _ := getWithHeader("/v1/conversations/conv_big/messages?page%5Bafter%5D=10&page%5Bsize%5D=3")

		require.Equal(t, http.StatusOK, status)
		var page apiMessages
		require.NoError(t, json.Unmarshal(body, &page))
		require.Len(t, page.Messages, 3)
		assert.Equal(t, "11", page.Messages[0].ID)
		assert.Equal(t, "13", page.Messages[2].ID)
	})

	t.Run("followingNextLinksWalksForwardWithTheChosenSize", func(t *testing.T) {
		var texts []string
		path := "/v1/conversations/conv_big/messages?page[after]=190&page[size]=15"
		for pages := 0; path != ""; pages++ {
			require.Less(t, pages, 10, "the walk ends")
			status, body, header := getWithHeader(path)
			require.Equal(t, http.StatusOK, status)
			var page apiMessages
			require.NoError(t, json.Unmarshal(body, &page))
			for _, m := range page.Messages {
				texts = append(texts, m.Text)
			}
			assert.Empty(t, linkRel(header.Get("Link"), "prev"), "a page read forwards never offers prev")
			path = linkRel(header.Get("Link"), "next")
		}

		require.Len(t, texts, 40)
		assert.Equal(t, "message 191", texts[0])
		assert.Equal(t, "message 230", texts[39])
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
	})

	t.Run("theLastPageHasNoLinkHeader", func(t *testing.T) {
		status, _, header := getWithHeader("/v1/conversations/conv_big/messages?page[after]=228")

		require.Equal(t, http.StatusOK, status)
		assert.Empty(t, header.Values("Link"))
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

	type conversationsPage struct {
		Conversations []struct {
			ID           string `json:"_id"`
			MessageCount int    `json:"messageCount"`
		} `json:"conversations"`
	}
	// walkConversations follows one relation of the Link header from path until it stops,
	// and returns the ids in the order the pages came and the number of pages.
	walkConversations := func(t *testing.T, path, rel string) ([][]string, []string) {
		t.Helper()

		var pages [][]string
		var links []string
		for n := 0; path != ""; n++ {
			require.Less(t, n, 20, "the walk ends")
			status, body, header := getWithHeader(path)
			require.Equal(t, http.StatusOK, status)
			assert.NotContains(t, string(body), `"meta"`, "the body holds no paging fields")
			assert.NotContains(t, string(body), `"links"`, "the body holds no paging fields")
			var page conversationsPage
			require.NoError(t, json.Unmarshal(body, &page))
			ids := []string{}
			for _, c := range page.Conversations {
				ids = append(ids, c.ID)
			}
			pages = append(pages, ids)
			other := map[string]string{"prev": "next", "next": "prev"}[rel]
			assert.Empty(t, linkRel(header.Get("Link"), other), "a walk never offers the other relation")
			links = append(links, header.Get("Link"))
			path = linkRel(header.Get("Link"), rel)
		}

		return pages, links
	}

	t.Run("aShortListHasNoLinkHeader", func(t *testing.T) {
		status, body, header := getWithHeader("/v1/conversations?idYapeAccount=acc-list")

		require.Equal(t, http.StatusOK, status)
		var page conversationsPage
		require.NoError(t, json.Unmarshal(body, &page))
		require.Len(t, page.Conversations, 7)
		assert.Equal(t, "list_tie_a", page.Conversations[0].ID, "the tie is broken by id")
		assert.Equal(t, "list_tie_b", page.Conversations[1].ID)
		assert.Equal(t, "list_5", page.Conversations[2].ID)
		assert.Empty(t, header.Values("Link"))
	})

	t.Run("followingPrevLinksWalksBackThroughEveryConversation", func(t *testing.T) {
		pages, links := walkConversations(t, "/v1/conversations?idYapeAccount=acc-list&page[size]=2", "prev")

		assert.Equal(t, [][]string{{"list_tie_a", "list_tie_b"}, {"list_5", "list_4"}, {"list_3", "list_2"}, {"list_1"}}, pages)
		assert.Equal(t, `</v1/conversations?idYapeAccount=acc-list&page[before]=list_tie_b&page[size]=2>; rel="prev"`, links[0])
		assert.Empty(t, links[len(links)-1], "the last page has no Link header")
	})

	t.Run("followingNextLinksWalksForwardFromTheOldest", func(t *testing.T) {
		pages, links := walkConversations(t, "/v1/conversations?idYapeAccount=acc-list&page[after]=list_1&page[size]=3", "next")

		// Each page is most recent first, and the walk goes towards the most recent ones.
		assert.Equal(t, [][]string{{"list_4", "list_3", "list_2"}, {"list_tie_a", "list_tie_b", "list_5"}}, pages)
		assert.Equal(t, `</v1/conversations?idYapeAccount=acc-list&page[after]=list_4&page[size]=3>; rel="next"`, links[0])
		assert.Empty(t, links[len(links)-1], "the last page has no Link header")
	})

	t.Run("anExactMultipleLeavesNoEmptyLastPage", func(t *testing.T) {
		pages, _ := walkConversations(t, "/v1/conversations?idYapeAccount=acc-list&page[size]=7", "prev")

		assert.Len(t, pages, 1)
		pages, _ = walkConversations(t, "/v1/conversations?idYapeAccount=acc-list&page[size]=1", "prev")
		assert.Len(t, pages, 7)
	})

	t.Run("aPageBeyondTheOldestIsEmptyWithoutALink", func(t *testing.T) {
		status, body, header := getWithHeader("/v1/conversations?idYapeAccount=acc-list&page[before]=list_1")

		require.Equal(t, http.StatusOK, status)
		assert.JSONEq(t, `{"conversations":[]}`, string(body))
		assert.Empty(t, header.Values("Link"))
	})

	t.Run("anAccountWithNoConversationsGetsAnEmptyPage", func(t *testing.T) {
		status, body, header := getWithHeader("/v1/conversations?idYapeAccount=nobody-here")

		require.Equal(t, http.StatusOK, status)
		assert.JSONEq(t, `{"conversations":[]}`, string(body))
		assert.Empty(t, header.Values("Link"))
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
			{name: "listBothCursors", path: "/v1/conversations?idYapeAccount=acc-list&page[before]=list_1&page[after]=list_2", wantStatus: 400, wantCode: "invalid_request"},
			{name: "listCursorOfAnotherAccount", path: "/v1/conversations?idYapeAccount=acc-list&page[before]=conv_big", wantStatus: 400, wantCode: "invalid_cursor"},
			{name: "listCursorThatDoesNotExist", path: "/v1/conversations?idYapeAccount=acc-list&page[after]=nope", wantStatus: 400, wantCode: "invalid_cursor"},
			{name: "listEmptyCursor", path: "/v1/conversations?idYapeAccount=acc-list&page[before]=", wantStatus: 400, wantCode: "invalid_cursor"},
			{name: "listMalformedCursor", path: "/v1/conversations?idYapeAccount=acc-list&page[before]=a%20b", wantStatus: 400, wantCode: "invalid_cursor"},
			{name: "listPageSizeZero", path: "/v1/conversations?idYapeAccount=acc-list&page[size]=0", wantStatus: 400, wantCode: "invalid_request"},
			{name: "listPageSizeOverTheMaximum", path: "/v1/conversations?idYapeAccount=acc-list&page[size]=101", wantStatus: 400, wantCode: "invalid_request"},
			{name: "listRepeatedCursor", path: "/v1/conversations?idYapeAccount=acc-list&page[before]=list_1&page[before]=list_2", wantStatus: 400, wantCode: "invalid_request"},
			{name: "listBadSizeBesideABadCursorIsASizeError", path: "/v1/conversations?idYapeAccount=acc-list&page[size]=0&page[before]=nope", wantStatus: 400, wantCode: "invalid_request"},
			{name: "pageSizeZero", path: "/v1/conversations/conv_big/messages?page[size]=0", wantStatus: 400, wantCode: "invalid_request"},
			{name: "pageSizeOverTheMaximum", path: "/v1/conversations/conv_big/messages?page[size]=101", wantStatus: 400, wantCode: "invalid_request"},
			{name: "pageSizeNotANumber", path: "/v1/conversations/conv_big/messages?page[size]=many", wantStatus: 400, wantCode: "invalid_request"},
			{name: "emptyPageSize", path: "/v1/conversations/conv_big/messages?page[size]=", wantStatus: 400, wantCode: "invalid_request"},
			{name: "pageSizeNegative", path: "/v1/conversations/conv_big/messages?page[size]=-5", wantStatus: 400, wantCode: "invalid_request"},
			{name: "pageSizeWithASign", path: "/v1/conversations/conv_big/messages?page[size]=%2B5", wantStatus: 400, wantCode: "invalid_request"},
			{name: "pageSizeWithLeadingZeros", path: "/v1/conversations/conv_big/messages?page[size]=05", wantStatus: 400, wantCode: "invalid_request"},
			{name: "pageSizeWithAnEncodedSpace", path: "/v1/conversations/conv_big/messages?page[size]=5%20", wantStatus: 400, wantCode: "invalid_request"},
			{name: "pageSizeBeyondAnInt64", path: "/v1/conversations/conv_big/messages?page[size]=99999999999999999999", wantStatus: 400, wantCode: "invalid_request"},
			{name: "badPageSizeBesideABadCursorIsAPageSizeError", path: "/v1/conversations/conv_big/messages?page[size]=0&page[before]=abc", wantStatus: 400, wantCode: "invalid_request"},
			{name: "goodPageSizeBesideABadCursorIsACursorError", path: "/v1/conversations/conv_big/messages?page[size]=5&page[before]=abc", wantStatus: 400, wantCode: "invalid_cursor"},
			{name: "goodPageSizeOnAnUnknownConversation", path: "/v1/conversations/conv_nope/messages?page[size]=5", wantStatus: 404, wantCode: "not_found"},
			{name: "repeatedPageSize", path: "/v1/conversations/conv_big/messages?page[size]=5&page[size]=6", wantStatus: 400, wantCode: "invalid_request"},
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
