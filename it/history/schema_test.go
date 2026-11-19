package history_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type column struct {
	dataType string
	nullable bool
}

func columnsOf(t *testing.T, pool *pgxpool.Pool, table string) map[string]column {
	t.Helper()

	rows, err := pool.Query(context.Background(),
		`SELECT column_name, data_type, is_nullable = 'YES' FROM information_schema.columns WHERE table_schema = 'history' AND table_name = $1`, table)
	require.NoError(t, err)
	defer rows.Close()

	got := map[string]column{}
	for rows.Next() {
		var name string
		var c column
		require.NoError(t, rows.Scan(&name, &c.dataType, &c.nullable))
		got[name] = c
	}
	require.NoError(t, rows.Err())

	return got
}

// TestSchema pins the shape the service's queries rely on: the columns they read and the keys and indexes that keep a page
// of messages or of conversations from scanning the account's history. The migrations themselves are tested where they live,
// in ms-ml-conv-history-migrations.
func TestSchema(t *testing.T) {
	pool, _ := newHistoryDB(t)
	ctx := context.Background()

	t.Run("columnsAndTypes", func(t *testing.T) {
		tests := []struct {
			table string
			want  map[string]column
		}{
			{
				table: "conversation_inbox",
				want: map[string]column{
					"conversation_id": {"character varying", false},
					"sequence":        {"integer", false},
					"event_name":      {"character varying", false},
					"event_version":   {"integer", false},
					"payload":         {"jsonb", false},
					"metadata":        {"jsonb", false},
					"correlation_id":  {"text", true},
					"id_yape_account": {"text", true},
					"actor":           {"text", true},
					"flow":            {"text", true},
					"occurred_at":     {"timestamp with time zone", false},
					"kafka_topic":     {"text", false},
					"kafka_partition": {"integer", false},
					"kafka_offset":    {"bigint", false},
					"received_at":     {"timestamp with time zone", false},
					"projected_at":    {"timestamp with time zone", true},
				},
			},
			{
				table: "conversation_messages",
				want: map[string]column{
					"conversation_id": {"character varying", false},
					"sequence":        {"integer", false},
					"role":            {"text", false},
					"author_id":       {"text", false},
					"type":            {"text", false},
					"text":            {"text", false},
					"truncated":       {"boolean", false},
					"event_name":      {"character varying", false},
					"id_yape_account": {"text", true},
					"flow":            {"text", true},
					"occurred_at":     {"timestamp with time zone", false},
				},
			},
			{
				table: "conversation_summaries",
				want: map[string]column{
					"id_yape_account":   {"text", false},
					"conversation_id":   {"character varying", false},
					"message_count":     {"integer", false},
					"first_occurred_at": {"timestamp with time zone", false},
					"last_occurred_at":  {"timestamp with time zone", false},
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.table, func(t *testing.T) {
				assert.Equal(t, tt.want, columnsOf(t, pool, tt.table))
			})
		}
	})

	t.Run("keysAndIndexes", func(t *testing.T) {
		tests := []struct {
			name  string
			query string
			want  string
		}{
			{
				name:  "inboxPrimaryKey",
				query: `SELECT string_agg(a.attname, ',' ORDER BY a.attnum) FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey) WHERE i.indrelid = 'conversation_inbox'::regclass AND i.indisprimary`,
				want:  "conversation_id,sequence",
			},
			{
				name:  "inboxPrimaryKeyIsNamedAfterTheStream",
				query: `SELECT conname FROM pg_constraint WHERE conrelid = 'conversation_inbox'::regclass AND contype = 'p'`,
				want:  "conversation_inbox_stream_key",
			},
			{
				name:  "messagesPrimaryKey",
				query: `SELECT string_agg(a.attname, ',' ORDER BY a.attnum) FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey) WHERE i.indrelid = 'conversation_messages'::regclass AND i.indisprimary`,
				want:  "conversation_id,sequence",
			},
			{
				name:  "summariesPrimaryKeyStartsWithTheAccount",
				query: `SELECT string_agg(a.attname, ',' ORDER BY array_position(i.indkey::int2[], a.attnum)) FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey) WHERE i.indrelid = 'conversation_summaries'::regclass AND i.indisprimary`,
				want:  "id_yape_account,conversation_id",
			},
			{
				name:  "summariesListIndexMatchesTheOrderOfTheList",
				query: `SELECT indexdef FROM pg_indexes WHERE indexname = 'conversation_summaries_list_idx'`,
				want:  "CREATE INDEX conversation_summaries_list_idx ON history.conversation_summaries USING btree (id_yape_account, last_occurred_at DESC, conversation_id DESC)",
			},
			{
				name:  "accountListIndex",
				query: `SELECT indexdef FROM pg_indexes WHERE indexname = 'conversation_messages_account_idx'`,
				want:  "CREATE INDEX conversation_messages_account_idx ON history.conversation_messages USING btree (id_yape_account, occurred_at DESC)",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var got string
				require.NoError(t, pool.QueryRow(ctx, tt.query).Scan(&got))
				assert.Equal(t, tt.want, got)
			})
		}
	})
}
