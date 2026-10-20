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
		`SELECT column_name, data_type, is_nullable = 'YES' FROM information_schema.columns WHERE table_name = $1`, table)
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

func tableExists(t *testing.T, pool *pgxpool.Pool, table string) bool {
	t.Helper()

	var exists bool
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)`, table).Scan(&exists))

	return exists
}

func TestSchema(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	migrateUp(t, pool)

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
					"source_event":    {"character varying", false},
					"id_yape_account": {"text", true},
					"received_at":     {"timestamp with time zone", false},
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
				name:  "accountListIndex",
				query: `SELECT indexdef FROM pg_indexes WHERE indexname = 'conversation_messages_account_idx'`,
				want:  "CREATE INDEX conversation_messages_account_idx ON public.conversation_messages USING btree (id_yape_account, received_at DESC)",
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

	t.Run("everyColumnHasAComment", func(t *testing.T) {
		tests := []struct{ table string }{{"conversation_inbox"}, {"conversation_messages"}}

		for _, tt := range tests {
			t.Run(tt.table, func(t *testing.T) {
				var uncommented []string
				require.NoError(t, pool.QueryRow(ctx, `
					SELECT COALESCE(array_agg(a.attname), '{}') FROM pg_attribute a
					WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
					  AND col_description(a.attrelid, a.attnum) IS NULL`, tt.table).Scan(&uncommented))
				assert.Empty(t, uncommented)

				var tableComment *string
				require.NoError(t, pool.QueryRow(ctx, `SELECT obj_description($1::regclass, 'pg_class')`, tt.table).Scan(&tableComment))
				assert.NotNil(t, tableComment)
			})
		}
	})

	t.Run("checksAndDefaults", func(t *testing.T) {
		const insertInbox = `INSERT INTO conversation_inbox (conversation_id, sequence, event_name, event_version, payload, metadata, actor, occurred_at, kafka_topic, kafka_partition, kafka_offset)
			VALUES ('c', $1::int, 'E', 1, '{}', '{}', $2, now(), 't', 0, 0)`
		const insertMessage = `INSERT INTO conversation_messages (conversation_id, sequence, role, author_id, text, source_event, received_at)
			VALUES ('c', $1::int, $2, 'a', 'hi', 'E', now())`

		tests := []struct {
			name    string
			sql     string
			args    []any
			wantErr string
		}{
			{name: "actorUser", sql: insertInbox, args: []any{1, "user"}},
			{name: "actorAgent", sql: insertInbox, args: []any{2, "agent"}},
			{name: "actorSystem", sql: insertInbox, args: []any{3, "system"}},
			{name: "actorNullIsAllowed", sql: insertInbox, args: []any{4, nil}},
			{name: "actorUnknownIsRejected", sql: insertInbox, args: []any{5, "robot"}, wantErr: "conversation_inbox_actor_check"},
			{name: "repeatedStreamPositionIsRejected", sql: insertInbox, args: []any{1, "user"}, wantErr: "conversation_inbox_stream_key"},
			{name: "roleAppUser", sql: insertMessage, args: []any{1, "appUser"}},
			{name: "roleAppMaker", sql: insertMessage, args: []any{2, "appMaker"}},
			{name: "roleUnknownIsRejected", sql: insertMessage, args: []any{3, "bot"}, wantErr: "conversation_messages_role_check"},
			{name: "repeatedMessageIsRejected", sql: insertMessage, args: []any{1, "appUser"}, wantErr: "conversation_messages_pkey"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := pool.Exec(ctx, tt.sql, tt.args...)

				if tt.wantErr != "" {
					require.Error(t, err)
					assert.Contains(t, err.Error(), tt.wantErr)
					return
				}
				require.NoError(t, err)
			})
		}

		var typ string
		var truncated bool
		require.NoError(t, pool.QueryRow(ctx, `SELECT type, truncated FROM conversation_messages WHERE sequence = 1`).Scan(&typ, &truncated))
		assert.Equal(t, "text", typ)
		assert.False(t, truncated)

		var receivedSet bool
		var projected *string
		require.NoError(t, pool.QueryRow(ctx, `SELECT received_at IS NOT NULL, projected_at::text FROM conversation_inbox WHERE sequence = 1`).Scan(&receivedSet, &projected))
		assert.True(t, receivedSet)
		assert.Nil(t, projected)
	})

	t.Run("upTwiceIsSafeAndKeepsRows", func(t *testing.T) {
		migrateUp(t, pool)

		var rows int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM conversation_inbox`).Scan(&rows))
		assert.Equal(t, 4, rows)
	})

	t.Run("downRemovesBothTablesAndRepeats", func(t *testing.T) {
		tests := []struct{ name string }{{"first"}, {"second"}}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				runMigration(t, pool, "000001_create_inbox_and_messages.down.sql")

				assert.False(t, tableExists(t, pool, "conversation_inbox"))
				assert.False(t, tableExists(t, pool, "conversation_messages"))
			})
		}
	})
}
