// Package history_test runs the history service against a real Postgres.
package history_test

import (
	"context"
	_ "embed"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport"
)

// historySchema is the schema the service reads: the tables, the indexes and the flow-segments view, in the schema history.
//
//go:embed testdata/schema.sql
var historySchema string

// historyDSN is a throwaway Postgres whose search_path is the schema history, as the service's default.
func historyDSN(t *testing.T) string {
	t.Helper()

	return testsupport.PostgresDSN(t) + "&search_path=history"
}

// newPool starts a throwaway Postgres and returns a pool on it, closed on cleanup.
func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	pool, err := pgxpool.New(context.Background(), historyDSN(t))
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return pool
}

// newHistoryDB starts a throwaway Postgres with the history schema and returns a
// pool on it and its DSN, which a started binary can use.
func newHistoryDB(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()

	dsn := historyDSN(t)
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	createSchema(t, pool)

	return pool, dsn
}

// createSchema creates the history schema. Every statement is IF NOT EXISTS or CREATE OR REPLACE, so a second call is safe.
func createSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	_, err := pool.Exec(context.Background(), historySchema)
	require.NoError(t, err)
}

// summaryDrift is the number of conversations whose summary differs from what grouping their messages
// gives: one that is missing, one that should not be there, and one with another count or other times.
const summaryDrift = `
	SELECT count(*) FROM (
	    (SELECT id_yape_account, conversation_id, count(*)::int AS n, min(occurred_at) AS first, max(occurred_at) AS last
	       FROM conversation_messages WHERE id_yape_account IS NOT NULL GROUP BY id_yape_account, conversation_id
	     EXCEPT
	     SELECT id_yape_account, conversation_id, message_count, first_occurred_at, last_occurred_at FROM conversation_summaries)
	    UNION ALL
	    (SELECT id_yape_account, conversation_id, message_count, first_occurred_at, last_occurred_at FROM conversation_summaries
	     EXCEPT
	     SELECT id_yape_account, conversation_id, count(*)::int, min(occurred_at), max(occurred_at)
	       FROM conversation_messages WHERE id_yape_account IS NOT NULL GROUP BY id_yape_account, conversation_id)
	) drift`

// assertNoSummaryDrift fails the test when conversation_summaries is not exactly what the messages say.
func assertNoSummaryDrift(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	var drift int
	require.NoError(t, pool.QueryRow(context.Background(), summaryDrift).Scan(&drift))
	require.Zero(t, drift, "%d conversation summaries differ from their messages", drift)
}

// refreshSummaries fills conversation_summaries from the messages, from the messages. A test that
// writes messages with plain SQL calls it before reading the list.
func refreshSummaries(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	_, err := pool.Exec(context.Background(), `
		TRUNCATE conversation_summaries;
		INSERT INTO conversation_summaries (id_yape_account, conversation_id, message_count, first_occurred_at, last_occurred_at)
		SELECT id_yape_account, conversation_id, count(*), min(occurred_at), max(occurred_at)
		FROM conversation_messages WHERE id_yape_account IS NOT NULL GROUP BY id_yape_account, conversation_id`)
	require.NoError(t, err)
}
