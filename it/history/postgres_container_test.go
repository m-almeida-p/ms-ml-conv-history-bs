package history_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport"
)

// The outage tests need a database they can stop and start again under the same
// connection string, with its data. This checks the helper that gives them one.
func TestPostgresContainerCanBeStoppedAndStartedAgain(t *testing.T) {
	pg := testsupport.StartPostgres(t)
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, pg.DSN)
	require.NoError(t, err)
	defer pool.Close()
	_, err = pool.Exec(ctx, "CREATE TABLE kept (n int); INSERT INTO kept VALUES (42)")
	require.NoError(t, err)

	pg.Stop(t)
	downCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	assert.Error(t, pool.Ping(downCtx), "a stopped database does not answer")

	pg.Start(t)
	var n int
	// The pool's old connections are replaced on demand.
	eventually(t, 20*time.Second, "the restarted database to answer", func() bool {
		return pool.QueryRow(ctx, "SELECT n FROM kept").Scan(&n) == nil
	})
	assert.Equal(t, 42, n, "the data and the address survive a restart")
}
