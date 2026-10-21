// Package history_test runs the history service against a real Postgres.
package history_test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport"
)

// defaultMigrationsDir is where the testing repo keeps this service's migrations,
// as a sibling checkout. HISTORY_MIGRATIONS_DIR points elsewhere.
const defaultMigrationsDir = "../../../ml-genai-agents-bs-testing/migrations/history/conversation"

func migrationsDir(t *testing.T) string {
	t.Helper()

	dir := os.Getenv("HISTORY_MIGRATIONS_DIR")
	if dir == "" {
		dir = defaultMigrationsDir
	}
	info, err := os.Stat(dir)
	require.NoError(t, err, "the history migrations live in the testing repo; check it out beside this one or set HISTORY_MIGRATIONS_DIR")
	require.True(t, info.IsDir(), dir)

	return dir
}

// newPool starts a throwaway Postgres and returns a pool on it, closed on cleanup.
func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	pool, err := pgxpool.New(context.Background(), testsupport.PostgresDSN(t))
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return pool
}

// newHistoryDB starts a throwaway Postgres with the history tables and returns a
// pool on it and its DSN, which a consumer process can use.
func newHistoryDB(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()

	dsn := testsupport.PostgresDSN(t)
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	migrateUp(t, pool)

	return pool, dsn
}

// runMigration executes one migration file.
func runMigration(t *testing.T, pool *pgxpool.Pool, file string) {
	t.Helper()

	sql, err := os.ReadFile(filepath.Join(migrationsDir(t), file))
	require.NoError(t, err)

	_, err = pool.Exec(context.Background(), string(sql))
	require.NoError(t, err, file)
}

// migrateUp applies every up migration of the folder, in order: the inbox and the
// messages tables, then the views over them.
func migrateUp(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	for _, file := range migrationFiles(t, ".up.sql", false) {
		runMigration(t, pool, file)
	}
}

// migrateDown applies every down migration, last first.
func migrateDown(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	for _, file := range migrationFiles(t, ".down.sql", true) {
		runMigration(t, pool, file)
	}
}

func migrationFiles(t *testing.T, suffix string, reverse bool) []string {
	t.Helper()

	files, err := filepath.Glob(filepath.Join(migrationsDir(t), "*"+suffix))
	require.NoError(t, err)
	sort.Strings(files)
	if reverse {
		slices.Reverse(files)
	}
	for i, f := range files {
		files[i] = filepath.Base(f)
	}

	return files
}
