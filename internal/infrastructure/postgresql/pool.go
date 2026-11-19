// Package postgresql builds the pgx pool every repository shares.
package postgresql

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/config"
)

// redactedPasswordMask replaces a password before a DSN reaches a log line.
const redactedPasswordMask = "xxxxx"

// DB is what a repository needs from the pool. *pgxpool.Pool satisfies it, and
// so does pgxmock in unit tests.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// BuildDSN renders a connection URL. An explicit DSN is returned as is (trimmed);
// otherwise the discrete fields are composed, each one escaped by net/url so a
// password with @, : or / cannot corrupt the URL.
func BuildDSN(cfg config.PostgresConfig) string {
	if dsn := strings.TrimSpace(cfg.DSN); dsn != "" {
		return dsn
	}

	query := url.Values{}
	query.Set("sslmode", cfg.SSLMode)
	if searchPath := strings.TrimSpace(cfg.SearchPath); searchPath != "" {
		query.Set("search_path", searchPath)
	}
	if cfg.ConnectTimeoutSeconds > 0 {
		query.Set("connect_timeout", strconv.Itoa(cfg.ConnectTimeoutSeconds))
	}

	dsn := url.URL{
		Scheme:   "postgres",
		Host:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Path:     "/" + cfg.Database,
		RawQuery: query.Encode(),
	}
	if cfg.Password == "" {
		dsn.User = url.User(cfg.User)
	} else {
		dsn.User = url.UserPassword(cfg.User, cfg.Password)
	}

	return dsn.String()
}

// RedactDSN masks the password so a connection string can be logged. A DSN that
// does not parse is dropped, since it may still hold credentials.
func RedactDSN(dsn string) string {
	if strings.TrimSpace(dsn) == "" {
		return ""
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		return "<redacted>"
	}
	if parsed.User == nil {
		return parsed.String()
	}
	if _, hasPassword := parsed.User.Password(); !hasPassword {
		return parsed.String()
	}

	parsed.User = url.UserPassword(parsed.User.Username(), redactedPasswordMask)

	return parsed.String()
}

// poolConfig translates the settings into a pgxpool config. Zero lifetime and
// idle values mean unlimited to pgx.
func poolConfig(cfg config.PostgresConfig) (*pgxpool.Config, error) {
	poolCfg, err := pgxpool.ParseConfig(BuildDSN(cfg))
	if err != nil {
		// The driver error can carry the DSN, so it is redacted, not wrapped.
		return nil, fmt.Errorf("parse postgres dsn: %s", redactError(err, cfg))
	}

	poolCfg.MaxConns = int32(cfg.MaxConns)
	poolCfg.MinConns = int32(cfg.MinConns)
	poolCfg.MaxConnLifetime = time.Duration(cfg.ConnMaxLifetimeSeconds) * time.Second
	poolCfg.MaxConnIdleTime = time.Duration(cfg.ConnMaxIdleTimeSeconds) * time.Second

	return poolCfg, nil
}

// NewPool opens the pool and pings it, so a broken deployment fails at startup
// rather than on the first message. It returns an error, never panics.
func NewPool(ctx context.Context, cfg config.PostgresConfig) (*pgxpool.Pool, error) {
	poolCfg, err := poolConfig(cfg)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %s", redactError(err, cfg))
	}

	pingCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.ConnectTimeoutSeconds)*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %s", redactError(err, cfg))
	}

	return pool, nil
}

// redactError removes the password from an error message. url.Parse-based
// redaction does not work on free text, so the known secret is replaced.
func redactError(err error, cfg config.PostgresConfig) string {
	message := err.Error()
	if cfg.Password != "" {
		message = strings.ReplaceAll(message, cfg.Password, redactedPasswordMask)
		message = strings.ReplaceAll(message, url.QueryEscape(cfg.Password), redactedPasswordMask)
	}
	if parsed, parseErr := url.Parse(cfg.DSN); parseErr == nil && parsed.User != nil {
		if password, ok := parsed.User.Password(); ok && password != "" {
			message = strings.ReplaceAll(message, password, redactedPasswordMask)
		}
	}

	return message
}
