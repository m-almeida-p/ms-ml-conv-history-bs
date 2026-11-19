package postgresql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/config"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/testsupport"
)

func validConfig() config.PostgresConfig {
	return config.PostgresConfig{
		Host: "db", Port: 5432, User: "app", Password: "p@ss/word:1", Database: "history", SSLMode: "disable",
		MaxConns: 10, MinConns: 2, ConnMaxLifetimeSeconds: 3600, ConnMaxIdleTimeSeconds: 300, ConnectTimeoutSeconds: 10,
	}
}

func TestBuildDSN(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*config.PostgresConfig)
		want   string
	}{
		{
			name:   "discreteFieldsEscapeThePassword",
			mutate: func(*config.PostgresConfig) {},
			want:   "postgres://app:p%40ss%2Fword%3A1@db:5432/history?connect_timeout=10&sslmode=disable",
		},
		{
			name:   "noPasswordKeepsTheUser",
			mutate: func(c *config.PostgresConfig) { c.Password = "" },
			want:   "postgres://app@db:5432/history?connect_timeout=10&sslmode=disable",
		},
		{
			name:   "zeroConnectTimeoutIsOmitted",
			mutate: func(c *config.PostgresConfig) { c.ConnectTimeoutSeconds = 0 },
			want:   "postgres://app:p%40ss%2Fword%3A1@db:5432/history?sslmode=disable",
		},
		{
			name:   "searchPathNamesTheSchemaTheTablesAreIn",
			mutate: func(c *config.PostgresConfig) { c.SearchPath = "history" },
			want:   "postgres://app:p%40ss%2Fword%3A1@db:5432/history?connect_timeout=10&search_path=history&sslmode=disable",
		},
		{
			name: "searchPathIsLeftToTheDsnWhenThereIsOne",
			mutate: func(c *config.PostgresConfig) {
				c.DSN = "postgres://other/db?search_path=history"
				c.SearchPath = "ignored"
			},
			want: "postgres://other/db?search_path=history",
		},
		{
			name:   "dsnWinsAndIsTrimmed",
			mutate: func(c *config.PostgresConfig) { c.DSN = "  postgres://other/db  " },
			want:   "postgres://other/db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)

			assert.Equal(t, tt.want, BuildDSN(cfg))
		})
	}
}

func TestRedactDSN(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{name: "empty", dsn: "  ", want: ""},
		{name: "passwordIsMasked", dsn: "postgres://app:secret@db:5432/history", want: "postgres://app:xxxxx@db:5432/history"},
		{name: "noPasswordUnchanged", dsn: "postgres://app@db:5432/history", want: "postgres://app@db:5432/history"},
		{name: "noUserUnchanged", dsn: "postgres://db:5432/history", want: "postgres://db:5432/history"},
		{name: "unparseableIsDropped", dsn: "postgres://app:secret@db:notaport/history", want: "<redacted>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RedactDSN(tt.dsn)

			assert.Equal(t, tt.want, got)
			assert.NotContains(t, got, "secret")
		})
	}
}

func TestPoolConfig(t *testing.T) {
	tests := []struct {
		name         string
		mutate       func(*config.PostgresConfig)
		wantErr      bool
		wantMax      int32
		wantMin      int32
		wantLifetime time.Duration
		wantIdle     time.Duration
	}{
		{name: "limitsAreCopied", mutate: func(*config.PostgresConfig) {}, wantMax: 10, wantMin: 2, wantLifetime: time.Hour, wantIdle: 5 * time.Minute},
		{
			name:     "zeroLifetimeMeansUnlimited",
			mutate:   func(c *config.PostgresConfig) { c.ConnMaxLifetimeSeconds, c.ConnMaxIdleTimeSeconds = 0, 0 },
			wantMax:  10,
			wantMin:  2,
			wantIdle: 0,
		},
		{
			name:    "badDSNIsRedacted",
			mutate:  func(c *config.PostgresConfig) { c.DSN = "postgres://app:topsecret@db:notaport/history" },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)

			got, err := poolConfig(cfg)

			if tt.wantErr {
				require.Error(t, err)
				assert.NotContains(t, err.Error(), "topsecret")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantMax, got.MaxConns)
			assert.Equal(t, tt.wantMin, got.MinConns)
			assert.Equal(t, tt.wantLifetime, got.MaxConnLifetime)
			assert.Equal(t, tt.wantIdle, got.MaxConnIdleTime)
		})
	}
}

func TestRedactError(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*config.PostgresConfig)
		err    string
	}{
		{name: "passwordInMessage", mutate: func(*config.PostgresConfig) {}, err: "dial failed for p@ss/word:1"},
		{name: "escapedPasswordInMessage", mutate: func(*config.PostgresConfig) {}, err: "dsn postgres://app:p%40ss%2Fword%3A1@db"},
		{
			name:   "dsnPasswordInMessage",
			mutate: func(c *config.PostgresConfig) { c.Password, c.DSN = "", "postgres://app:fromdsn@db/history" },
			err:    "cannot reach fromdsn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(&cfg)

			got := redactError(assert.AnError, cfg)
			assert.NotEmpty(t, got)

			got = redactError(errString(tt.err), cfg)
			assert.NotContains(t, got, "p@ss/word:1")
			assert.NotContains(t, got, "p%40ss%2Fword%3A1")
			assert.NotContains(t, got, "fromdsn")
			assert.Contains(t, got, redactedPasswordMask)
		})
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestNewPool(t *testing.T) {
	dsn := testsupport.PostgresDSN(t)

	tests := []struct {
		name      string
		cfg       func() config.PostgresConfig
		wantErr   string
		wantNoPwd string
	}{
		{
			name: "connectsAndPings",
			cfg: func() config.PostgresConfig {
				cfg := validConfig()
				cfg.DSN = dsn

				return cfg
			},
		},
		{
			name: "unreachableServerFailsThePing",
			cfg: func() config.PostgresConfig {
				cfg := validConfig()
				cfg.Host, cfg.Port, cfg.ConnectTimeoutSeconds = "127.0.0.1", 1, 2

				return cfg
			},
			wantErr:   "ping postgres",
			wantNoPwd: "p@ss/word:1",
		},
		{
			name: "badDSNFailsTheParse",
			cfg: func() config.PostgresConfig {
				cfg := validConfig()
				cfg.DSN = "postgres://app:topsecret@db:notaport/history"

				return cfg
			},
			wantErr:   "parse postgres dsn",
			wantNoPwd: "topsecret",
		},
		{
			name: "invalidPoolLimitsFailTheCreate",
			cfg: func() config.PostgresConfig {
				cfg := validConfig()
				cfg.DSN = dsn
				cfg.MaxConns = 0

				return cfg
			},
			wantErr: "create postgres pool",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := NewPool(context.Background(), tt.cfg())

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				if tt.wantNoPwd != "" {
					assert.NotContains(t, err.Error(), tt.wantNoPwd)
				}
				assert.Nil(t, pool)
				return
			}

			require.NoError(t, err)
			defer pool.Close()

			var one int
			require.NoError(t, pool.QueryRow(context.Background(), "SELECT 1").Scan(&one))
			assert.Equal(t, 1, one)

			var db DB = pool
			assert.NotNil(t, db)
		})
	}
}
