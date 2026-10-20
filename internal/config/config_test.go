package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const repoConfig = "../../config.yaml"

// envKeys are every variable config.yaml reads, cleared so the developer's shell
// cannot leak into a case.
var envKeys = []string{
	"SERVER_PORT",
	"POSTGRES_DSN", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD",
	"POSTGRES_DB", "POSTGRES_SSL_MODE", "POSTGRES_MAX_CONNS", "POSTGRES_MIN_CONNS",
	"POSTGRES_CONN_MAX_LIFETIME", "POSTGRES_CONN_MAX_IDLE_TIME", "POSTGRES_CONNECT_TIMEOUT",
	"KAFKA_CLIENT_BROKERS", "KAFKA_TOPIC_EVENTS", "KAFKA_GROUP_ID", "KAFKA_AUTO_OFFSET_RESET",
	"LOG_LEVEL",
}

func clearEnv(t *testing.T) {
	t.Helper()

	for _, key := range envKeys {
		t.Setenv(key, "")
	}
}

func TestExpandEnvWithDefaults(t *testing.T) {
	tests := []struct {
		name  string
		input string
		env   map[string]string
		want  string
	}{
		{name: "unsetUsesDefault", input: "${SPEC_A:-fallback}", want: "fallback"},
		{name: "setWins", input: "${SPEC_A:-fallback}", env: map[string]string{"SPEC_A": "value"}, want: "value"},
		{name: "emptyUsesDefault", input: "${SPEC_A:-fallback}", env: map[string]string{"SPEC_A": ""}, want: "fallback"},
		{name: "emptyDefault", input: "x${SPEC_A:-}y", want: "xy"},
		{name: "defaultIsTrimmed", input: "${SPEC_A:-  padded  }", want: "padded"},
		{name: "plainVariable", input: "${SPEC_B}", env: map[string]string{"SPEC_B": "plain"}, want: "plain"},
		{name: "plainVariableUnset", input: "[${SPEC_B}]", want: "[]"},
		{name: "textWithoutVariables", input: "port: 3100", want: "port: 3100"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SPEC_A", "")
			t.Setenv("SPEC_B", "")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			assert.Equal(t, tt.want, expandEnvWithDefaults(tt.input))
		})
	}
}

func TestLoad(t *testing.T) {
	defaults := Config{
		Server: ServerConfig{Port: 3100},
		Postgres: PostgresConfig{
			Host: "localhost", Port: 5432, User: "postgres", Database: "postgres", SSLMode: "disable",
			MaxConns: 10, MinConns: 2, ConnMaxLifetimeSeconds: 3600, ConnMaxIdleTimeSeconds: 300, ConnectTimeoutSeconds: 10,
		},
		Kafka: KafkaConfig{
			Brokers: "localhost:9092", Topic: "yape.public.conversation_events",
			GroupID: "ms-ml-conv-history-bs", AutoOffsetReset: "earliest",
			MaxPollIntervalMs: 300000, SessionTimeoutMs: 45000,
		},
		Logging: LoggingConfig{Level: "info"},
	}

	tests := []struct {
		name    string
		env     map[string]string
		want    func(Config) Config
		wantErr string
	}{
		{name: "defaults", want: func(c Config) Config { return c }},
		{
			name: "environmentOverrides",
			env: map[string]string{
				"SERVER_PORT": "8080", "POSTGRES_HOST": "db", "POSTGRES_PASSWORD": "secret", "POSTGRES_MAX_CONNS": "20",
				"KAFKA_CLIENT_BROKERS": "kafka:29092", "KAFKA_AUTO_OFFSET_RESET": "latest", "LOG_LEVEL": "debug",
				"KAFKA_MAX_POLL_INTERVAL_MS": "20000", "KAFKA_SESSION_TIMEOUT_MS": "6000",
			},
			want: func(c Config) Config {
				c.Server.Port = 8080
				c.Postgres.Host = "db"
				c.Postgres.Password = "secret"
				c.Postgres.MaxConns = 20
				c.Kafka.Brokers = "kafka:29092"
				c.Kafka.AutoOffsetReset = "latest"
				c.Kafka.MaxPollIntervalMs = 20000
				c.Kafka.SessionTimeoutMs = 6000
				c.Logging.Level = "debug"
				return c
			},
		},
		{
			name: "dsnReplacesDiscreteFieldChecks",
			env:  map[string]string{"POSTGRES_DSN": "postgres://u:p@h:5432/d", "POSTGRES_SSL_MODE": "bogus"},
			want: func(c Config) Config {
				c.Postgres.DSN = "postgres://u:p@h:5432/d"
				c.Postgres.SSLMode = "bogus"
				return c
			},
		},
		{name: "serverPortZero", env: map[string]string{"SERVER_PORT": "0"}, wantErr: "server.port"},
		{name: "serverPortTooHigh", env: map[string]string{"SERVER_PORT": "70000"}, wantErr: "server.port"},
		{name: "postgresPortZero", env: map[string]string{"POSTGRES_PORT": "0"}, wantErr: "postgres.port"},
		{name: "postgresSSLModeUnknown", env: map[string]string{"POSTGRES_SSL_MODE": "bogus"}, wantErr: "postgres.ssl_mode"},
		{name: "postgresMaxConnsZero", env: map[string]string{"POSTGRES_MAX_CONNS": "0"}, wantErr: "postgres.max_conns"},
		{name: "postgresMinConnsNegative", env: map[string]string{"POSTGRES_MIN_CONNS": "-1"}, wantErr: "postgres.min_conns must be greater"},
		{name: "postgresMinAboveMax", env: map[string]string{"POSTGRES_MIN_CONNS": "11"}, wantErr: "must not exceed"},
		{name: "postgresConnectTimeoutZero", env: map[string]string{"POSTGRES_CONNECT_TIMEOUT": "0"}, wantErr: "connect_timeout_seconds"},
		{name: "postgresLifetimeNegative", env: map[string]string{"POSTGRES_CONN_MAX_LIFETIME": "-1"}, wantErr: "conn_max_lifetime_seconds"},
		{name: "postgresIdleTimeNegative", env: map[string]string{"POSTGRES_CONN_MAX_IDLE_TIME": "-1"}, wantErr: "conn_max_idle_time_seconds"},
		{name: "kafkaOffsetResetUnknown", env: map[string]string{"KAFKA_AUTO_OFFSET_RESET": "middle"}, wantErr: "kafka.auto_offset_reset"},
		{name: "kafkaSessionTimeoutBelowOneSecond", env: map[string]string{"KAFKA_SESSION_TIMEOUT_MS": "500"}, wantErr: "kafka.session_timeout_ms"},
		{name: "kafkaSessionTimeoutNegative", env: map[string]string{"KAFKA_SESSION_TIMEOUT_MS": "-1"}, wantErr: "kafka.session_timeout_ms"},
		{name: "kafkaPollIntervalBelowTheSessionTimeout", env: map[string]string{"KAFKA_SESSION_TIMEOUT_MS": "20000", "KAFKA_MAX_POLL_INTERVAL_MS": "10000"}, wantErr: "kafka.max_poll_interval_ms"},
		{name: "kafkaPollIntervalEqualToTheSessionTimeoutIsAllowed", env: map[string]string{"KAFKA_SESSION_TIMEOUT_MS": "10000", "KAFKA_MAX_POLL_INTERVAL_MS": "10000"}, want: func(c Config) Config {
			c.Kafka.SessionTimeoutMs, c.Kafka.MaxPollIntervalMs = 10000, 10000
			return c
		}},
		{name: "logLevelUnknown", env: map[string]string{"LOG_LEVEL": "loud"}, wantErr: "logging.level"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			got, err := Load(repoConfig)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, got)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want(defaults), *got)
		})
	}
}

func TestLoadFromContent(t *testing.T) {
	const validKafka = "kafka:\n  brokers: b:9092\n  topic: t\n  group_id: g\n  auto_offset_reset: earliest\n  max_poll_interval_ms: 300000\n  session_timeout_ms: 45000\nlogging:\n  level: info\n"

	tests := []struct {
		name    string
		content string
		missing bool
		wantErr string
	}{
		{name: "fileMissing", missing: true, wantErr: "read config"},
		{name: "yamlInvalid", content: "server: [unclosed", wantErr: "parse config"},
		{name: "noDSNNoHost", content: "server:\n  port: 1\npostgres:\n  port: 5432\n" + validKafka, wantErr: "postgres.host"},
		{
			name:    "noDSNNoUser",
			content: "server:\n  port: 1\npostgres:\n  host: h\n  port: 5432\n" + validKafka,
			wantErr: "postgres.user",
		},
		{
			name:    "noDSNNoDatabase",
			content: "server:\n  port: 1\npostgres:\n  host: h\n  port: 5432\n  user: u\n" + validKafka,
			wantErr: "postgres.database",
		},
		{
			name:    "kafkaBrokersMissing",
			content: "server:\n  port: 1\npostgres:\n  dsn: postgres://h/d\n  max_conns: 1\n  connect_timeout_seconds: 1\nkafka:\n  topic: t\n  group_id: g\n  auto_offset_reset: earliest\n  max_poll_interval_ms: 300000\n  session_timeout_ms: 45000\nlogging:\n  level: info\n",
			wantErr: "kafka.brokers",
		},
		{
			name:    "kafkaTopicMissing",
			content: "server:\n  port: 1\npostgres:\n  dsn: postgres://h/d\n  max_conns: 1\n  connect_timeout_seconds: 1\nkafka:\n  brokers: b\n  group_id: g\n  auto_offset_reset: earliest\n  max_poll_interval_ms: 300000\n  session_timeout_ms: 45000\nlogging:\n  level: info\n",
			wantErr: "kafka.topic",
		},
		{
			name:    "kafkaGroupIDMissing",
			content: "server:\n  port: 1\npostgres:\n  dsn: postgres://h/d\n  max_conns: 1\n  connect_timeout_seconds: 1\nkafka:\n  brokers: b\n  topic: t\n  auto_offset_reset: earliest\n  max_poll_interval_ms: 300000\n  session_timeout_ms: 45000\nlogging:\n  level: info\n",
			wantErr: "kafka.group_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if !tt.missing {
				require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))
			}

			got, err := Load(path)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Nil(t, got)
		})
	}
}

func TestLoadDefaultPath(t *testing.T) {
	tests := []struct {
		name    string
		dir     string
		wantErr bool
	}{
		{name: "repoRootHasConfig", dir: "../..", wantErr: false},
		{name: "emptyDirHasNone", dir: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			dir := tt.dir
			if dir == "" {
				dir = t.TempDir()
			}
			t.Chdir(dir)

			got, err := Load("")

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, 3100, got.Server.Port)
		})
	}
}

// TestLoadTakesEnvironmentValuesAsTheyAre: a value is data, never YAML. Each one
// must come back exactly as it was set.
func TestLoadTakesEnvironmentValuesAsTheyAre(t *testing.T) {
	values := []string{
		"plain", "pa ss", "  leading and trailing  ", "123", "true", "false", "null", "Null", "~", "0x1f", "1e3", "-x", "- x", "? x",
		"*secret", "&anchor", "!x", "!!str x", "a: b", "key: value", "pa #ss", "# all comment", "{x}", "[x]", "{a: 1}", "'q'", `"dq"`, `a"b`,
		"%x", "@x", "`x", "|", ">", "a\tb", "p\nq", "line1\nline2: x", "a$b", "${HOME}", "$(whoami)", "ñandú ✓ 日本語", "postgres://u:p%23ss@h:5432/d?x=a#b",
		strings.Repeat("x", 4096),
	}

	for _, want := range values {
		t.Run(fmt.Sprintf("%.24q", want), func(t *testing.T) {
			clearEnv(t)
			t.Setenv("POSTGRES_PASSWORD", want)
			t.Setenv("POSTGRES_DSN", want)
			t.Setenv("KAFKA_GROUP_ID", want)

			got, err := Load(repoConfig)

			require.NoError(t, err)
			assert.Equal(t, want, got.Postgres.Password)
			assert.Equal(t, want, got.Postgres.DSN)
			assert.Equal(t, want, got.Kafka.GroupID)
		})
	}
}

func TestLoadKeepsNumbersTypedAndRejectsNonsense(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		want    int
		wantErr string
	}{
		{name: "number", env: map[string]string{"SERVER_PORT": "8080"}, want: 8080},
		{name: "numberWithSpaces", env: map[string]string{"SERVER_PORT": " 8081 "}, wantErr: "cannot unmarshal"},
		{name: "text", env: map[string]string{"SERVER_PORT": "abc"}, wantErr: "cannot unmarshal"},
		{name: "nullText", env: map[string]string{"SERVER_PORT": "null"}, wantErr: "cannot unmarshal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			got, err := Load(repoConfig)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Server.Port)
		})
	}
}

func TestLoadAnEmptyOrNonMappingFile(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{name: "emptyFile", content: "", wantErr: "server.port"},
		{name: "onlyComments", content: "# nothing here\n", wantErr: "server.port"},
		{name: "aListInsteadOfAMapping", content: "- a\n- b\n", wantErr: "parse config"},
		{name: "aBareScalar", content: "hello\n", wantErr: "parse config"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tt.content), 0o600))

			got, err := Load(path)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Nil(t, got)
		})
	}
}

func TestLoadDefaultsAreSubjectToTheSameRules(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := "server:\n  port: ${SPEC_PORT:-3100}\npostgres:\n  dsn: ${SPEC_DSN:-postgres://u:p@h/d?x=a#b}\n  max_conns: 1\n  connect_timeout_seconds: 1\n" +
		"kafka:\n  brokers: b:9092\n  topic: ${SPEC_TOPIC:-null}\n  group_id: g\n  auto_offset_reset: earliest\n  max_poll_interval_ms: 300000\n  session_timeout_ms: 45000\nlogging:\n  level: info\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	t.Setenv("SPEC_PORT", "")
	t.Setenv("SPEC_DSN", "")
	t.Setenv("SPEC_TOPIC", "")

	got, err := Load(path)

	require.NoError(t, err)
	assert.Equal(t, 3100, got.Server.Port)
	assert.Equal(t, "postgres://u:p@h/d?x=a#b", got.Postgres.DSN, "a default with a # is not a comment")
	assert.Equal(t, "null", got.Kafka.Topic, "a default of null is the text null")
}

// auto.offset.reset=latest is allowed, for starting at the end of a topic on
// purpose, but it silently skips history when committed offsets are lost, and the
// inbox is the only durable copy. The consumer says so at startup.
func TestKafkaWarnings(t *testing.T) {
	tests := []struct {
		name   string
		reset  string
		wantIn string // empty: no warning
	}{
		{name: "earliestIsSilent", reset: "earliest"},
		{name: "latestWarns", reset: "latest", wantIn: "kafka.auto_offset_reset is latest"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warnings := KafkaConfig{AutoOffsetReset: tt.reset}.Warnings()

			if tt.wantIn == "" {
				assert.Empty(t, warnings)
				return
			}
			require.Len(t, warnings, 1)
			assert.Contains(t, warnings[0], tt.wantIn)
			assert.Contains(t, warnings[0], "history")
		})
	}
}
