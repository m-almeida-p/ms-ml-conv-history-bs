// Package config loads config.yaml, expanding ${VAR:-default} from the
// environment, and rejects unusable settings at startup.
package config

import (
	"cmp"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const defaultPath = "config.yaml"

var (
	supportedSSLModes     = []string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
	supportedOffsetResets = []string{"earliest", "latest"}
	supportedLogLevels    = []string{"debug", "info", "warn", "error"}
)

// Config is the whole service configuration.
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	MCP      MCPConfig      `yaml:"mcp"`
	Postgres PostgresConfig `yaml:"postgres"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	Logging  LoggingConfig  `yaml:"logging"`
}

// ServerConfig configures the HTTP reader.
type ServerConfig struct {
	Port int `yaml:"port"`
}

// defaultMCPHost is where the MCP server listens when no host is set. The server has no
// authentication and lists accounts, so it stays on the loopback interface unless told otherwise.
const defaultMCPHost = "127.0.0.1"

// MCPConfig configures the MCP server. A Port of 0 means the section is absent, which only
// cmd/mcp treats as an error: the API and the consumer do not read it.
type MCPConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

// Addr is the address the MCP server listens on: the host, or the loopback interface when none is set.
func (c MCPConfig) Addr() string {
	return net.JoinHostPort(cmp.Or(c.Host, defaultMCPHost), strconv.Itoa(c.Port))
}

// PostgresConfig configures the pool. DSN, when set, wins over the discrete
// connection fields.
type PostgresConfig struct {
	DSN      string `yaml:"dsn"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"ssl_mode"`
	// SearchPath is the schema (or comma-separated schemas) the unqualified table names are read from. Empty
	// keeps the database user's own search_path. With a DSN, put search_path in the DSN instead.
	SearchPath             string `yaml:"search_path"`
	MaxConns               int    `yaml:"max_conns"`
	MinConns               int    `yaml:"min_conns"`
	ConnMaxLifetimeSeconds int    `yaml:"conn_max_lifetime_seconds"`
	ConnMaxIdleTimeSeconds int    `yaml:"conn_max_idle_time_seconds"`
	ConnectTimeoutSeconds  int    `yaml:"connect_timeout_seconds"`
}

// KafkaConfig configures the consumer. Brokers is a comma-separated list.
//
// MaxPollIntervalMs is how long the consumer may go without polling before the
// broker takes its partitions away. A store that keeps failing blocks polling (the
// event is retried in place), so this bounds how long an outage the consumer rides
// out without leaving the group. SessionTimeoutMs is how long the broker waits for a
// heartbeat; the broker enforces its own minimum (6000 by default).
type KafkaConfig struct {
	Brokers           string `yaml:"brokers"`
	Topic             string `yaml:"topic"`
	GroupID           string `yaml:"group_id"`
	AutoOffsetReset   string `yaml:"auto_offset_reset"`
	MaxPollIntervalMs int    `yaml:"max_poll_interval_ms"`
	SessionTimeoutMs  int    `yaml:"session_timeout_ms"`
}

// Warnings lists settings that are valid but risky, for the caller to log at
// startup.
func (k KafkaConfig) Warnings() []string {
	var warnings []string
	if k.AutoOffsetReset == "latest" {
		warnings = append(warnings, "kafka.auto_offset_reset is latest: a group with no committed offset (a new group, or offsets that expired) starts at the end of the topic and skips the history before it; the inbox is the only durable copy, so use earliest unless that is the intent")
	}

	return warnings
}

// LoggingConfig configures lib-go-log.
type LoggingConfig struct {
	Level string `yaml:"level"`
}

// Load reads the YAML at path (config.yaml when empty), expands the environment
// and validates the result.
//
// The file is parsed first and ${VAR:-default} is expanded in the parsed values.
// Pasting the variables into the text before parsing would let a value change the
// file: a password with a "#", a quote, a leading "*" or "&", or a newline would be
// cut short, read as something else, or break the load.
func Load(path string) (*Config, error) {
	if path == "" {
		path = defaultPath
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	expandNode(&doc)

	var cfg Config
	if doc.Kind != 0 {
		if err := doc.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
	}

	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

// expandNode expands ${VAR:-default} in every scalar of the tree.
func expandNode(n *yaml.Node) {
	if n.Kind == yaml.ScalarNode && strings.Contains(n.Value, "$") {
		if expanded := expandEnvWithDefaults(n.Value); expanded != n.Value {
			n.Value = expanded
			switch {
			case isNullLiteral(expanded):
				// The text "null" is a value, not an absent one.
				n.Style, n.Tag = yaml.DoubleQuotedStyle, "!!str"
			case n.Style == 0:
				// An unquoted scalar is typed again from its new text: "3100" is
				// an int, "pa #ss" stays a string.
				n.Tag = ""
			}
		}
	}

	for _, child := range n.Content {
		expandNode(child)
	}
}

func isNullLiteral(s string) bool {
	switch s {
	case "null", "Null", "NULL", "~":
		return true
	}

	return false
}

// expandEnvWithDefaults expands ${VAR} and ${VAR:-default}. A variable that is
// unset or empty takes the default.
func expandEnvWithDefaults(s string) string {
	return os.Expand(s, func(key string) string {
		if idx := strings.Index(key, ":-"); idx > 0 {
			if value := os.Getenv(strings.TrimSpace(key[:idx])); value != "" {
				return value
			}

			return strings.TrimSpace(key[idx+2:])
		}

		return os.Getenv(key)
	})
}

func validate(cfg Config) error {
	if err := validateServer(cfg.Server); err != nil {
		return err
	}
	if err := validateMCP(cfg.MCP); err != nil {
		return err
	}
	if err := validatePostgres(cfg.Postgres); err != nil {
		return err
	}
	if err := validateKafka(cfg.Kafka); err != nil {
		return err
	}

	if !slices.Contains(supportedLogLevels, cfg.Logging.Level) {
		return fmt.Errorf("logging.level must be one of %s", strings.Join(supportedLogLevels, ", "))
	}

	return nil
}

func validateServer(cfg ServerConfig) error {
	if cfg.Port < 1 || cfg.Port > 65535 {
		return fmt.Errorf("server.port must be between 1 and 65535")
	}

	return nil
}

func validateMCP(cfg MCPConfig) error {
	if cfg.Port < 0 || cfg.Port > 65535 {
		return fmt.Errorf("mcp.port must be between 1 and 65535")
	}

	return nil
}

func validatePostgres(cfg PostgresConfig) error {
	if strings.TrimSpace(cfg.DSN) == "" {
		if strings.TrimSpace(cfg.Host) == "" {
			return fmt.Errorf("postgres.host is required without a dsn")
		}
		if cfg.Port < 1 || cfg.Port > 65535 {
			return fmt.Errorf("postgres.port must be between 1 and 65535")
		}
		if strings.TrimSpace(cfg.User) == "" {
			return fmt.Errorf("postgres.user is required without a dsn")
		}
		if strings.TrimSpace(cfg.Database) == "" {
			return fmt.Errorf("postgres.database is required without a dsn")
		}
		if !slices.Contains(supportedSSLModes, cfg.SSLMode) {
			return fmt.Errorf("postgres.ssl_mode must be one of %s", strings.Join(supportedSSLModes, ", "))
		}
	}

	if cfg.MaxConns < 1 {
		return fmt.Errorf("postgres.max_conns must be greater than 0")
	}
	if cfg.MinConns < 0 {
		return fmt.Errorf("postgres.min_conns must be greater than or equal to 0")
	}
	if cfg.MinConns > cfg.MaxConns {
		return fmt.Errorf("postgres.min_conns must not exceed postgres.max_conns")
	}
	if cfg.ConnectTimeoutSeconds < 1 {
		return fmt.Errorf("postgres.connect_timeout_seconds must be greater than 0")
	}
	if cfg.ConnMaxLifetimeSeconds < 0 {
		return fmt.Errorf("postgres.conn_max_lifetime_seconds must be greater than or equal to 0")
	}
	if cfg.ConnMaxIdleTimeSeconds < 0 {
		return fmt.Errorf("postgres.conn_max_idle_time_seconds must be greater than or equal to 0")
	}

	return nil
}

func validateKafka(cfg KafkaConfig) error {
	if strings.TrimSpace(cfg.Brokers) == "" {
		return fmt.Errorf("kafka.brokers is required")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return fmt.Errorf("kafka.topic is required")
	}
	if strings.TrimSpace(cfg.GroupID) == "" {
		return fmt.Errorf("kafka.group_id is required")
	}
	if !slices.Contains(supportedOffsetResets, cfg.AutoOffsetReset) {
		return fmt.Errorf("kafka.auto_offset_reset must be one of %s", strings.Join(supportedOffsetResets, ", "))
	}
	if cfg.SessionTimeoutMs < 1000 {
		return fmt.Errorf("kafka.session_timeout_ms must be at least 1000")
	}
	if cfg.MaxPollIntervalMs < cfg.SessionTimeoutMs {
		return fmt.Errorf("kafka.max_poll_interval_ms must not be below kafka.session_timeout_ms")
	}

	return nil
}
