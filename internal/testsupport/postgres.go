// Package testsupport holds helpers that only tests import.
package testsupport

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	pgDatabase = "history_test"
	pgUser     = "test_user"
	pgPassword = "test_password"
)

// Label marks every container the tests start, so a leak can be found with
// docker ps --filter label=conv-history-it and removed.
const Label = "conv-history-it"

// Postgres is a throwaway Postgres 18. It keeps one host port and its data, so
// it can be stopped and started again under the same DSN, which is how a test
// simulates a database outage.
type Postgres struct {
	// DSN connects to it from the host, and stays valid across Stop and Start.
	DSN string
	// User, Password and Database are what a container on the same network connects with.
	User, Password, Database string
	id                       string
}

// PostgresOption changes how a throwaway Postgres is started.
type PostgresOption func(*postgresSettings)

type postgresSettings struct {
	args        []string
	network     string
	aliases     []string
	memoryBytes int64
}

// WithArgs replaces the server's command line, for example "postgres", "-c", "wal_level=logical".
func WithArgs(args ...string) PostgresOption {
	return func(s *postgresSettings) { s.args = args }
}

// OnNetwork attaches the container to a Docker network under the given aliases, so other
// containers of the test reach it by name.
func OnNetwork(name string, aliases ...string) PostgresOption {
	return func(s *postgresSettings) { s.network, s.aliases = name, aliases }
}

// WithMemory caps the memory of the container, so a test cannot starve the machine it runs on.
func WithMemory(bytes int64) PostgresOption {
	return func(s *postgresSettings) { s.memoryBytes = bytes }
}

// RequireDocker skips the test when the Docker daemon is not reachable. It also points
// testcontainers at a Colima or Docker Desktop socket when DOCKER_HOST is unset.
func RequireDocker(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if !dockerRunning(ctx) {
		t.Skip("Docker daemon is not running or reachable")
	}
}

// StartPostgres starts a throwaway Postgres 18 on a host port of its own. The test
// is skipped when Docker is not reachable, and the container is removed on cleanup.
func StartPostgres(t *testing.T, opts ...PostgresOption) *Postgres {
	t.Helper()

	RequireDocker(t)

	pg, terminate, err := RunPostgres(context.Background(), opts...)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(terminate)

	return pg
}

// RunPostgres starts a throwaway Postgres and returns it with the function that removes it. Unlike
// StartPostgres it is tied to no test, so a fixture shared by several tests can own it.
func RunPostgres(ctx context.Context, opts ...PostgresOption) (*Postgres, func(), error) {
	var settings postgresSettings
	for _, opt := range opts {
		opt(&settings)
	}

	port, err := FreePort()
	if err != nil {
		return nil, nil, err
	}

	req := testcontainers.ContainerRequest{
		Image:        "postgres:18-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Cmd:          settings.args,
		Labels:       map[string]string{Label: "postgres"},
		Env: map[string]string{
			"POSTGRES_DB":       pgDatabase,
			"POSTGRES_USER":     pgUser,
			"POSTGRES_PASSWORD": pgPassword,
		},
		// A fixed host port: a stopped container is given a new random one when
		// it starts again, and the consumer under test holds the old DSN.
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{"5432/tcp": []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: strconv.Itoa(port)}}}
			hc.Memory = settings.memoryBytes
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}
	if settings.network != "" {
		req.Networks = []string{settings.network}
		req.NetworkAliases = map[string][]string{settings.network: settings.aliases}
	}

	started, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, nil, err
	}
	terminate := func() { _ = started.Terminate(context.Background()) }

	return &Postgres{
		DSN:      fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", pgUser, pgPassword, port, pgDatabase),
		User:     pgUser,
		Password: pgPassword,
		Database: pgDatabase,
		id:       started.GetContainerID(),
	}, terminate, nil
}

// PostgresDSN starts a throwaway Postgres and returns its DSN.
func PostgresDSN(t *testing.T) string {
	t.Helper()

	return StartPostgres(t).DSN
}

// Stop stops the database, as an outage would. Its data is kept.
func (p *Postgres) Stop(t *testing.T) {
	t.Helper()

	if out, err := exec.Command("docker", "stop", p.id).CombinedOutput(); err != nil {
		t.Fatalf("stop postgres: %v: %s", err, out)
	}
}

// Start starts the stopped database again and returns once it accepts connections.
func (p *Postgres) Start(t *testing.T) {
	t.Helper()

	if out, err := exec.Command("docker", "start", p.id).CombinedOutput(); err != nil {
		t.Fatalf("start postgres: %v: %s", err, out)
	}

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if exec.Command("docker", "exec", p.id, "pg_isready", "-U", pgUser, "-d", pgDatabase).Run() == nil {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("postgres did not accept connections after start")
}

// FreePort asks the system for a port nobody holds. A test uses it where a container needs a host
// port that must be known before the container starts, and never a fixed one.
func FreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find a free port: %w", err)
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

// dockerRunning reports whether the daemon answers. It also points
// testcontainers at a Colima or Docker Desktop socket when DOCKER_HOST is unset.
func dockerRunning(ctx context.Context) bool {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

	if os.Getenv("DOCKER_HOST") == "" {
		if home, err := os.UserHomeDir(); err == nil {
			for _, socket := range []string{
				filepath.Join(home, ".colima", "default", "docker.sock"),
				filepath.Join(home, ".docker", "run", "docker.sock"),
				"/var/run/docker.sock",
			} {
				if _, err := os.Stat(socket); err == nil {
					_ = os.Setenv("DOCKER_HOST", "unix://"+socket)
					break
				}
			}
		}
	}

	return exec.CommandContext(ctx, "docker", "info").Run() == nil
}
