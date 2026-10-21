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

// Postgres is a throwaway Postgres 18. It keeps one host port and its data, so
// it can be stopped and started again under the same DSN, which is how a test
// simulates a database outage.
type Postgres struct {
	// DSN connects to it, and stays valid across Stop and Start.
	DSN string
	id  string
}

// StartPostgres starts a throwaway Postgres 18 on a host port of its own. The test
// is skipped when Docker is not reachable, and the container is removed on cleanup.
func StartPostgres(t *testing.T) *Postgres {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if !dockerRunning(ctx) {
		t.Skip("Docker daemon is not running or reachable")
	}

	port := freePort(t)

	started, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:18-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_DB":       pgDatabase,
				"POSTGRES_USER":     pgUser,
				"POSTGRES_PASSWORD": pgPassword,
			},
			// A fixed host port: a stopped container is given a new random one when
			// it starts again, and the consumer under test holds the old DSN.
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = nat.PortMap{"5432/tcp": []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: strconv.Itoa(port)}}}
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := started.Terminate(context.Background()); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})

	return &Postgres{
		DSN: fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", pgUser, pgPassword, port, pgDatabase),
		id:  started.GetContainerID(),
	}
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

// freePort asks the system for a port nobody holds.
func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find a free port: %v", err)
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
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
