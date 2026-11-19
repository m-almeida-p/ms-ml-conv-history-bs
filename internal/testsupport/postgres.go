// Package testsupport holds helpers that only tests import.
package testsupport

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	pgDatabase = "history_test"
	pgUser     = "test_user"
	pgPassword = "test_password"
	// pgMemory caps the container, so a heavy test cannot take the machine.
	pgMemory = 512 << 20
)

// Label marks every container the tests start, so a leak can be found with
// docker ps --filter label=conv-history-it and removed.
const Label = "conv-history-it"

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

// PostgresDSN starts a throwaway Postgres 18 in a container (testcontainers) on a host port of its own and returns the DSN
// that reaches it. The test is skipped when Docker is not reachable, and the container is removed on cleanup.
func PostgresDSN(t *testing.T) string {
	t.Helper()

	RequireDocker(t)
	ctx := context.Background()

	started, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:18-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Labels:       map[string]string{Label: "postgres"},
			Env: map[string]string{
				"POSTGRES_DB":       pgDatabase,
				"POSTGRES_USER":     pgUser,
				"POSTGRES_PASSWORD": pgPassword,
			},
			HostConfigModifier: func(hc *container.HostConfig) { hc.Memory = pgMemory },
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = started.Terminate(context.Background()) })

	host, err := started.Host(ctx)
	if err != nil {
		t.Fatalf("postgres host: %v", err)
	}
	port, err := started.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("postgres port: %v", err)
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", pgUser, pgPassword, host, port.Port(), pgDatabase)
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
