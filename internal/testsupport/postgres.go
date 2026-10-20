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

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	pgDatabase = "history_test"
	pgUser     = "test_user"
	pgPassword = "test_password"
)

// PostgresDSN starts a throwaway Postgres 16 and returns its DSN. The test is
// skipped when Docker is not reachable, and the container is removed on cleanup.
func PostgresDSN(t *testing.T) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if !dockerRunning(ctx) {
		t.Skip("Docker daemon is not running or reachable")
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_DB":       pgDatabase,
				"POSTGRES_USER":     pgUser,
				"POSTGRES_PASSWORD": pgPassword,
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
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("postgres host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5432")
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
