package history_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// lockedBuffer collects a process's output while the test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// buildBinary builds ./cmd/<name> into a temporary directory.
func buildBinary(t *testing.T, name string) string {
	t.Helper()

	bin := filepath.Join(t.TempDir(), name)
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/"+name)
	cmd.Dir = "../.."
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	return bin
}

type serviceProcess struct {
	cmd  *exec.Cmd
	out  *lockedBuffer
	done chan error
}

// startBinary runs one of the service's binaries from the repo root (where
// config.yaml is) with extra environment variables, and captures its output.
func startBinary(t *testing.T, bin string, extraEnv []string) *serviceProcess {
	t.Helper()

	out := &lockedBuffer{}
	cmd := exec.Command(bin)
	cmd.Dir = "../.."
	cmd.Env = append(append(os.Environ(),
		"ENVIRONMENT=production", // do not read a developer's .env
		"LOG_LEVEL=info",
	), extraEnv...)
	cmd.Stdout, cmd.Stderr = out, out
	require.NoError(t, cmd.Start())

	p := &serviceProcess{cmd: cmd, out: out, done: make(chan error, 1)}
	go func() { p.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
		}
		if t.Failed() {
			t.Logf("process output:\n%s", out.String())
		}
	})

	return p
}

// stop sends SIGTERM and requires a clean exit.
func (p *serviceProcess) stop(t *testing.T) {
	t.Helper()

	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	select {
	case err := <-p.done:
		require.NoError(t, err, "the process exits 0 on SIGTERM")
	case <-time.After(30 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Fatal("the process did not stop after SIGTERM")
	}
}

// eventually polls until check passes or the time is up.
func eventually(t *testing.T, timeout time.Duration, what string, check func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}
