// Command mcp serves the conversation history to an agent as MCP tools over streamable HTTP.
//
// There is no authentication: any agent that reaches it can read any account. This is a PoC and
// must not be deployed.
package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/config"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/history"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/insights"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/logging"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/mcp"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql/repository"
)

// shutdownTimeout bounds how long in-flight calls get after SIGINT or SIGTERM.
const shutdownTimeout = 10 * time.Second

func main() {
	log := logging.New(os.Stdout)

	if err := run(log); err != nil {
		log.Errorf("mcp failed: %v", err)
		os.Exit(1)
	}
}

func run(log *logging.Logger) error {
	// A local .env fills the variables config.yaml reads. Deployed environments
	// set them directly.
	if os.Getenv("ENVIRONMENT") != "production" {
		if err := godotenv.Load(); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("load .env: %w", err)
		}
	}

	cfg, err := config.Load("")
	if err != nil {
		return err
	}
	if cfg.MCP.Port == 0 {
		return errors.New("config validation failed: mcp.port is required")
	}
	if err := logging.SetLevel(cfg.Logging.Level); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgresql.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer pool.Close()

	server := mcp.NewServer(mcp.Deps{
		History:  history.NewService(repository.NewPostgresMessageRepository(pool)),
		Insights: insights.NewService(repository.NewPostgresInsightsRepository(pool)),
		Log:      log,
	})
	httpServer := mcp.NewHTTPServer(cfg.MCP.Addr(), mcp.NewHandler(server, pool.Ping))

	failed := make(chan error, 1)
	go func() { failed <- httpServer.ListenAndServe() }()
	log.Infof("serving MCP on %s (no authentication: PoC only)", cfg.MCP.Addr())

	select {
	case err := <-failed:
		// ListenAndServe returns at once when the port is taken.
		return err
	case <-ctx.Done():
	}

	log.Infof("stopping")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("shut down: %w", err)
	}

	return nil
}
