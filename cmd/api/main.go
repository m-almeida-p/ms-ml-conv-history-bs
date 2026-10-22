// Command api serves the conversation history over HTTP.
//
// There is no authentication: this is a PoC and must not be deployed.
package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/config"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/history"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/api"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/logging"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql/repository"
)

// shutdownTimeout bounds how long in-flight requests get after SIGINT or SIGTERM.
const shutdownTimeout = 10 * time.Second

// @title ms-ml-conv-history-bs API
// @version 1.0
// @description Conversation history of the Yape assistant: the chat between the user and the agent, shaped after the Smooch message. PoC: there is no authentication, so it must not be deployed.
// @host localhost:3100
// @BasePath /
// @schemes http
func main() {
	log := logging.New(os.Stdout)

	if err := run(log); err != nil {
		log.Errorf("api failed: %v", err)
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

	server := api.NewServer(history.NewService(repository.NewPostgresMessageRepository(pool)), log, pool.Ping)

	failed := make(chan error, 1)
	go func() { failed <- server.Start(":" + strconv.Itoa(cfg.Server.Port)) }()
	log.Infof("serving the history on :%d", cfg.Server.Port)

	select {
	case err := <-failed:
		// Start returns at once when the port is taken.
		return err
	case <-ctx.Done():
	}

	log.Infof("stopping")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("shut down: %w", err)
	}

	return nil
}
