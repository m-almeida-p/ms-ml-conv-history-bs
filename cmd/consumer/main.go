// Command consumer reads the conversation events topic and builds the chat history.
//
//	consumer          read the topic until SIGINT or SIGTERM
//	consumer rebuild  recreate the chat messages from the inbox, then exit
package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/yaperos/ms-ml-conv-history-bs/internal/config"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/port/service"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/core/projection"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/kafka"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/logging"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql"
	"github.com/yaperos/ms-ml-conv-history-bs/internal/infrastructure/postgresql/repository"
)

func main() {
	log := logging.New(os.Stdout)

	if err := run(os.Args[1:], log); err != nil {
		log.Errorf("consumer failed: %v", err)
		os.Exit(1)
	}
}

func run(args []string, log *logging.Logger) error {
	command := "run"
	if len(args) > 0 {
		command = args[0]
	}
	if command != "run" && command != "rebuild" {
		return fmt.Errorf("unknown command %q (use run or rebuild)", command)
	}

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

	projector := projection.NewService(repository.NewInbox(pool))

	if command == "rebuild" {
		return rebuild(ctx, projector, log)
	}

	return consume(ctx, cfg.Kafka, projector, log)
}

func rebuild(ctx context.Context, projector service.Projection, log *logging.Logger) error {
	result, err := projector.Rebuild(ctx)
	if err != nil {
		return err
	}
	log.Infof("rebuilt the chat messages: events=%d messages=%d invalid=%d", result.Events, result.Messages, result.Invalid)

	return nil
}

func consume(ctx context.Context, cfg config.KafkaConfig, projector service.Projection, log *logging.Logger) error {
	for _, warning := range cfg.Warnings() {
		log.Warnf("%s", warning)
	}

	source, err := kafka.NewSource(cfg)
	if err != nil {
		return err
	}
	log.Infof("consuming %s as group %s", cfg.Topic, cfg.GroupID)

	return kafka.NewConsumer(source, projector, log).Run(ctx)
}
