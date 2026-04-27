package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/user-for-download/go-dota/internal/config"
	"github.com/user-for-download/go-dota/internal/logger"
	postgresstore "github.com/user-for-download/go-dota/internal/storage/postgres"
	redisstore "github.com/user-for-download/go-dota/internal/storage/redis"
	"github.com/user-for-download/go-dota/internal/worker"
)

func main() {
	migrateOnly := flag.Bool("migrate-only", false, "run database migrations and exit")
	flag.Parse()

	log := logger.Init()
	log.Info("starting parser")

	cfg, err := config.Load()
	if err != nil {
		log.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pgClient, err := postgresstore.NewClient(ctx, cfg.PostgresURL)
	if err != nil {
		log.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pgClient.Close()

	repo := postgresstore.NewRepository(pgClient)
	if err := repo.Migrate(ctx); err != nil {
		log.Error("failed to run migrations", "error", err)
		os.Exit(1)
	}

	if *migrateOnly {
		log.Info("migrations complete – exiting")
		return
	}

	redisClient, err := redisstore.NewClientWithConfig(ctx, cfg.RedisURL, redisstore.ClientConfig{
		MaxRetryCount: cfg.MaxRetries,
	})
	if err != nil {
		log.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer func(redisClient *redisstore.Client) {
		if err := redisClient.Close(); err != nil {
			log.Error("redisClient close", "error", err)
		}
	}(redisClient)

	parser := worker.NewParser(
		redisClient, repo, cfg.ParserWorkers, log,
		cfg.DLQBatchSize, cfg.DLQMaxPerTick,
	)
	if err := parser.Run(ctx); err != nil {
		log.Error("parser error", "error", err)
		os.Exit(1)
	}
}