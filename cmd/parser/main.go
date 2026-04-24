package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/user-for-download/go-dota/internal/config"
	"github.com/user-for-download/go-dota/internal/logger"
	postgresstore "github.com/user-for-download/go-dota/internal/storage/postgres"
	redisstore "github.com/user-for-download/go-dota/internal/storage/redis"
	"github.com/user-for-download/go-dota/internal/worker"
)

var allowedKeys = []string{"leagues", "players", "teams", "default"}

func main() {
	key := flag.String("key", "default", fmt.Sprintf("Fetch key: %v", allowedKeys))
	flag.Parse()

	validKeys := make(map[string]bool)
	for _, k := range allowedKeys {
		validKeys[k] = true
	}
	if !validKeys[*key] {
		_, err := fmt.Fprintf(os.Stderr, "Invalid --key value. Allowed values: %v\n", allowedKeys)
		if err != nil {
			return
		}
		os.Exit(1)
	}

	log := logger.Init()
	log.Info("starting fetcher", "key", *key)

	cfg, err := config.Load()
	if err != nil {
		log.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisClient, err := redisstore.NewClientWithConfig(ctx, cfg.RedisURL, redisstore.ClientConfig{
		MaxRetryCount: cfg.MaxRetries,
	})
	if err != nil {
		log.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer func(redisClient *redisstore.Client) {
		err := redisClient.Close()
		if err != nil {

		}
	}(redisClient)

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

	parser := worker.NewParser(redisClient, repo, cfg.ParserWorkers, log)
	if err := parser.Run(ctx); err != nil {
		log.Error("parser error", "error", err)
		os.Exit(1)
	}

	fetcher := worker.NewFetcher(redisClient, repo, *key, cfg.SQLDir, log, cfg.MaxQueueSize, cfg.MaxProxyFails)

	if err := fetcher.Run(ctx); err != nil {
		log.Error("fetcher error", "error", err)
		err := os.Stdout.Sync()
		if err != nil {
			return
		}
		os.Exit(1)
	}

	log.Info("fetcher completed successfully")
}
