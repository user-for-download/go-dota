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
	log.Info("starting parser", "key", *key)

	cfg, err := config.Load()
	if err != nil {
		log.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Redis
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

	// Main DB (normalized match schema)
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

	// Legacy DB (raw JSON dumps in parsed_data)
	legacyClient, err := postgresstore.NewClient(ctx, cfg.LegacyPostgresURL)
	if err != nil {
		log.Error("failed to connect to legacy postgres", "error", err)
		os.Exit(1)
	}
	defer legacyClient.Close()

	legacyRepo := postgresstore.NewLegacyRepository(legacyClient)
	if err := legacyRepo.EnsureSchema(ctx); err != nil {
		log.Error("failed to ensure legacy schema", "error", err)
		os.Exit(1)
	}

	// Parser writes to legacy DB
	parser := worker.NewParser(redisClient, legacyRepo, cfg.ParserWorkers, log)
	if err := parser.Run(ctx); err != nil {
		log.Error("parser error", "error", err)
		os.Exit(1)
	}

	// Fetcher dedupes against legacy DB
	fetcher := worker.NewFetcher(
		redisClient, legacyRepo, *key, cfg.SQLDir, log,
		cfg.MaxQueueSize, cfg.MaxProxyFails,
	)
	if err := fetcher.Run(ctx); err != nil {
		log.Error("fetcher error", "error", err)
		if syncErr := os.Stdout.Sync(); syncErr != nil {
			return
		}
		os.Exit(1)
	}

	log.Info("fetcher completed successfully")
}
