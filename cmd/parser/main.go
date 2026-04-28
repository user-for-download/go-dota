package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/user-for-download/go-dota/internal/config"
	"github.com/user-for-download/go-dota/internal/logger"
	"github.com/user-for-download/go-dota/internal/readiness"
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

	// Only run migrations if explicitly requested via --migrate-only.
	// Normal operation expects the dedicated migrate service to have run already.
	if *migrateOnly {
		if err := repo.Migrate(ctx); err != nil {
			log.Error("failed to run migrations", "error", err)
			os.Exit(1)
		}
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

	// Wait for dependencies to be ready
	if err := readiness.WaitAll(ctx, log,
		readiness.Check{Name: "redis", Probe: readiness.Redis(redisClient.Instance()), Timeout: 10 * time.Second},
		readiness.Check{Name: "postgres", Probe: readiness.Postgres(pgClient.Pool()), Timeout: 10 * time.Second},
		readiness.Check{Name: "schema", Probe: readiness.SchemaApplied(pgClient.Pool(), "001_init.sql"), Timeout: 10 * time.Second},
		readiness.Check{Name: "enricher_bootstrap", Probe: readiness.EnricherBootstrapped(redisClient.Instance()), Timeout: 15 * time.Minute},
	); err != nil {
		log.Error("not ready", "error", err)
		os.Exit(1)
	}

	parser := worker.NewParser(
		redisClient, repo, cfg.ParserWorkers, log,
		cfg.DLQBatchSize, cfg.DLQMaxPerTick,
	)
	if err := parser.Run(ctx); err != nil {
		log.Error("parser error", "error", err)
		os.Exit(1)
	}
}
