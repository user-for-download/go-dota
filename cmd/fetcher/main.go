package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
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
	file := flag.String("file", "default.sql", "SQL file path (relative to SQL_DIR)")
	oneShot := flag.Bool("once", false, "run a single fetch pass and exit")
	interval := flag.Duration("interval", 24*time.Hour, "fetch interval (ignored with --once)")
	flag.Parse()

	log := logger.Init()

	cfg, err := config.Load()
	if err != nil {
		log.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	sqlPath := filepath.Join(cfg.SQLDir, *file)
	if _, err := os.Stat(sqlPath); err != nil {
		log.Error("sql file not found", "path", sqlPath, "error", err)
		os.Exit(1)
	}

	log.Info("starting fetcher", "path", sqlPath, "once", *oneShot, "interval", *interval)

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
		if err := redisClient.Close(); err != nil {
			log.Error("redisClient close", "error", err)
		}
	}(redisClient)

	// Main DB for dedup
	pgClient, err := postgresstore.NewClient(ctx, cfg.PostgresURL)
	if err != nil {
		log.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pgClient.Close()

	repo := postgresstore.NewRepository(pgClient)
	// Migrations are handled by the dedicated migrate service.

	// Wait for dependencies to be ready
	if err := readiness.WaitAll(ctx, log,
		readiness.Check{Name: "redis", Probe: readiness.Redis(redisClient.Instance()), Timeout: 10 * time.Second},
		readiness.Check{Name: "postgres", Probe: readiness.Postgres(pgClient.Pool()), Timeout: 10 * time.Second},
		readiness.Check{Name: "schema", Probe: readiness.SchemaApplied(pgClient.Pool(), "001_init.sql"), Timeout: 10 * time.Second},
		readiness.Check{Name: "proxy_pool", Probe: readiness.ProxyPool(redisClient.Instance(), 5), Timeout: 10 * time.Minute},
		readiness.Check{Name: "enricher_bootstrap", Probe: readiness.EnricherBootstrapped(redisClient.Instance()), Timeout: 15 * time.Minute},
	); err != nil {
		log.Error("not ready", "error", err)
		os.Exit(1)
	}

	fetcher := worker.NewFetcher(
		redisClient, repo, sqlPath, log,
		cfg.MaxQueueSize, cfg.MaxProxyFails,
	)

	for {
		if err := fetcher.Run(ctx); err != nil {
			log.Error("fetcher error", "error", err)
		} else {
			log.Info("fetcher pass completed successfully")
		}

		if *oneShot {
			return
		}

		log.Info("fetcher sleeping until next interval", "interval", *interval)
		select {
		case <-ctx.Done():
			return
		case <-time.After(*interval):
		}
	}
}
