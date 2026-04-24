package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/user-for-download/go-dota/internal/config"
	"github.com/user-for-download/go-dota/internal/logger"
	redisstore "github.com/user-for-download/go-dota/internal/storage/redis"
	"github.com/user-for-download/go-dota/internal/worker"
)

func main() {
	log := logger.Init()
	log.Info("starting proxy manager")

	cfg, err := config.Load()
	if err != nil {
		log.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisClient, err := redisstore.NewClient(ctx, cfg.RedisURL)
	if err != nil {
		log.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			log.Error("redis close", "error", err)
		}
	}()

	refreshInterval := time.Duration(cfg.ProxyRefreshMin) * time.Minute
	pm := worker.NewProxyManagerWithConfig(
		redisClient,
		cfg.ProxyProviderURL,
		cfg.HealthCheckURL,
		refreshInterval,
		log,
		cfg.ProxyLocalFile,
	)

	if err := pm.Run(ctx); err != nil {
		log.Error("proxy manager error", "error", err)
		os.Exit(1)
	}
}
