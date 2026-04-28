package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/user-for-download/go-dota/internal/config"
	"github.com/user-for-download/go-dota/internal/logger"
	"github.com/user-for-download/go-dota/internal/readiness"
	redisstore "github.com/user-for-download/go-dota/internal/storage/redis"
	"github.com/user-for-download/go-dota/internal/worker"
)

func main() {
	log := logger.Init()
	log.Info("starting collector")

	cfg, err := config.Load()
	if err != nil {
		log.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	redisClient, err := redisstore.NewClientWithConfig(ctx, cfg.RedisURL, redisstore.ClientConfig{
		MaxRetryCount: cfg.MaxRetries,
		MaxReqPerMin:  cfg.MaxProxyReqPerMin,
		MaxReqPerDay:  cfg.MaxProxyReqPerDay,
	})
	if err != nil {
		log.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer func(redisClient *redisstore.Client) {
		err := redisClient.Close()
		if err != nil {
			log.Error("redisClient", "error", err)
		}
	}(redisClient)

	// Wait for dependencies to be ready
	if err := readiness.WaitAll(ctx, log,
		readiness.Check{
			Name:    "redis",
			Probe:   readiness.Redis(redisClient.Instance()),
			Timeout: 10 * time.Second,
		},
		readiness.Check{
			Name:    "proxy_pool",
			Probe:   readiness.ProxyPool(redisClient.Instance(), 5),
			Timeout: 10 * time.Minute,
		},
	); err != nil {
		log.Error("not ready", "error", err)
		os.Exit(1)
	}

	collector := worker.NewCollector(
		redisClient,
		cfg.CollectorWorkers,
		log,
		cfg.SkipTLSVerify,
		cfg.MaxProxyFails,
		cfg.CollectorMaxRetries,
		cfg.CollectorMaxRateLimitRetries,
		cfg.MaxQueueSize,
	)

	collector.Run(ctx)
	log.Info("collector main loop exiting")
}
