package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/user-for-download/go-dota/internal/config"
	"github.com/user-for-download/go-dota/internal/logger"
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

	collector := worker.NewCollector(
		redisClient,
		cfg.TargetAPIURL,
		cfg.CollectorWorkers,
		log,
		cfg.SkipTLSVerify,
		cfg.MaxProxyFails,
		cfg.CollectorMaxRetries,
		cfg.CollectorMaxRateLimitRetries,
	)

	collector.Run(ctx)
	log.Info("collector main loop exiting")
}
