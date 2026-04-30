package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/user-for-download/go-dota/internal/config"
	"github.com/user-for-download/go-dota/internal/httpx"
	"github.com/user-for-download/go-dota/internal/logger"
	postgresstore "github.com/user-for-download/go-dota/internal/storage/postgres"
	redisstore "github.com/user-for-download/go-dota/internal/storage/redis"
	"github.com/user-for-download/go-dota/internal/worker"
)

func main() {
	oneShot := flag.Bool("once", false, "run a single enrichment pass and exit")
	interval := flag.Duration("interval", 24*time.Hour, "enrichment interval (ignored with --once)")
	flag.Parse()

	log := logger.Init()
	log.Info("starting enricher", "once", *oneShot, "interval", *interval)

	cfg, err := config.Load()
	if err != nil {
		log.Error("config load", "error", err)
		os.Exit(1)
	}

	httpx.SetDefaultMaxPoolSize(cfg.MaxPoolSize)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb, err := redisstore.NewClientWithConfig(ctx, cfg.RedisURL, redisstore.ClientConfig{
		MaxRetryCount: cfg.MaxRetries,
	})
	if err != nil {
		log.Error("redis connect", "error", err)
		os.Exit(1)
	}
	defer func(rdb *redisstore.Client) {
		err := rdb.Close()
		if err != nil {
			log.Error("redis store", "error", err)
		}
	}(rdb)

	db, err := postgresstore.NewClient(ctx, cfg.PostgresURL)
	if err != nil {
		log.Error("postgres connect", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	repo := postgresstore.NewRepository(db)

	// Direct mapping - cleaner than checking each string individually
	enricherCfg := worker.EnricherConfig{
		HeroesURL:      cfg.EnricherHeroesURL,
		LeaguesURL:     cfg.EnricherLeaguesURL,
		TeamsURL:       cfg.EnricherTeamsURL,
		ItemsURL:       cfg.EnricherItemsURL,
		GameModesURL:   cfg.EnricherGameModesURL,
		LobbyTypesURL:  cfg.EnricherLobbyTypesURL,
		PatchesURL:     cfg.EnricherPatchesURL,
		SkipTLSVerify:  cfg.SkipTLSVerify,
	}

	enricher := worker.NewEnricher(rdb, repo, log, enricherCfg)

	for {
		if err := enricher.Run(ctx); err != nil {
			log.Error("enrichment pass failed", "error", err)
		}
		if *oneShot {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(*interval):
		}
	}
}
