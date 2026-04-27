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
	postgresstore "github.com/user-for-download/go-dota/internal/storage/postgres"
	"github.com/user-for-download/go-dota/internal/worker"
)

func main() {
	oneShot := flag.Bool("once", false, "run a single maintenance pass and exit")
	interval := flag.Duration("interval", 24*time.Hour, "maintenance interval")
	ahead := flag.Int("ahead", 3, "forward quarters of partitions to maintain")
	retention := flag.Int("retention-months", 0, "months of data to retain; 0 = disabled")
	detachOnly := flag.Bool("detach-only", true, "detach instead of dropping old partitions")
	flag.Parse()

	log := logger.Init()
	log.Info("starting partition-manager",
		"once", *oneShot,
		"interval", *interval,
		"ahead", *ahead,
		"retention_months", *retention,
		"detach_only", *detachOnly,
	)

	cfg, err := config.Load()
	if err != nil {
		log.Error("config load", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := postgresstore.NewClient(ctx, cfg.PostgresURL)
	if err != nil {
		log.Error("postgres connect", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	pmc := worker.PartitionManagerConfig{
		MaxAheadMonths:   *ahead,
		RetentionMonths: *retention,
		DetachOnly:      *detachOnly,
	}
	if !*oneShot {
		pmc.CheckInterval = *interval
	}

	pm := worker.NewPartitionManager(postgresstore.NewRepository(db), log, pmc)

	if err := pm.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("partition manager error", "error", err)
		os.Exit(1)
	}
	log.Info("partition-manager exiting")
}