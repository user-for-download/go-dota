package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/user-for-download/go-dota/internal/storage/postgres"
)

type PartitionManager struct {
	repo    *postgres.Repository
	log    *slog.Logger

	maxAheadMonths   int
	retentionMonths int
	detachOnly     bool
	checkInterval  time.Duration
}

type PartitionManagerConfig struct {
	MaxAheadMonths   int
	RetentionMonths int
	DetachOnly      bool
	CheckInterval   time.Duration
}

func NewPartitionManager(
	repo *postgres.Repository,
	log *slog.Logger,
	cfg PartitionManagerConfig,
) *PartitionManager {
	if cfg.MaxAheadMonths <= 0 {
		cfg.MaxAheadMonths = 3
	}
	return &PartitionManager{
		repo:            repo,
		log:             log,
		maxAheadMonths:  cfg.MaxAheadMonths,
		retentionMonths: cfg.RetentionMonths,
		detachOnly:     cfg.DetachOnly,
		checkInterval:  cfg.CheckInterval,
	}
}

func (pm *PartitionManager) Run(ctx context.Context) error {
	for {
		if err := pm.once(ctx); err != nil {
			pm.log.Error("partition maintenance failed", "error", err)
		}
		if pm.checkInterval == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pm.checkInterval):
		}
	}
}

func (pm *PartitionManager) once(ctx context.Context) error {
	created, err := pm.ensureForwardPartitions(ctx)
	if err != nil {
		return fmt.Errorf("ensure forward: %w", err)
	}

	var removed int
	if pm.retentionMonths > 0 {
		removed, err = pm.enforceRetention(ctx)
		if err != nil {
			return fmt.Errorf("enforce retention: %w", err)
		}
	}

	pm.log.Info("partition maintenance complete",
		"created", created,
		"removed", removed,
		"detach_only", pm.detachOnly,
	)
	return nil
}

func (pm *PartitionManager) ensureForwardPartitions(ctx context.Context) (int, error) {
	now := time.Now().UTC()
	existing, err := pm.repo.ListMatchPartitions(ctx)
	if err != nil {
		return 0, err
	}
	have := make(map[string]struct{}, len(existing))
	for _, p := range existing {
		have[p.Name] = struct{}{}
	}

	created := 0
	for i := 0; i < pm.maxAheadMonths; i++ {
		target := time.Date(now.Year(), now.Month()+time.Month(i*3), 1, 0, 0, 0, 0, time.UTC)
		name := postgres.QuarterPartitionName(target)
		if _, ok := have[name]; ok {
			continue
		}
		from, to := postgres.QuarterBounds(target)
		if err := pm.repo.CreateMatchPartition(ctx, name, from, to); err != nil {
			return created, fmt.Errorf("create %s: %w", name, err)
		}
		pm.log.Info("created partition", "name", name, "from", from, "to", to)
		created++
	}
	return created, nil
}

func (pm *PartitionManager) enforceRetention(ctx context.Context) (int, error) {
	cutoff := time.Now().UTC().AddDate(0, -pm.retentionMonths, 0).Unix()
	parts, err := pm.repo.ListMatchPartitions(ctx)
	if err != nil {
		return 0, err
	}

	removed := 0
	for _, p := range parts {
		if p.ToEpoch > cutoff {
			continue
		}
		if err := pm.repo.DetachMatchPartition(ctx, p.Name); err != nil {
			pm.log.Warn("detach failed", "name", p.Name, "error", err)
			continue
		}
		pm.log.Info("detached partition", "name", p.Name, "to_epoch", p.ToEpoch)

		if !pm.detachOnly {
			if err := pm.repo.DropMatchPartition(ctx, p.Name); err != nil {
				pm.log.Warn("drop failed", "name", p.Name, "error", err)
				continue
			}
			pm.log.Info("dropped partition", "name", p.Name)
		}
		removed++
	}
	return removed, nil
}