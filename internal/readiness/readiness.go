package readiness

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type Probe func(context.Context) error

type Check struct {
	Name    string
	Probe   Probe
	Timeout time.Duration
	Every   time.Duration
}

func WaitAll(ctx context.Context, log *slog.Logger, checks ...Check) error {
	for _, c := range checks {
		if err := waitOne(ctx, log, c); err != nil {
			return fmt.Errorf("%s: %w", c.Name, err)
		}
	}
	return nil
}

func waitOne(ctx context.Context, log *slog.Logger, c Check) error {
	every := c.Every
	if every == 0 {
		every = 2 * time.Second
	}
	var deadline <-chan time.Time
	if c.Timeout > 0 {
		t := time.NewTimer(c.Timeout)
		defer t.Stop()
		deadline = t.C
	}

	start := time.Now()
	last := start
	log.Info("readiness: waiting", "dep", c.Name)

	for {
		if err := c.Probe(ctx); err == nil {
			log.Info("readiness: ready",
				"dep", c.Name,
				"waited", time.Since(start).Round(time.Second))
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timeout after %s", c.Timeout)
		case <-time.After(every):
		}
		if time.Since(last) > 10*time.Second {
			log.Info("readiness: still waiting",
				"dep", c.Name,
				"elapsed", time.Since(start).Round(time.Second))
			last = time.Now()
		}
	}
}