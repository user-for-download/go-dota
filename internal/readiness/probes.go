package readiness

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
)

func Postgres(p *pgxpool.Pool) Probe {
	return func(ctx context.Context) error { return p.Ping(ctx) }
}

func Redis(c *goredis.Client) Probe {
	return func(ctx context.Context) error { return c.Ping(ctx).Err() }
}

func ProxyPool(c *goredis.Client, minSize int64) Probe {
	return func(ctx context.Context) error {
		n, err := c.SCard(ctx, "proxy_pool").Result()
		if err != nil {
			return err
		}
		if n < minSize {
			return fmt.Errorf("only %d/%d proxies", n, minSize)
		}
		return nil
	}
}

func SchemaApplied(p *pgxpool.Pool, version string) Probe {
	return func(ctx context.Context) error {
		var ok bool
		if err := p.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`,
			version,
		).Scan(&ok); err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("missing migration %s", version)
		}
		return nil
	}
}

func EnricherBootstrapped(c *goredis.Client) Probe {
	return func(ctx context.Context) error {
		v, err := c.Get(ctx, "enricher:bootstrapped").Result()
		if err != nil {
			if errors.Is(err, goredis.Nil) {
				return fmt.Errorf("enricher has not completed first pass")
			}
			return err
		}
		if v != "1" {
			return fmt.Errorf("enricher bootstrap marker invalid: %q", v)
		}
		return nil
	}
}