package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Client wraps a pgxpool connection pool.
type Client struct {
	pool *pgxpool.Pool
}

// NewClient creates and validates a PostgreSQL connection pool.
func NewClient(ctx context.Context, postgresURL string) (*Client, error) {
	config, err := pgxpool.ParseConfig(postgresURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	return &Client{pool: pool}, nil
}

// Close releases all pool connections.
func (c *Client) Close() {
	c.pool.Close()
}

// Pool exposes the underlying pgxpool.Pool.
func (c *Client) Pool() *pgxpool.Pool {
	return c.pool
}