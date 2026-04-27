package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

type ClientConfig struct {
	MaxRetryCount int
	MaxReqPerMin  int
	MaxReqPerDay  int
}

func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		MaxRetryCount: 3,
		MaxReqPerMin:  60,
		MaxReqPerDay:  3000,
	}
}

// Client wraps a go-redis client with dependency-injection-friendly construction.
type Client struct {
	rdb *goredis.Client
	cfg ClientConfig
}

// NewClient creates and validates a Redis connection.
func NewClient(ctx context.Context, redisURL string) (*Client, error) {
	return NewClientWithConfig(ctx, redisURL, DefaultClientConfig())
}

// NewClientWithConfig creates and validates a Redis connection with custom config.
func NewClientWithConfig(ctx context.Context, redisURL string, cfg ClientConfig) (*Client, error) {
	opts, err := goredis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	rdb := goredis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Client{rdb: rdb, cfg: cfg}, nil
}

// Close releases the Redis connection.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Instance exposes the underlying go-redis client for advanced use if needed.
func (c *Client) Instance() *goredis.Client {
	return c.rdb
}
