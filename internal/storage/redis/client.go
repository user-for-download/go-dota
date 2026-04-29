package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	metricsIngestFailedTotal  = "metrics:ingest_failed_total"
	metricsIngestFailedByKind = "metrics:ingest_failed_by_kind"
	metricsIngestSuccessTotal = "metrics:ingest_success_total"
	metricsParseFailedTotal   = "metrics:parse_failed_total"
	metricsLastFailedSample   = "metrics:last_failed_sample"
)

func (c *Client) IncrIngestFailure(ctx context.Context, kind, errMsg string, matchID int64) error {
	pipe := c.rdb.Pipeline()
	pipe.Incr(ctx, metricsIngestFailedTotal)
	pipe.HIncrBy(ctx, metricsIngestFailedByKind, kind, 1)
	sampleBytes, _ := json.Marshal(map[string]any{
		"ts":       time.Now().Unix(),
		"match_id": matchID,
		"kind":     kind,
		"error":    errMsg,
	})
	pipe.Set(ctx, metricsLastFailedSample, string(sampleBytes), 24*time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

func (c *Client) IncrIngestSuccess(ctx context.Context) error {
	return c.rdb.Incr(ctx, metricsIngestSuccessTotal).Err()
}

func (c *Client) IncrParseFailure(ctx context.Context, kind string) error {
	pipe := c.rdb.Pipeline()
	pipe.Incr(ctx, metricsParseFailedTotal)
	pipe.HIncrBy(ctx, metricsIngestFailedByKind, kind, 1)
	_, err := pipe.Exec(ctx)
	return err
}

type IngestMetrics struct {
	SuccessTotal     int64            `json:"ingest_success_total"`
	FailedTotal      int64            `json:"ingest_failed_total"`
	ParseFailedTotal int64            `json:"parse_failed_total"`
	FailedByKind     map[string]int64 `json:"ingest_failed_by_kind"`
	LastFailedSample string           `json:"last_failed_sample,omitempty"`
}

func (c *Client) GetIngestMetrics(ctx context.Context) (IngestMetrics, error) {
	var m IngestMetrics
	pipe := c.rdb.Pipeline()
	successCmd := pipe.Get(ctx, metricsIngestSuccessTotal)
	failedCmd := pipe.Get(ctx, metricsIngestFailedTotal)
	parseFailedCmd := pipe.Get(ctx, metricsParseFailedTotal)
	byKindCmd := pipe.HGetAll(ctx, metricsIngestFailedByKind)
	sampleCmd := pipe.Get(ctx, metricsLastFailedSample)
	_, _ = pipe.Exec(ctx)

	m.SuccessTotal, _ = successCmd.Int64()
	m.FailedTotal, _ = failedCmd.Int64()
	m.ParseFailedTotal, _ = parseFailedCmd.Int64()
	if vals, err := byKindCmd.Result(); err == nil {
		m.FailedByKind = make(map[string]int64, len(vals))
		for k, v := range vals {
			var n int64
			fmt.Sscanf(v, "%d", &n)
			m.FailedByKind[k] = n
		}
	}
	m.LastFailedSample, _ = sampleCmd.Result()
	return m, nil
}

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
