package worker

import (
	"context"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/user-for-download/go-dota/internal/httpx"
	"github.com/user-for-download/go-dota/internal/models"
	"github.com/user-for-download/go-dota/internal/storage/redis"
)

// Collector pulls FetchTasks from Redis, fetches the target URL through a
// proxy from the pool, and pushes the raw response into the parse queue.
//
// Failure handling is layered:
//   - Per-proxy: RecordProxyFailure / RecordProxySuccess in Redis adjusts the
//     proxy's weight in the pool; repeated failures evict it.
//   - Per-request: up to `maxRetries` attempts with different proxies.
//   - Rate limiting: enforced in Redis via AtomicRateLimit.
//
// There is deliberately no global circuit breaker. A bad proxy should not
// stall every worker; the per-proxy signal + pool rotation is sufficient, and
// a shared breaker was observed to deadlock under burst failures.
type Collector struct {
	redisClient    *redis.Client
	numWorkers   int
	logger      *slog.Logger
	httpClient   *httpx.ProxiedClient
	maxProxyFails int
	maxRetries   int
	maxRateLimitRetries int
	maxQueueSize int64
}

func NewCollector(
	redisClient *redis.Client,
	numWorkers int,
	logger *slog.Logger,
	skipTLSVerify bool,
	maxProxyFails int,
	maxRetries int,
	maxRateLimitRetries int,
	maxQueueSize int64,
) *Collector {
	opts := httpx.DefaultOptions()
	opts.SkipTLSVerify = skipTLSVerify
	pool := httpx.NewTransportPool(opts)

	if maxProxyFails <= 0 {
		maxProxyFails = redis.DefaultMaxProxyFails
	}
	if maxRetries <= 0 {
		maxRetries = 5
	}
	if maxRateLimitRetries <= 0 {
		maxRateLimitRetries = 20
	}
	if maxQueueSize <= 0 {
		maxQueueSize = 10000
	}

	return &Collector{
		redisClient:        redisClient,
		numWorkers:       numWorkers,
		logger:           logger,
		httpClient:       httpx.NewProxiedClient(pool, 30*time.Second),
		maxProxyFails:    maxProxyFails,
		maxRetries:       maxRetries,
		maxRateLimitRetries: maxRateLimitRetries,
		maxQueueSize:     maxQueueSize,
	}
}

func (c *Collector) Run(ctx context.Context) {
	c.logger.Info("collector starting workers", "count", c.numWorkers)

	var wg sync.WaitGroup
	for i := 0; i < c.numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c.worker(ctx, id)
		}(i)
	}

	<-ctx.Done()
	c.logger.Info("collector shutting down, waiting for workers to drain")
	wg.Wait()
	c.logger.Info("collector all workers stopped")
}

func (c *Collector) worker(ctx context.Context, id int) {
	c.logger.Info("collector worker started", "worker_id", id)
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("collector worker stopping", "worker_id", id)
			return
		default:
		}

		task, err := c.redisClient.PopFetchTask(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Debug("waiting for tasks from fetcher", "worker_id", id)
			continue
		}

		c.processTask(ctx, task, id)
	}
}

// processTask attempts to fetch one task, rotating through proxies on failure.
//
// Outcomes:
//   - success: enqueue raw data for the parser, return
//   - transient failure (bad proxy, rate limit): try another proxy
//   - exhaustion: log and drop; the fetcher layer's seen-set prevents
//     immediate re-queue of the same match
func (c *Collector) processTask(ctx context.Context, task models.FetchTask, workerID int) {
	noProxyCounter := 0
	rateLimitRetries := 0

	for attempt := 0; attempt < c.maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Wait for a non-empty pool.
		proxyCount, err := c.redisClient.GetProxyCount(ctx)
		if err != nil {
			c.logger.Warn("failed to get proxy count", "worker_id", workerID, "error", err)
			continue
		}
		if proxyCount == 0 {
			noProxyCounter++
			if noProxyCounter%60 == 0 {
				c.logger.Warn("no proxies available, waiting...", "worker_id", workerID)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(jitteredSleep(time.Second)):
			}
			continue
		}
		noProxyCounter = 0

		proxyURL, err := c.redisClient.GetWeightedRandomProxy(ctx)
		if err != nil {
			c.logger.Warn("no proxy available, backing off",
				"worker_id", workerID, "attempt", attempt, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(jitteredSleep(2 * time.Second)):
			}
			continue
		}

		// Respect per-proxy rate limits.
		allowed, err := c.redisClient.AtomicRateLimit(ctx, proxyURL)
		if err != nil {
			c.logger.Warn("rate limit check failed",
				"worker_id", workerID, "proxy", proxyURL, "error", err)
			continue
		}
		if !allowed {
			rateLimitRetries++
			c.logger.Debug("proxy rate limited, trying another",
				"worker_id", workerID, "proxy", proxyURL,
				"rate_limit_retries", rateLimitRetries)

			if rateLimitRetries >= c.maxRateLimitRetries {
				c.logger.Warn("too many rate limits, re-enqueueing task for later",
					"worker_id", workerID, "url", task.URL)
				ok, err := c.redisClient.PushFetchTaskWithCap(ctx, task, c.maxQueueSize)
				if err != nil {
					c.logger.Error("failed to re-enqueue task after rate limit exhaustion",
						"worker_id", workerID, "task", task.URL, "error", err)
				} else if !ok {
					c.logger.Warn("fetch queue at capacity, dropping task",
						"worker_id", workerID, "task", task.URL)
				}
				return
			}
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitteredSleep(time.Second)):
		}
		continue
	}

		// Do the fetch.
		resp, err := c.httpClient.Get(ctx, task.URL, proxyURL)
		if err != nil {
			_ = c.redisClient.RecordProxyFailure(ctx, proxyURL, c.maxProxyFails)
			c.httpClient.RemoveProxy(proxyURL)
			c.logger.Debug("fetch failed (network error)",
				"worker_id", workerID, "proxy", proxyURL, "error", err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			_ = c.redisClient.RecordProxyFailure(ctx, proxyURL, c.maxProxyFails)
			c.httpClient.RemoveProxy(proxyURL)
			c.logger.Debug("fetch failed (bad status)",
				"worker_id", workerID, "proxy", proxyURL, "status", resp.StatusCode)
			continue
		}

		// Success path.
		_ = c.redisClient.RecordProxySuccess(ctx, proxyURL)

		taskID := uuid.New().String()
		if err := c.redisClient.AtomicEnqueueRawData(ctx, taskID, resp.Body); err != nil {
			c.logger.Error("enqueue raw data failed",
				"worker_id", workerID, "task_id", taskID, "error", err)
			continue
		}

		c.logger.Info("task fetched and queued",
			"task_id", taskID,
			"match_id", task.MatchID,
			"proxy", proxyURL,
			"worker_id", workerID,
			"attempt", attempt)
		return
	}

	c.logger.Warn("task exhausted retries, sending to DLQ",
		"worker_id", workerID, "url", task.URL)
	if err := c.redisClient.PushFetchDLQTask(ctx, task); err != nil {
		c.logger.Error("failed to push task to fetch DLQ",
			"worker_id", workerID, "error", err)
	}
}

func jitteredSleep(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	quarter := int64(base / 4)
	if quarter <= 0 {
		return base
	}
	return base + time.Duration(rand.Int63n(quarter))
}
