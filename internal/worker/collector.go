package worker

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/user-for-download/go-dota/internal/httpx"
	"github.com/user-for-download/go-dota/internal/models"
	"github.com/user-for-download/go-dota/internal/storage/redis"
)

type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

type CircuitBreaker struct {
	state          CircuitState
	failures       int
	successes      int
	lastFailure    time.Time
	maxFailures    int
	minSuccesses   int
	resetTimeout   time.Duration
	maxBackoff     time.Duration
	probeInFlight  bool
	mu             sync.Mutex
}

func NewCircuitBreaker(maxFailures, minSuccesses int, resetTimeout, maxBackoff time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:        CircuitClosed,
		maxFailures:  maxFailures,
		minSuccesses: minSuccesses,
		resetTimeout: resetTimeout,
		maxBackoff:   maxBackoff,
	}
}

func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.lastFailure) > cb.resetTimeout {
			cb.state = CircuitHalfOpen
			cb.successes = 0
			cb.probeInFlight = false
			return true
		}
		return false
	case CircuitHalfOpen:
		if cb.probeInFlight {
			return false
		}
		cb.probeInFlight = true
		return true
	default:
		return false
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == CircuitHalfOpen {
		cb.successes++
		cb.probeInFlight = false
		if cb.successes >= cb.minSuccesses {
			cb.state = CircuitClosed
			cb.failures = 0
		}
	} else {
		cb.failures = 0
	}
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()

	if cb.state == CircuitHalfOpen {
		cb.state = CircuitOpen
		cb.probeInFlight = false
	} else if cb.failures >= cb.maxFailures {
		cb.state = CircuitOpen
	}
}

func (cb *CircuitBreaker) BackoffDuration() time.Duration {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	backoff := cb.resetTimeout
	for i := 0; i < cb.failures && backoff < cb.maxBackoff; i++ {
		backoff *= 2
	}
	if backoff > cb.maxBackoff {
		backoff = cb.maxBackoff
	}
	return backoff
}

type Collector struct {
	redisClient    *redis.Client
	targetAPIURL string
	numWorkers    int
	logger       *slog.Logger
	httpClient    *httpx.ProxiedClient
	circuitBreaker *CircuitBreaker
	maxProxyFails  int
}

func NewCollector(
	redisClient *redis.Client,
	targetAPIURL string,
	numWorkers int,
	logger *slog.Logger,
	skipTLSVerify bool,
	maxProxyFails int,
) *Collector {
	opts := httpx.DefaultOptions()
	opts.SkipTLSVerify = skipTLSVerify
	pool := httpx.NewTransportPool(opts)
	if maxProxyFails <= 0 {
		maxProxyFails = redis.DefaultMaxProxyFails
	}
	return &Collector{
		redisClient:    redisClient,
		targetAPIURL:   targetAPIURL,
		numWorkers:   numWorkers,
		logger:     logger,
		httpClient:  httpx.NewProxiedClient(pool, 15*time.Second),
		circuitBreaker: NewCircuitBreaker(5, 3, 30*time.Second, 60*time.Second),
		maxProxyFails: maxProxyFails,
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
			c.logger.Debug("waiting for tasks from fetcher")
			continue
		}

		c.processTask(ctx, task, id)
	}
}

func (c *Collector) processTask(ctx context.Context, task models.FetchTask, workerID int) {
	const maxRetries = 5
	const maxRateLimitRetries = 20
	noProxyCounter := 0
	rateLimitRetries := 0

	if !c.circuitBreaker.Allow() {
		backoff := c.circuitBreaker.BackoffDuration()
		c.logger.Warn("circuit breaker open, re-enqueueing task", "worker_id", workerID, "backoff", backoff)

		if err := c.redisClient.PushFetchTask(ctx, task); err != nil {
			c.logger.Error("failed to re-enqueue task after circuit breaker open", "task", task.MatchID, "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		return
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		proxyCount, err := c.redisClient.GetProxyCount(ctx)
		if err != nil {
			c.logger.Warn("failed to get proxy count", "error", err)
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
			case <-time.After(time.Second):
			}
			continue
		}
		noProxyCounter = 0

		proxyURL, err := c.redisClient.GetRandomProxy(ctx)
		if err != nil {
			c.logger.Warn("no proxy available, backing off",
				"worker_id", workerID, "attempt", attempt, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		allowed, err := c.redisClient.AtomicRateLimit(ctx, proxyURL)
		if err != nil {
			c.logger.Warn("rate limit check failed", "proxy", proxyURL, "error", err)
			continue
		}
		if !allowed {
			c.logger.Debug("proxy rate limited, trying another", "proxy", proxyURL)
			rateLimitRetries++
			if rateLimitRetries >= maxRateLimitRetries {
				c.logger.Warn("too many rate limits, re-enqueueing task for later", "url", task.URL, "worker_id", workerID)
				if err := c.redisClient.PushFetchTask(ctx, task); err != nil {
					c.logger.Error("failed to re-enqueue task after rate limit exhaustion", "task", task.URL, "error", err)
				}
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
			continue
		}

		resp, err := c.httpClient.Get(ctx, task.URL, proxyURL)
		if err != nil {
			c.circuitBreaker.RecordFailure()
			_ = c.redisClient.RecordProxyFailure(ctx, proxyURL, c.maxProxyFails)
			c.httpClient.RemoveProxy(proxyURL)
			c.logger.Debug("fetch failed (network error)", "proxy", proxyURL, "error", err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			c.circuitBreaker.RecordFailure()
			_ = c.redisClient.RecordProxyFailure(ctx, proxyURL, c.maxProxyFails)
			c.httpClient.RemoveProxy(proxyURL)
			c.logger.Debug("fetch failed (bad status)", "proxy", proxyURL, "status", resp.StatusCode)
			continue
		}

		c.circuitBreaker.RecordSuccess()
		_ = c.redisClient.RecordProxySuccess(ctx, proxyURL)

		taskID := uuid.New().String()
		if err := c.redisClient.AtomicEnqueueRawData(ctx, taskID, resp.Body); err != nil {
			c.logger.Error("enqueue raw data failed", "task_id", taskID, "error", err)
			continue
		}

		c.logger.Info("task fetched and queued",
			"task_id", taskID, "proxy", proxyURL, "worker_id", workerID)
		return
	}

	c.circuitBreaker.RecordFailure()
	c.logger.Warn("task exhausted retries, dropping", "url", task.URL, "worker_id", workerID)
}