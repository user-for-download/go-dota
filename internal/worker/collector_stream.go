package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/user-for-download/go-dota/internal/config"
	"github.com/user-for-download/go-dota/internal/httpx"
	"github.com/user-for-download/go-dota/internal/models"
	"github.com/user-for-download/go-dota/internal/pipeline"
	"github.com/user-for-download/go-dota/internal/storage/redis"
)

type StreamCollector struct {
	redisClient   *redis.Client
	payloadStore PayloadStore
	httpClient   *httpx.ProxiedClient
	maxRetries   int
	maxProxyFails int
	logger       *slog.Logger
	batchSize    int
	blockMS      int
	workerID     string
}

type PayloadStore interface {
	Save(ctx context.Context, taskID string, data []byte) error
	Get(ctx context.Context, taskID string) (json.RawMessage, error)
	Delete(ctx context.Context, taskID string) error
	Extend(ctx context.Context, taskID string) error
}

func NewStreamCollector(
	redisClient *redis.Client,
	payloadStore PayloadStore,
	httpClient *httpx.ProxiedClient,
	logger *slog.Logger,
	cfg *config.Config,
) *StreamCollector {
	return &StreamCollector{
		redisClient:   redisClient,
		payloadStore:  payloadStore,
		httpClient:    httpClient,
		maxRetries:    cfg.CollectorMaxRetries,
		maxProxyFails: cfg.MaxProxyFails,
		logger:        logger,
		batchSize:     cfg.StreamBatchSize,
		blockMS:       cfg.StreamBlockMS,
		workerID:      fmt.Sprintf("collector-%d", rand.Intn(10000)),
	}
}

func (c *StreamCollector) Run(ctx context.Context) error {
	if err := c.redisClient.EnsureStreamGroups(ctx); err != nil {
		return fmt.Errorf("ensure stream groups: %w", err)
	}

	c.logger.Info("stream collector started", "worker_id", c.workerID)

	staleClaimTicker := time.NewTicker(5 * time.Minute)
	defer staleClaimTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-staleClaimTicker.C:
			c.recoverStaleTasks(ctx)
		default:
		}

		msgs, err := c.redisClient.ReadFetchTasks(ctx, pipeline.CollectorGroup, c.workerID, int64(c.batchSize), c.blockMS)
		if err != nil {
			c.logger.Error("read fetch tasks", "error", err)
			time.Sleep(time.Second)
			continue
		}

		if len(msgs) == 0 {
			continue
		}

		for _, msg := range msgs {
			c.processFetchMessage(ctx, msg)
		}
	}
}

func (c *StreamCollector) recoverStaleTasks(ctx context.Context) {
	staleMsgs, err := c.redisClient.ClaimStaleFetchTasks(ctx, c.workerID, 5*time.Minute, int64(c.batchSize))
	if err != nil {
		c.logger.Error("claim stale fetch tasks failed", "error", err)
		return
	}

	if len(staleMsgs) == 0 {
		return
	}

	c.logger.Info("recovered stale pending tasks", "count", len(staleMsgs))
	for _, msg := range staleMsgs {
		c.processFetchMessage(ctx, msg)
	}
}

func (c *StreamCollector) processFetchMessage(ctx context.Context, msg models.StreamMessage) {
	data, ok := msg.Task.(map[string]interface{})
	if !ok {
		c.logger.Warn("invalid task format", "id", msg.ID)
		_ = c.redisClient.AckFetchTask(ctx, pipeline.FetchTasksStream, msg.ID)
		return
	}

	matchID, _ := data["match_id"].(string)
	url, _ := data["url"].(string)

	if matchID == "" || url == "" {
		c.logger.Warn("missing match_id or url", "id", msg.ID)
		_ = c.redisClient.AckFetchTask(ctx, pipeline.FetchTasksStream, msg.ID)
		return
	}

	task := models.FetchStreamTask{
		MatchID: matchID,
		URL:     url,
	}

	taskCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	proxy, leaseToken, err := c.redisClient.AcquireLeasedProxy(taskCtx, 2*time.Minute, 10)
	if err != nil {
		c.logger.Warn("no free proxy available", "match_id", matchID, "error", err)
		c.handleFailure(ctx, msg, task, "no_proxy", msg.RetryCount)
		return
	}
	defer func() {
		if err := c.redisClient.ReleaseProxyLease(context.Background(), proxy, leaseToken); err != nil {
			c.logger.Warn("release proxy lease failed", "proxy", proxy, "error", err)
		}
	}()

	if !isUsableProxyURL(proxy) {
		c.logger.Warn("invalid proxy URL", "proxy", proxy)
		_ = c.redisClient.RemoveProxy(ctx, proxy)
		c.handleFailure(ctx, msg, task, "invalid_proxy_url", msg.RetryCount)
		return
	}

	allowed, err := c.redisClient.AtomicRateLimit(taskCtx, proxy)
	if err != nil {
		c.logger.Warn("rate limit check failed", "proxy", proxy, "error", err)
		c.handleFailure(ctx, msg, task, "rate_limit_check_failed", msg.RetryCount)
		return
	}
	if !allowed {
		c.logger.Warn("proxy rate limited", "proxy", proxy)
		c.handleFailure(ctx, msg, task, "proxy_rate_limited", msg.RetryCount)
		return
	}

	resp, err := c.httpClient.Get(taskCtx, url, proxy)
	if err != nil {
		c.logger.Warn("fetch failed", "match_id", matchID, "proxy", proxy, "error", err)
		_ = c.redisClient.RecordProxyFailure(ctx, proxy, c.maxProxyFails)
		c.httpClient.RemoveProxy(proxy)
		c.handleFailure(ctx, msg, task, "fetch_failed", msg.RetryCount)
		return
	}

	if resp.StatusCode != 200 {
		c.logger.Warn("bad status", "match_id", matchID, "status", resp.StatusCode)
		_ = c.redisClient.RecordProxyFailure(ctx, proxy, c.maxProxyFails)
		c.httpClient.RemoveProxy(proxy)
		c.handleFailure(ctx, msg, task, fmt.Sprintf("bad_status_%d", resp.StatusCode), msg.RetryCount)
		return
	}

	if err := c.payloadStore.Save(ctx, matchID, resp.Body); err != nil {
		c.logger.Error("save raw payload", "match_id", matchID, "error", err)
		c.handleFailure(ctx, msg, task, "save_failed", msg.RetryCount)
		return
	}

	parseTask := models.ParseStreamTask{
		TaskID:  matchID,
		MatchID: matchID,
	}
	if err := c.redisClient.AddParseStreamTask(ctx, parseTask); err != nil {
		c.logger.Error("add parse task", "match_id", matchID, "error", err)
		c.handleFailure(ctx, msg, task, "add_parse_task_failed", msg.RetryCount)
		return
	}

	_ = c.redisClient.RecordProxySuccess(ctx, proxy)

	if err := c.redisClient.AckFetchTask(ctx, pipeline.FetchTasksStream, msg.ID); err != nil {
		c.logger.Error("failed to ack fetch task after success", "match_id", matchID, "error", err)
	}

	c.logger.Info("fetched and queued", "match_id", matchID, "proxy", proxy)
}

func (c *StreamCollector) handleFailure(ctx context.Context, msg models.StreamMessage, task models.FetchStreamTask, reason string, retryCount int) {
	retryCount++

	var err error
	if retryCount > c.maxRetries {
		c.logger.Warn("max retries exceeded, adding to DLQ", "match_id", task.MatchID, "reason", reason, "retries", retryCount)
		err = c.redisClient.AddFetchToDLQ(ctx, task, reason, retryCount)
	} else {
		backoff := time.Duration(min(retryCount*retryCount, 30)) * time.Second
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			c.logger.Warn("retry aborted due to context cancel", "match_id", task.MatchID)
			return
		case <-timer.C:
		}
		c.logger.Warn("retrying task", "match_id", task.MatchID, "reason", reason, "retry", retryCount, "backoff", backoff)
		err = c.redisClient.ReaddFetchStreamTask(ctx, task, retryCount)
	}

	if err != nil {
		c.logger.Error("failed to requeue/DLQ fetch task; leaving pending", "match_id", task.MatchID, "reason", reason, "error", err)
		return
	}

	if err := c.redisClient.AckFetchTask(ctx, pipeline.FetchTasksStream, msg.ID); err != nil {
		c.logger.Error("failed to ack fetch task after requeue/DLQ", "match_id", task.MatchID, "error", err)
	}
}