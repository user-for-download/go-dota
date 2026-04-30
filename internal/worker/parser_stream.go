package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/user-for-download/go-dota/internal/config"
	"github.com/user-for-download/go-dota/internal/models"
	"github.com/user-for-download/go-dota/internal/pipeline"
	"github.com/user-for-download/go-dota/internal/storage/postgres"
	"github.com/user-for-download/go-dota/internal/storage/redis"
)

type StreamParser struct {
	redisClient   *redis.Client
	repo          *postgres.Repository
	payloadStore  PayloadStore
	logger        *slog.Logger
	maxRetries    int
	batchSize     int
	blockMS        int
	workerID      string
}

func NewStreamParser(
	redisClient *redis.Client,
	repo *postgres.Repository,
	payloadStore PayloadStore,
	logger *slog.Logger,
	cfg *config.Config,
) *StreamParser {
	return &StreamParser{
		redisClient:  redisClient,
		repo:         repo,
		payloadStore: payloadStore,
		logger:       logger,
		maxRetries:   cfg.MaxRetries,
		batchSize:    cfg.StreamBatchSize,
		blockMS:      cfg.StreamBlockMS,
		workerID:     fmt.Sprintf("parser-%d", rand.Intn(10000)),
	}
}

func (p *StreamParser) Run(ctx context.Context) error {
	if err := p.redisClient.EnsureStreamGroups(ctx); err != nil {
		return fmt.Errorf("ensure stream groups: %w", err)
	}

	p.logger.Info("stream parser started", "worker_id", p.workerID)

	staleClaimTicker := time.NewTicker(5 * time.Minute)
	defer staleClaimTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-staleClaimTicker.C:
			p.recoverStaleTasks(ctx)
		default:
		}

		msgs, err := p.redisClient.ReadParseTasks(ctx, pipeline.ParserGroup, p.workerID, int64(p.batchSize), p.blockMS)
		if err != nil {
			p.logger.Error("read parse tasks", "error", err)
			time.Sleep(time.Second)
			continue
		}

		if len(msgs) == 0 {
			continue
		}

		for _, msg := range msgs {
			p.processParseMessage(ctx, msg)
		}
	}
}

func (p *StreamParser) recoverStaleTasks(ctx context.Context) {
	staleMsgs, err := p.redisClient.ClaimStaleParseTasks(ctx, p.workerID, 5*time.Minute, int64(p.batchSize))
	if err != nil {
		p.logger.Error("claim stale parse tasks failed", "error", err)
		return
	}

	if len(staleMsgs) == 0 {
		return
	}

	p.logger.Info("recovered stale pending tasks", "count", len(staleMsgs))
	for _, msg := range staleMsgs {
		p.processParseMessage(ctx, msg)
	}
}

func (p *StreamParser) processParseMessage(ctx context.Context, msg models.StreamMessage) {
	data, ok := msg.Task.(map[string]interface{})
	if !ok {
		p.logger.Warn("invalid task format", "id", msg.ID)
		_ = p.redisClient.AckParseTask(ctx, pipeline.ParseTasksStream, msg.ID)
		return
	}

	taskID, _ := data["task_id"].(string)
	matchID, _ := data["match_id"].(string)

	if taskID == "" {
		p.logger.Warn("missing task_id", "id", msg.ID)
		_ = p.redisClient.AckParseTask(ctx, pipeline.ParseTasksStream, msg.ID)
		return
	}

	task := models.ParseStreamTask{
		TaskID:  taskID,
		MatchID: matchID,
	}

	payload, err := p.payloadStore.Get(ctx, taskID)
	if err != nil || payload == nil {
		p.logger.Warn("payload not found", "task_id", taskID, "error", err)
		p.handleFailure(ctx, msg, task, "payload_not_found", msg.RetryCount)
		return
	}

	_ = p.payloadStore.Extend(ctx, taskID)

	var match models.Match
	if err := json.Unmarshal(payload, &match); err != nil {
		p.logger.Error("unmarshal match", "task_id", taskID, "error", err)
		_ = p.redisClient.IncrParseFailure(ctx, "json_unmarshal")
		p.sendToDLQ(ctx, msg, task, "json_unmarshal", msg.RetryCount)
		return
	}

	if err := match.Validate(); err != nil {
		p.logger.Error("validate match", "task_id", taskID, "error", err)
		_ = p.redisClient.IncrParseFailure(ctx, "validation")
		p.sendToDLQ(ctx, msg, task, "validation_failed", msg.RetryCount)
		return
	}

	if err := p.repo.IngestMatch(ctx, &match); err != nil {
		p.logger.Error("ingest match", "match_id", match.MatchID, "error", err)
		kind := postgres.ClassifyIngestError(err)
		_ = p.redisClient.IncrIngestFailure(ctx, string(kind), err.Error(), match.MatchID)
		switch kind {
		case postgres.IngestErrSerialization, postgres.IngestErrDeadlock, postgres.IngestErrLocked:
			p.handleFailure(ctx, msg, task, "db_ingest_failed", msg.RetryCount)
		case postgres.IngestErrOther:
			p.handleFailure(ctx, msg, task, "db_ingest_transient", msg.RetryCount)
		default:
			p.sendToDLQ(ctx, msg, task, string(kind), msg.RetryCount)
		}
		return
	}

	_ = p.payloadStore.Delete(ctx, taskID)
	if err := p.redisClient.AckParseTask(ctx, pipeline.ParseTasksStream, msg.ID); err != nil {
		p.logger.Error("failed to ack parse task after success", "task_id", taskID, "match_id", matchID, "error", err)
	}
	_ = p.redisClient.IncrIngestSuccess(ctx)

	if matchID != "" {
		_ = p.redisClient.UnmarkFetchIDSeen(ctx, matchID)
	}

	p.logger.Info("parsed and ingested", "match_id", match.MatchID)
}

func (p *StreamParser) handleFailure(ctx context.Context, msg models.StreamMessage, task models.ParseStreamTask, reason string, retryCount int) {
	retryCount++

	var err error
	if retryCount > p.maxRetries {
		p.logger.Warn("max retries exceeded, adding to DLQ", "task_id", task.TaskID, "reason", reason, "retries", retryCount)
		err = p.redisClient.AddParseToDLQ(ctx, task, reason, retryCount)
	} else {
		backoff := time.Duration(min(retryCount*retryCount, 30)) * time.Second
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			p.logger.Warn("retry aborted due to context cancel", "task_id", task.TaskID)
			return
		case <-timer.C:
		}
		p.logger.Warn("retrying task", "task_id", task.TaskID, "reason", reason, "retry", retryCount, "backoff", backoff)
		_ = p.payloadStore.Extend(ctx, task.TaskID)
		err = p.redisClient.ReaddParseStreamTask(ctx, task, retryCount)
	}

	if err != nil {
		p.logger.Error("failed to requeue/DLQ parse task; leaving pending", "task_id", task.TaskID, "reason", reason, "error", err)
		return
	}

	if err := p.redisClient.AckParseTask(ctx, pipeline.ParseTasksStream, msg.ID); err != nil {
		p.logger.Error("failed to ack parse task after requeue/DLQ", "task_id", task.TaskID, "error", err)
	}
}

func (p *StreamParser) sendToDLQ(ctx context.Context, msg models.StreamMessage, task models.ParseStreamTask, reason string, retryCount int) {
	p.logger.Warn("permanent failure, sending to DLQ", "task_id", task.TaskID, "reason", reason)

	err := p.redisClient.AddParseToDLQ(ctx, task, reason, retryCount)
	if err != nil {
		p.logger.Error("failed to add to DLQ", "task_id", task.TaskID, "error", err)
		return
	}

	_ = p.payloadStore.Delete(ctx, task.TaskID)

	if err := p.redisClient.AckParseTask(ctx, pipeline.ParseTasksStream, msg.ID); err != nil {
		p.logger.Error("failed to ack parse task after DLQ", "task_id", task.TaskID, "error", err)
	}
}