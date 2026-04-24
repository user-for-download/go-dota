package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/user-for-download/go-dota/internal/models"
	postgresstore "github.com/user-for-download/go-dota/internal/storage/postgres"
	redisstore "github.com/user-for-download/go-dota/internal/storage/redis"
)

type Parser struct {
	redisClient *redisstore.Client
	pgRepo     *postgresstore.Repository
	numWorkers  int
	logger      *slog.Logger
	recoverDLQ  bool
	dlqBatchSize int
	dlqMaxPerTick int
}

func NewParser(
	redisClient *redisstore.Client,
	pgRepo *postgresstore.Repository,
	numWorkers int,
	logger *slog.Logger,
) *Parser {
	return &Parser{
		redisClient: redisClient,
		pgRepo:     pgRepo,
		numWorkers:  numWorkers,
		logger:     logger,
		recoverDLQ:  true,
		dlqBatchSize: 100,
		dlqMaxPerTick: 500,
	}
}

func NewParserWithConfig(
	redisClient *redisstore.Client,
	pgRepo *postgresstore.Repository,
	numWorkers int,
	logger *slog.Logger,
	dlqBatchSize, dlqMaxPerTick int,
) *Parser {
	return &Parser{
		redisClient: redisClient,
		pgRepo:     pgRepo,
		numWorkers:  numWorkers,
		logger:     logger,
		recoverDLQ:  true,
		dlqBatchSize: dlqBatchSize,
		dlqMaxPerTick: dlqMaxPerTick,
	}
}

func (p *Parser) Run(ctx context.Context) error {
	p.logger.Info("parser starting workers", "count", p.numWorkers)

	if p.recoverDLQ {
		p.logger.Info("attempting DLQ recovery on startup")
		count, err := p.redisClient.RequeueFailedTasks(ctx)
		if err != nil {
			p.logger.Warn("DLQ recovery failed", "error", err)
		} else if count > 0 {
			p.logger.Info("DLQ recovery succeeded", "requeued", count)
		}
	}

	go p.periodicDLQDrain(ctx)

	var wg sync.WaitGroup
	for i := 0; i < p.numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			p.worker(ctx, workerID)
		}(i)
	}

	<-ctx.Done()
	p.logger.Info("waiting for parser workers to finish processing...")
	wg.Wait()
	p.logger.Info("all parser workers stopped")
	return nil
}

func (p *Parser) periodicDLQDrain(ctx context.Context) {
	batchSize := p.dlqBatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	maxPerTick := p.dlqMaxPerTick
	if maxPerTick <= 0 {
		maxPerTick = 500
	}
	if maxPerTick < batchSize {
		maxPerTick = batchSize
	}

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			total := 0
			// Loop runs at most maxPerTick/batchSize batches per tick.
			// If maxPerTick < batchSize, maxPerTick is set to batchSize (see above), so loop executes at least once.
			for i := 0; i < maxPerTick/batchSize; i++ {
				count, err := p.redisClient.RequeueFailedTasksBatch(ctx, batchSize)
				if err != nil {
					p.logger.Warn("periodic DLQ drain failed", "error", err)
					break
				}
				if count == 0 {
					break
				}
				total += int(count)
			}
			if total > 0 {
				p.logger.Info("periodic DLQ drain succeeded", "requeued", total)
			}
		}
	}
}

func (p *Parser) worker(ctx context.Context, id int) {
	p.logger.Info("parser worker started", "worker_id", id)
	idleCounter := 0
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("parser worker stopping", "worker_id", id)
			return
		default:
		}

		taskID, err := p.redisClient.PopParseTask(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			idleCounter++
			if idleCounter%60 == 0 {
				p.logger.Warn("no tasks in queue, waiting...", "worker_id", id)
			}
			time.Sleep(time.Second)
			continue
		}
		idleCounter = 0

		p.processTask(ctx, taskID, id)
	}
}

func (p *Parser) processTask(ctx context.Context, taskID string, workerID int) {
	data, err := p.redisClient.GetRawData(ctx, taskID)
	if err != nil {
		p.logger.Error("get raw data failed", "task_id", taskID, "error", err)
		_ = p.redisClient.ExtendRawDataTTL(ctx, taskID)
		_ = p.redisClient.IncrementRetryCount(ctx, taskID)
		_ = p.redisClient.PushFailedTask(ctx, taskID)
		return
	}
	if data == nil {
		p.logger.Warn("raw data expired or missing, discarding task", "task_id", taskID)
		return
	}

	var apiResp models.APIResponse
	if err := json.Unmarshal(data, &apiResp); err != nil {
		p.logger.Error("unmarshal failed", "task_id", taskID, "error", err)
		_ = p.redisClient.ExtendRawDataTTL(ctx, taskID)
		_ = p.redisClient.IncrementRetryCount(ctx, taskID)
		_ = p.redisClient.PushFailedTask(ctx, taskID)
		return
	}

	if apiResp.ID == "" {
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(data, &raw); err == nil {
			if idBytes, ok := raw["id"]; ok {
				var idStr string
				if json.Unmarshal(idBytes, &idStr) == nil && idStr != "" {
					apiResp.ID = idStr
				}
			}
			if apiResp.ID == "" {
				if midBytes, ok := raw["match_id"]; ok {
					var idNum json.Number
					if json.Unmarshal(midBytes, &idNum) == nil {
						idInt, err := idNum.Int64()
						if err == nil {
							apiResp.ID = strconv.FormatInt(idInt, 10)
						}
					}
				}
			}
		}
	}

	if apiResp.ID == "" {
		p.logger.Error("no ID found in payload, discarding task (malformed data)", "task_id", taskID, "payload_size", len(data))
		_ = p.redisClient.DeleteRawData(ctx, taskID)
		_ = p.redisClient.DeleteRetryCount(ctx, taskID)
		return
	}

	if apiResp.Payload == nil {
		apiResp.Payload = data
	}

	if err := p.pgRepo.UpsertParsedData(ctx, apiResp.ID, apiResp.Payload); err != nil {
		p.logger.Error("db upsert failed", "task_id", taskID, "external_id", apiResp.ID, "error", err)
		_ = p.redisClient.ExtendRawDataTTL(ctx, taskID)
		_ = p.redisClient.IncrementRetryCount(ctx, taskID)
		_ = p.redisClient.PushFailedTask(ctx, taskID)
		return
	}

	if err := p.redisClient.DeleteRawData(ctx, taskID); err != nil {
		p.logger.Warn("delete raw data failed", "task_id", taskID, "error", err)
	}

	_ = p.redisClient.DeleteRetryCount(ctx, taskID)

	p.logger.Info("task parsed and stored",
		"task_id", taskID, "external_id", apiResp.ID, "worker_id", workerID)
}