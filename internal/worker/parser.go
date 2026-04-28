package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/user-for-download/go-dota/internal/models"
	postgresstore "github.com/user-for-download/go-dota/internal/storage/postgres"
	redisstore "github.com/user-for-download/go-dota/internal/storage/redis"
)

type Parser struct {
	redisClient   *redisstore.Client
	pgRepo        *postgresstore.Repository
	numWorkers    int
	logger        *slog.Logger
	dlqBatchSize  int
	dlqMaxPerTick int
}

func NewParser(
	redisClient *redisstore.Client,
	pgRepo *postgresstore.Repository,
	numWorkers int,
	logger *slog.Logger,
	dlqBatchSize, dlqMaxPerTick int,
) *Parser {
	return &Parser{
		redisClient:   redisClient,
		pgRepo:        pgRepo,
		numWorkers:    numWorkers,
		logger:        logger,
		dlqBatchSize:  dlqBatchSize,
		dlqMaxPerTick: dlqMaxPerTick,
	}
}

func (p *Parser) Run(ctx context.Context) error {
	p.logger.Info("parser starting workers", "count", p.numWorkers)

	p.logger.Info("attempting DLQ recovery on startup")
	count, err := p.redisClient.RequeueFailedTasks(ctx)
	if err != nil {
		p.logger.Warn("DLQ recovery failed", "error", err)
	} else if count > 0 {
		p.logger.Info("DLQ recovery succeeded", "requeued", count)
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
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}
		idleCounter = 0

		p.processTask(ctx, taskID, id)
	}
}

func (p *Parser) processTask(ctx context.Context, taskID string, workerID int) {
	log := p.logger.With("task_id", taskID, "worker_id", workerID)

	data, err := p.redisClient.GetRawData(ctx, taskID)
	if err != nil {
		log.Error("get raw data failed", "error", err)
		_ = p.redisClient.ExtendRawDataTTL(ctx, taskID)
		_ = p.redisClient.IncrementRetryCount(ctx, taskID)
		_ = p.redisClient.PushFailedTask(ctx, taskID)
		return
	}
	if data == nil {
		log.Warn("raw data expired or missing, discarding task")
		return
	}

	var m models.Match
	if err := json.Unmarshal(data, &m); err != nil {
		log.Error("unmarshal match payload failed (poison pill)", "error", err)
		_ = p.redisClient.PushPermanentFailedTask(ctx, taskID)
		_ = p.redisClient.DeleteRetryCount(ctx, taskID)
		return
	}

	if err := m.Validate(); err != nil {
		log.Error("match validation failed (poison pill)", "match_id", m.MatchID, "error", err)
		_ = p.redisClient.PushPermanentFailedTask(ctx, taskID)
		_ = p.redisClient.DeleteRetryCount(ctx, taskID)
		return
	}

	if err := p.pgRepo.IngestMatch(ctx, &m); err != nil {
		log.Error("ingest match failed", "match_id", m.MatchID, "error", err)

		if isPermanentIngestError(err) {
			_ = p.redisClient.PushPermanentFailedTask(ctx, taskID)
			_ = p.redisClient.DeleteRetryCount(ctx, taskID)
			return
		}

		_ = p.redisClient.ExtendRawDataTTL(ctx, taskID)
		_ = p.redisClient.IncrementRetryCount(ctx, taskID)
		_ = p.redisClient.PushFailedTask(ctx, taskID)
		return
	}

	if err := p.redisClient.DeleteRawData(ctx, taskID); err != nil {
		log.Warn("delete raw data failed", "error", err)
	}
	_ = p.redisClient.DeleteRetryCount(ctx, taskID)

	log.Info("match ingested", "match_id", m.MatchID, "is_parsed", m.IsParsed(), "players", len(m.Players))
}

func isPermanentIngestError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23514", "23502": // check_violation, not_null_violation
			return true
		}
		// 23503 (FK violation) is transient: enricher will populate missing
		// lookup rows on its next pass. Failed tasks go to retry queue.
	}
	return false
}
