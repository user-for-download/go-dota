package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/user-for-download/go-dota/internal/config"
	"github.com/user-for-download/go-dota/internal/logger"
	"github.com/user-for-download/go-dota/internal/storage/postgres"
)

func main() {
	log := logger.Init()
	cfg, err := config.Load()
	if err != nil {
		log.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		log.Error("invalid redis url", "error", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(opts)
	defer func(rdb *redis.Client) {
		if err := rdb.Close(); err != nil {
			log.Error("redis.Client close", "error", err)
		}
	}(rdb)

	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}

	mainPool, err := pgxpool.New(ctx, cfg.PostgresURL)
	if err != nil {
		log.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer mainPool.Close()

	if err := mainPool.Ping(ctx); err != nil {
		log.Error("failed to ping postgres", "error", err)
		os.Exit(1)
	}

	repo := postgres.NewRepositoryFromPool(mainPool)

	h := &handler{repo: repo, rdb: rdb, log: log}
	srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.MonitorPort), Handler: h.routes()}

	go func() {
		log.Info("starting monitor", "port", cfg.MonitorPort)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "error", err)
		}
	}()

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdown); err != nil {
		log.Error("server shutdown error", "error", err)
	}
}

type handler struct {
	repo *postgres.Repository
	rdb  *redis.Client
	log  *slog.Logger
}

func (h *handler) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("GET /metrics", h.metrics)
	return mux
}

func (h *handler) health(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if err := h.rdb.Ping(ctx).Err(); err != nil {
		h.log.Warn("health check: redis ping failed", "error", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("redis unavailable"))
		return
	}

	if err := h.repo.Ping(ctx); err != nil {
		h.log.Warn("health check: postgres ping failed", "error", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("postgres unavailable"))
		return
	}

	_, _ = w.Write([]byte("OK"))
}

type metricsOutput struct {
	RedisFetchQueue           int64            `json:"redis_fetch_queue"`
	RedisParseQueue           int64            `json:"redis_parse_queue"`
	RedisFailedQueue          int64            `json:"redis_failed_queue"`
	RedisPermanentFailedQueue int64           `json:"redis_permanent_failed_queue"`
	MatchesCount              int64            `json:"matches_count"`
	PlayerMatchesCount        int64            `json:"player_matches_count"`
	ParsedMatchesCount        int64            `json:"parsed_matches_count"`
	FetcherLastRunTS          int64            `json:"fetcher_last_run_ts,omitempty"`
	FetcherLastRunDiscovered  int              `json:"fetcher_last_run_discovered,omitempty"`
	FetcherLastRunNewInDB     int              `json:"fetcher_last_run_new_in_db,omitempty"`
	FetcherLastRunPushed      int              `json:"fetcher_last_run_pushed,omitempty"`
	ParserRetryCountAvg       float64          `json:"parser_retry_count_avg,omitempty"`
	DLQOldestAgeSeconds       int64            `json:"dlq_oldest_age_seconds,omitempty"`
	IngestSuccessTotal        int64            `json:"ingest_success_total"`
	IngestFailedTotal         int64            `json:"ingest_failed_total"`
	ParseFailedTotal          int64            `json:"parse_failed_total"`
	IngestFailedByKind        map[string]int64 `json:"ingest_failed_by_kind,omitempty"`
	LastFailedSample          string           `json:"last_failed_sample,omitempty"`
	Errors                    []string         `json:"errors,omitempty"`
}

func (h *handler) metrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")

	out := metricsOutput{}
	var errs []string

	pipe := h.rdb.Pipeline()
	lastRun := pipe.Get(ctx, "fetcher:last_run")
	fetchQ := pipe.LLen(ctx, "fetch_queue")
	parseQ := pipe.LLen(ctx, "parse_queue")
	failedQ := pipe.LLen(ctx, "failed_queue")
	permFailedQ := pipe.LLen(ctx, "permanent_failed_queue")

	_, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		h.log.Warn("metrics: redis pipeline error", "error", err)
	}

	if data, err := lastRun.Bytes(); err == nil {
		var stats struct {
			TS         int64 `json:"ts"`
			Discovered int   `json:"discovered"`
			NewInDB    int   `json:"new_in_db"`
			Pushed     int   `json:"pushed"`
		}
		if json.Unmarshal(data, &stats) == nil {
			out.FetcherLastRunTS = stats.TS
			out.FetcherLastRunDiscovered = stats.Discovered
			out.FetcherLastRunNewInDB = stats.NewInDB
			out.FetcherLastRunPushed = stats.Pushed
		}
	}

	if val, err := fetchQ.Result(); err == nil {
		out.RedisFetchQueue = val
	}
	if val, err := parseQ.Result(); err == nil {
		out.RedisParseQueue = val
	}
	if val, err := failedQ.Result(); err == nil {
		out.RedisFailedQueue = val
	}
	if val, err := permFailedQ.Result(); err == nil {
		out.RedisPermanentFailedQueue = val
	}

	// Calculate retry count average from retry_count:* keys
	retryCountAvg, oldestAge, err := h.calculateRetryMetrics(ctx)
	if err != nil {
		h.log.Warn("metrics: calculate retry metrics failed", "error", err)
	} else {
		out.ParserRetryCountAvg = retryCountAvg
		out.DLQOldestAgeSeconds = oldestAge
	}

	if total, parsed, players, err := h.repo.CountMatches(ctx); err != nil {
		h.log.Warn("metrics: CountMatches failed", "error", err)
		errs = append(errs, "postgres: failed to count matches")
	} else {
		out.MatchesCount = total
		out.ParsedMatchesCount = parsed
		out.PlayerMatchesCount = players
	}

	if len(errs) > 0 {
		out.Errors = errs
		w.WriteHeader(http.StatusInternalServerError)
	}

	ingestPipe := h.rdb.Pipeline()
	sucCmd := ingestPipe.Get(ctx, "metrics:ingest_success_total")
	failCmd := ingestPipe.Get(ctx, "metrics:ingest_failed_total")
	parseFailCmd := ingestPipe.Get(ctx, "metrics:parse_failed_total")
	byKindCmd := ingestPipe.HGetAll(ctx, "metrics:ingest_failed_by_kind")
	sampleCmd := ingestPipe.Get(ctx, "metrics:last_failed_sample")
	_, _ = ingestPipe.Exec(ctx)

	out.IngestSuccessTotal, _ = sucCmd.Int64()
	out.IngestFailedTotal, _ = failCmd.Int64()
	out.ParseFailedTotal, _ = parseFailCmd.Int64()
	if vals, err := byKindCmd.Result(); err == nil && len(vals) > 0 {
		out.IngestFailedByKind = make(map[string]int64, len(vals))
		for k, v := range vals {
			n, _ := strconv.ParseInt(v, 10, 64)
			out.IngestFailedByKind[k] = n
		}
	}
	out.LastFailedSample, _ = sampleCmd.Result()

	resp, _ := json.Marshal(out)
	_, _ = w.Write(resp)
}

func (h *handler) calculateRetryMetrics(ctx context.Context) (avg float64, oldestAge int64, err error) {
	const retryKeyPrefix = "retry_count:"
	const retryTTL = 86400 // seconds, matches queue.go retryCountTTL

	var totalRetry int64
	var count int64
	var oldestTTL int64 = -1

	// Use SCAN to find all retry_count:* keys (capped for performance)
	iter := h.rdb.Scan(ctx, 0, retryKeyPrefix+"*", 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()

		// Get retry count
		retryStr, err := h.rdb.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		retryVal, err := strconv.ParseInt(retryStr, 10, 64)
		if err != nil {
			continue
		}
		totalRetry += retryVal
		count++

		// Get TTL for oldest calculation
		ttl, err := h.rdb.TTL(ctx, key).Result()
		if err != nil {
			continue
		}
		if ttl > 0 {
			age := retryTTL - int64(ttl.Seconds())
			if oldestTTL < 0 || age > oldestTTL {
				oldestTTL = age
			}
		}
	}
	if err := iter.Err(); err != nil {
		return 0, 0, err
	}

	if count > 0 {
		avg = float64(totalRetry) / float64(count)
	}
	if oldestTTL > 0 {
		oldestAge = oldestTTL
	}
	return avg, oldestAge, nil
}
