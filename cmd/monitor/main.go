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
	"github.com/user-for-download/go-dota/internal/pipeline"
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
	RedisFetchStreamLen    int64 `json:"redis_fetch_stream_len"`
	RedisParseStreamLen    int64 `json:"redis_parse_stream_len"`
	FetchPendingCount     int64 `json:"fetch_pending_count"`
	ParsePendingCount     int64 `json:"parse_pending_count"`
	FetchDLQLen           int64 `json:"fetch_dlq_len"`
	ParseDLQLen           int64 `json:"parse_dlq_len"`
	MatchesCount          int64 `json:"matches_count"`
	PlayerMatchesCount    int64 `json:"player_matches_count"`
	ParsedMatchesCount    int64 `json:"parsed_matches_count"`
	FetcherLastRunTS       int64 `json:"fetcher_last_run_ts,omitempty"`
	FetcherLastRunDiscovered int  `json:"fetcher_last_run_discovered,omitempty"`
	FetcherLastRunNewInDB int  `json:"fetcher_last_run_new_in_db,omitempty"`
	FetcherLastRunPushed   int  `json:"fetcher_last_run_pushed,omitempty"`
	IngestSuccessTotal    int64 `json:"ingest_success_total"`
	IngestFailedTotal     int64 `json:"ingest_failed_total"`
	ParseFailedTotal      int64 `json:"parse_failed_total"`
	IngestFailedByKind    map[string]int64 `json:"ingest_failed_by_kind,omitempty"`
	LastFailedSample      string           `json:"last_failed_sample,omitempty"`
	Errors                []string         `json:"errors,omitempty"`
}

func (h *handler) metrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")

	out := metricsOutput{}
	var errs []string

	pipe := h.rdb.Pipeline()
	lastRun := pipe.Get(ctx, "fetcher:last_run")
	fetchStreamLen := pipe.XLen(ctx, pipeline.FetchTasksStream)
	parseStreamLen := pipe.XLen(ctx, pipeline.ParseTasksStream)
	fetchDLQLen := pipe.XLen(ctx, pipeline.FetchDLQStream)
	parseDLQLen := pipe.XLen(ctx, pipeline.ParseDLQStream)

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

	if val, err := fetchStreamLen.Result(); err == nil {
		out.RedisFetchStreamLen = val
	}
	if val, err := parseStreamLen.Result(); err == nil {
		out.RedisParseStreamLen = val
	}
	if val, err := fetchDLQLen.Result(); err == nil {
		out.FetchDLQLen = val
	}
	if val, err := parseDLQLen.Result(); err == nil {
		out.ParseDLQLen = val
	}

	fetchPending, parsePending := h.getPendingCounts(ctx)
	out.FetchPendingCount = fetchPending
	out.ParsePendingCount = parsePending

	if total, parsed, players, err := h.repo.CountMatches(ctx); err != nil {
		h.log.Warn("metrics: CountMatches failed", "error", err)
		errs = append(errs, "postgres: failed to count matches")
	} else {
		out.MatchesCount = total
		out.ParsedMatchesCount = parsed
		out.PlayerMatchesCount = players
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

	if len(errs) > 0 {
		out.Errors = errs
		resp, _ := json.Marshal(out)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(resp)
		return
	}

	resp, _ := json.Marshal(out)
	_, _ = w.Write(resp)
}

func (h *handler) getPendingCounts(ctx context.Context) (fetchPending, parsePending int64) {
	fetchResult := h.rdb.XPending(ctx, pipeline.FetchTasksStream, pipeline.CollectorGroup)
	if p, err := fetchResult.Result(); err == nil {
		fetchPending = p.Count
	}
	parseResult := h.rdb.XPending(ctx, pipeline.ParseTasksStream, pipeline.ParserGroup)
	if p, err := parseResult.Result(); err == nil {
		parsePending = p.Count
	}
	return fetchPending, parsePending
}
