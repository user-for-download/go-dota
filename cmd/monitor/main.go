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
	RedisFetchQueue            int64   `json:"redis_fetch_queue"`
	RedisParseQueue           int64   `json:"redis_parse_queue"`
	RedisFailedQueue          int64   `json:"redis_failed_queue"`
	RedisPermanentFailedQueue int64   `json:"redis_permanent_failed_queue"`
	MatchesCount              int64   `json:"matches_count"`
	PlayerMatchesCount        int64   `json:"player_matches_count"`
	ParsedMatchesCount        int64   `json:"parsed_matches_count"`
	FetcherLastRunTS          int64   `json:"fetcher_last_run_ts,omitempty"`
	FetcherLastRunDiscovered int     `json:"fetcher_last_run_discovered,omitempty"`
	FetcherLastRunNewInDB    int     `json:"fetcher_last_run_new_in_db,omitempty"`
	FetcherLastRunPushed    int     `json:"fetcher_last_run_pushed,omitempty"`
	Errors                   []string `json:"errors,omitempty"`
}

func (h *handler) metrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")

	out := metricsOutput{}
	var errs []string

	if data, err := h.rdb.Get(ctx, "fetcher:last_run").Bytes(); err == nil {
		var stats struct {
			TS         int64 `json:"ts"`
			Discovered int   `json:"discovered"`
			NewInDB   int   `json:"new_in_db"`
			Pushed    int   `json:"pushed"`
		}
		if json.Unmarshal(data, &stats) == nil {
			out.FetcherLastRunTS = stats.TS
			out.FetcherLastRunDiscovered = stats.Discovered
			out.FetcherLastRunNewInDB = stats.NewInDB
			out.FetcherLastRunPushed = stats.Pushed
		}
	}

if val, err := h.rdb.LLen(ctx, "fetch_queue").Result(); err != nil {
		h.log.Warn("metrics: fetch_queue len failed", "error", err)
		errs = append(errs, "redis: fetch_queue")
	} else {
		out.RedisFetchQueue = val
	}

	if val, err := h.rdb.LLen(ctx, "parse_queue").Result(); err != nil {
		h.log.Warn("metrics: parse_queue len failed", "error", err)
		errs = append(errs, "redis: parse_queue")
	} else {
		out.RedisParseQueue = val
	}

	if val, err := h.rdb.LLen(ctx, "failed_queue").Result(); err != nil {
		h.log.Warn("metrics: failed_queue len failed", "error", err)
		errs = append(errs, "redis: failed_queue")
	} else {
		out.RedisFailedQueue = val
	}

	if val, err := h.rdb.LLen(ctx, "permanent_failed_queue").Result(); err != nil {
		h.log.Warn("metrics: permanent_failed_queue len failed", "error", err)
		errs = append(errs, "redis: permanent_failed_queue")
	} else {
		out.RedisPermanentFailedQueue = val
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

	resp, _ := json.Marshal(out)
	_, _ = w.Write(resp)
}
