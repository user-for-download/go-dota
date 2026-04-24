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
		err := rdb.Close()
		if err != nil {
			log.Error("redis.Client", "error", err)
		}
	}(rdb)

	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}

	db, err := pgxpool.New(ctx, cfg.PostgresURL)
	if err != nil {
		log.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Error("failed to ping postgres", "error", err)
		os.Exit(1)
	}

	repo := postgres.NewRepositoryFromPool(db)

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
	err = srv.Shutdown(shutdown)
	if err != nil {
		return
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
		_, err := w.Write([]byte("redis unavailable"))
		if err != nil {
			return
		}
		return
	}

	if err := h.repo.Ping(ctx); err != nil {
		h.log.Warn("health check: postgres ping failed", "error", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, err := w.Write([]byte("postgres unavailable"))
		if err != nil {
			return
		}
		return
	}

	_, err := w.Write([]byte("OK"))
	if err != nil {
		return
	}
}

func (h *handler) metrics(w http.ResponseWriter, r *http.Request) {
	h.log.Info("metrics request received")
	ctx := r.Context()
	var out metricsOutput
	var errors []string

	if val, err := h.rdb.SCard(ctx, "proxy_pool").Result(); err != nil {
		h.log.Warn("metrics: SCard proxy_pool failed", "error", err)
		errors = append(errors, "redis: failed to get proxy count")
	} else {
		out.ValidProxies = val
	}

	if val, err := h.rdb.LLen(ctx, "fetch_queue").Result(); err != nil {
		h.log.Warn("metrics: LLen fetch_queue failed", "error", err)
		errors = append(errors, "redis: failed to get fetch_queue length")
	} else {
		out.RedisFetchQueue = val
	}

	if val, err := h.rdb.LLen(ctx, "parse_queue").Result(); err != nil {
		h.log.Warn("metrics: LLen parse_queue failed", "error", err)
		errors = append(errors, "redis: failed to get parse_queue length")
	} else {
		out.RedisParseQueue = val
	}

	if val, err := h.rdb.LLen(ctx, "failed_queue").Result(); err != nil {
		h.log.Warn("metrics: LLen failed_queue failed", "error", err)
		errors = append(errors, "redis: failed to get failed_queue length")
	} else {
		out.RedisFailedQueue = val
	}

	if val, err := h.rdb.LLen(ctx, "permanent_failed_queue").Result(); err != nil {
		h.log.Warn("metrics: LLen permanent_failed_queue failed", "error", err)
		errors = append(errors, "redis: failed to get permanent_failed_queue length")
	} else {
		out.RedisPermanentFailedQueue = val
	}

	if val, err := h.repo.CountUniqueExternalIDs(ctx); err != nil {
		h.log.Warn("metrics: CountUniqueExternalIDs failed", "error", err)
		errors = append(errors, "postgres: failed to get count")
	} else {
		out.PostgresCount = val
	}

	if len(errors) > 0 {
		out.Errors = errors
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(out)
	if err != nil {
		return
	}
}

type metricsOutput struct {
	ValidProxies              int64    `json:"valid_proxies"`
	RedisFetchQueue           int64    `json:"redis_fetch_queue"`
	RedisParseQueue           int64    `json:"redis_parse_queue"`
	RedisFailedQueue          int64    `json:"redis_failed_queue"`
	RedisPermanentFailedQueue int64    `json:"redis_permanent_failed_queue"`
	PostgresCount             int64    `json:"postgres_count"`
	Errors                    []string `json:"errors,omitempty"`
}
