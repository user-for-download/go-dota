package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/user-for-download/go-dota/internal/config"
	"github.com/user-for-download/go-dota/internal/httpx"
	"github.com/user-for-download/go-dota/internal/models"
	"github.com/user-for-download/go-dota/internal/pipeline"
	"github.com/user-for-download/go-dota/internal/storage/postgres"
	"github.com/user-for-download/go-dota/internal/storage/redis"
)

type StreamFetcher struct {
	redisClient   *redis.Client
	repo          *postgres.Repository
	sqlPath       string
	logger        *slog.Logger
	httpClient    *httpx.ProxiedClient
	maxQueueSize int64
	maxProxyFails int
}

func NewStreamFetcher(
	redisClient *redis.Client,
	repo *postgres.Repository,
	sqlPath string,
	logger *slog.Logger,
	cfg *config.Config,
) *StreamFetcher {
	opts := httpx.DefaultOptions()
	opts.SkipTLSVerify = cfg.SkipTLSVerify
	pool := httpx.NewTransportPool(opts)
	maxProxyFails := cfg.MaxProxyFails
	if maxProxyFails <= 0 {
		maxProxyFails = 3
	}
	return &StreamFetcher{
		redisClient:   redisClient,
		repo:          repo,
		sqlPath:       sqlPath,
		logger:        logger,
		httpClient:    httpx.NewProxiedClient(pool, 60*time.Second),
		maxQueueSize: cfg.MaxQueueSize,
		maxProxyFails: maxProxyFails,
	}
}

type explorerResponse struct {
	Rows []map[string]interface{} `json:"rows"`
}

func extractMatchIDsFromRow(row map[string]interface{}) []int64 {
	for _, key := range []string{"match_id", "match_ids"} {
		if v, ok := row[key]; ok {
			if ids := extractIDsFromValue(v); len(ids) > 0 {
				return ids
			}
		}
	}

	var ids []int64
	for _, v := range row {
		ids = append(ids, extractIDsFromValue(v)...)
	}
	return ids
}

func extractIDsFromValue(v any) []int64 {
	var extracted []int64
	switch val := v.(type) {
	case float64:
		extracted = []int64{int64(val)}
	case string:
		for _, idStr := range strings.Split(val, ",") {
			idStr = strings.TrimSpace(idStr)
			if idStr == "" {
				continue
			}
			if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
				extracted = append(extracted, id)
			}
		}
	case []interface{}:
		for _, item := range val {
			switch inner := item.(type) {
			case float64:
				extracted = append(extracted, int64(inner))
			case string:
				if id, err := strconv.ParseInt(strings.TrimSpace(inner), 10, 64); err == nil {
					extracted = append(extracted, id)
				}
			}
		}
	}
	return extracted
}

func (f *StreamFetcher) Run(ctx context.Context) error {
	sql, err := f.loadQuery()
	if err != nil {
		return fmt.Errorf("load query: %w", err)
	}

	f.logger.Info("stream fetcher started", "path", f.sqlPath)

	if err := f.waitForProxies(ctx, 5*time.Minute); err != nil {
		return fmt.Errorf("waiting for proxies: %w", err)
	}

	matchIDs, err := f.fetchMatchIDs(ctx, sql)
	if err != nil {
		return fmt.Errorf("fetch match IDs: %w", err)
	}

	f.logger.Info("discovered match IDs", "count", len(matchIDs))

	unknownIDs, err := f.repo.FilterUnknownMatchIDs(ctx, matchIDs)
	if err != nil {
		return fmt.Errorf("filter match ids: %w", err)
	}

	f.logger.Info("starting task push")

	streamLen, err := f.redisClient.Instance().XLen(ctx, pipeline.FetchTasksStream).Result()
	if err == nil && streamLen > f.maxQueueSize {
		f.logger.Warn("queue full, throttling", "len", streamLen, "max", f.maxQueueSize)
		return fmt.Errorf("queue full: %d > %d", streamLen, f.maxQueueSize)
	}

	pushed := 0
	pushedIDs := make([]string, 0, len(unknownIDs))
	for _, id := range unknownIDs {
		idStr := fmt.Sprintf("%d", id)

		seen, err := f.redisClient.IsFetchIDSeen(ctx, idStr)
		if err != nil {
			f.logger.Warn("IsFetchIDSeen failed, pushing anyway", "id", idStr, "error", err)
		} else if seen {
			continue
		}

		task := models.FetchStreamTask{
			MatchID: idStr,
			URL:     fmt.Sprintf("https://api.opendota.com/api/matches/%d", id),
		}

		if err := f.redisClient.AddFetchStreamTask(ctx, task); err != nil {
			f.logger.Error("add fetch task failed", "match_id", id, "error", err)
			continue
		}

		pushed++
		pushedIDs = append(pushedIDs, idStr)
	}

	if len(pushedIDs) > 0 {
		if err := f.redisClient.MarkFetchIDSeenBatch(ctx, pushedIDs); err != nil {
			f.logger.Warn("MarkFetchIDSeenBatch failed", "error", err)
		}
	}

	f.logger.Info("tasks pushed to stream", "count", pushed)
	f.recordStats(ctx, len(matchIDs), len(unknownIDs), pushed)
	return nil
}

func (f *StreamFetcher) recordStats(ctx context.Context, discovered, newInDB, pushed int) {
	stats := map[string]any{
		"ts":         time.Now().Unix(),
		"discovered": discovered,
		"new_in_db":  newInDB,
		"pushed":     pushed,
	}
	b, err := json.Marshal(stats)
	if err != nil {
		return
	}
	_ = f.redisClient.Instance().Set(ctx, "fetcher:last_run", string(b), 24*time.Hour).Err()
}

func (f *StreamFetcher) loadQuery() (string, error) {
	data, err := f.sqlFile()
	if err != nil {
		return "", fmt.Errorf("read query file %s: %w", f.sqlPath, err)
	}
	sql := string(data)
	sql = strings.ReplaceAll(sql, "\n", " ")
	for strings.Contains(sql, "  ") {
		sql = strings.ReplaceAll(sql, "  ", " ")
	}
	sql = strings.TrimSpace(sql)
	return "https://api.opendota.com/api/explorer?sql=" + url.QueryEscape(sql), nil
}

func (f *StreamFetcher) sqlFile() ([]byte, error) {
	return os.ReadFile(f.sqlPath)
}

func (f *StreamFetcher) waitForProxies(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n, err := f.redisClient.GetProxyCount(ctx)
		if err == nil && n > 0 {
			f.logger.Info("proxy pool ready", "count", n)
			return nil
		}
		f.logger.Info("waiting for proxy pool to populate...")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	return fmt.Errorf("proxy pool not populated within %s", timeout)
}

func (f *StreamFetcher) fetchMatchIDs(ctx context.Context, targetURL string) ([]int64, error) {
	var matchIDs []int64

	f.logger.Info("starting fetchMatchIDs loop", "target", targetURL)
	for attempt := 0; attempt < 5; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		proxy, leaseToken, err := f.redisClient.AcquireLeasedProxy(ctx, 2*time.Minute, 10)
		if err != nil {
			f.logger.Warn("no free proxy available", "attempt", attempt, "error", err)
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}

		releaseLease := func() {
			if err := f.redisClient.ReleaseProxyLease(context.Background(), proxy, leaseToken); err != nil {
				f.logger.Warn("release proxy lease failed", "proxy", proxy, "error", err)
			}
		}

		f.logger.Info("fetching with proxy", "attempt", attempt, "proxy", proxy)

		allowed, err := f.redisClient.AtomicRateLimit(ctx, proxy)
		if err != nil {
			releaseLease()
			f.logger.Warn("rate limit check failed", "proxy", proxy, "error", err)
			continue
		}
		if !allowed {
			releaseLease()
			f.logger.Warn("proxy rate limited", "proxy", proxy)
			continue
		}

		resp, err := f.httpClient.Get(ctx, targetURL, proxy)
		if err != nil {
			releaseLease()
			f.httpClient.CloseIdleConnections(proxy)
			f.httpClient.RemoveProxy(proxy)
			_ = f.redisClient.RecordProxyFailure(ctx, proxy, f.maxProxyFails)
			f.logger.Warn("fetch failed, trying next proxy", "proxy", proxy, "error", err)
			continue
		}

		if resp.StatusCode != 200 {
			releaseLease()
			f.httpClient.CloseIdleConnections(proxy)
			f.httpClient.RemoveProxy(proxy)
			_ = f.redisClient.RecordProxyFailure(ctx, proxy, f.maxProxyFails)
			continue
		}

		var explorer explorerResponse
		if err := json.Unmarshal(resp.Body, &explorer); err != nil {
			releaseLease()
			f.httpClient.RemoveProxy(proxy)
			_ = f.redisClient.RecordProxyFailure(ctx, proxy, f.maxProxyFails)
			f.logger.Warn("fetch failed with bad json, trying next proxy", "proxy", proxy)
			continue
		}

		if len(explorer.Rows) == 0 {
			f.logger.Info("explorer returned empty rows (valid state)", "proxy", proxy)
			_ = f.redisClient.RecordProxySuccess(ctx, proxy)
			releaseLease()
			return nil, nil
		}

		seen := make(map[int64]bool)
		for _, row := range explorer.Rows {
			foundIDs := extractMatchIDsFromRow(row)
			for _, id := range foundIDs {
				if !seen[id] {
					seen[id] = true
					matchIDs = append(matchIDs, id)
				}
			}
		}

		f.logger.Info("fetch succeeded", "proxy", proxy, "match_count", len(matchIDs))
		_ = f.redisClient.RecordProxySuccess(ctx, proxy)
		releaseLease()
		return matchIDs, nil
	}

	return nil, fmt.Errorf("all attempts failed")
}