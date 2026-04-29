package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/user-for-download/go-dota/internal/httpx"
	"github.com/user-for-download/go-dota/internal/models"
	"github.com/user-for-download/go-dota/internal/storage/postgres"
	"github.com/user-for-download/go-dota/internal/storage/redis"
)

type Fetcher struct {
	redisClient   *redis.Client
	repo          *postgres.Repository
	sqlPath       string
	logger        *slog.Logger
	httpClient    *httpx.ProxiedClient
	maxQueueSize  int64
	maxProxyFails int
}

func NewFetcher(
	redisClient *redis.Client,
	repo *postgres.Repository,
	sqlPath string,
	logger *slog.Logger,
	maxQueueSize int64,
	maxProxyFails int,
) *Fetcher {
	pool := httpx.NewTransportPool(httpx.DefaultOptions())
	if maxProxyFails <= 0 {
		maxProxyFails = redis.DefaultMaxProxyFails
	}
	if maxQueueSize <= 0 {
		maxQueueSize = 10000
	}
	return &Fetcher{
		redisClient:   redisClient,
		repo:          repo,
		sqlPath:       sqlPath,
		logger:        logger,
		httpClient:    httpx.NewProxiedClient(pool, 60*time.Second),
		maxQueueSize:  maxQueueSize,
		maxProxyFails: maxProxyFails,
	}
}

func (f *Fetcher) Run(ctx context.Context) error {
	sql, err := f.loadQuery()
	if err != nil {
		return fmt.Errorf("load query: %w", err)
	}

	f.logger.Info("fetcher started", "path", f.sqlPath)

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

	pushed := 0
	pushedIDs := make([]int64, 0, len(unknownIDs))
	for _, id := range unknownIDs {
		idStr := strconv.FormatInt(id, 10)

		seen, err := f.redisClient.IsFetchIDSeen(ctx, idStr)
		if err != nil {
			f.logger.Warn("IsFetchIDSeen failed, pushing anyway",
				"id", idStr, "error", err)
		} else if seen {
			continue
		}

		task := models.FetchTask{
			MatchID: idStr,
			URL:     fmt.Sprintf("https://api.opendota.com/api/matches/%d", id),
		}

		ok, err := f.redisClient.PushFetchTaskWithCap(ctx, task, f.maxQueueSize)
		if err != nil {
			f.logger.Error("push task failed", "match_id", id, "error", err)
			continue
		}
		if !ok {
			f.logger.Warn("fetch queue full, stopping push", "queue_size", f.maxQueueSize)
			break
		}
		pushedIDs = append(pushedIDs, id)
		pushed++
	}

	// Batch mark all pushed IDs as seen (more efficient than per-ID)
	if len(pushedIDs) > 0 {
		idStrs := make([]string, len(pushedIDs))
		for i, id := range pushedIDs {
			idStrs[i] = strconv.FormatInt(id, 10)
		}
		if err := f.redisClient.MarkFetchIDSeenBatch(ctx, idStrs); err != nil {
			f.logger.Warn("MarkFetchIDSeenBatch failed, IDs may be re-pushed",
				"error", err)
		}
	}

	f.logger.Info("tasks pushed to queue", "count", pushed)

	f.recordStats(ctx, len(matchIDs), len(unknownIDs), pushed)
	return nil
}

func (f *Fetcher) recordStats(ctx context.Context, discovered, newInDB, pushed int) {
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

func (f *Fetcher) loadQuery() (string, error) {
	data, err := os.ReadFile(f.sqlPath)
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

func (f *Fetcher) waitForProxies(ctx context.Context, timeout time.Duration) error {
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

func (f *Fetcher) fetchMatchIDs(ctx context.Context, targetURL string) ([]int64, error) {
	var matchIDs []int64
	var lastErr error

	f.logger.Info("starting fetchMatchIDs loop", "target", targetURL)
	for attempt := 0; attempt < 5; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		proxy, err := f.redisClient.GetWeightedRandomProxy(ctx)
		if err != nil {
			f.logger.Warn("no proxy available", "attempt", attempt, "error", err)
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			lastErr = fmt.Errorf("no proxy available: %w", err)
			continue
		}

		if !isUsableProxyURL(proxy) {
			f.logger.Warn("proxy pool returned malformed URL, removing",
				"attempt", attempt,
				"proxy", proxy)
			_ = f.redisClient.RemoveProxy(ctx, proxy)
			continue
		}

		f.logger.Info("fetching with proxy", "attempt", attempt, "proxy", proxy)
		resp, err := f.httpClient.Get(ctx, targetURL, proxy)
		if err != nil {
			f.httpClient.CloseIdleConnections(proxy)
			f.httpClient.RemoveProxy(proxy)
			_ = f.redisClient.RecordProxyFailure(ctx, proxy, f.maxProxyFails)
			lastErr = err
			f.logger.Warn("fetch failed, trying next proxy", "proxy", proxy, "error", err)
			continue
		}

		if resp.StatusCode != 200 {
			f.httpClient.CloseIdleConnections(proxy)
			f.httpClient.RemoveProxy(proxy)
			_ = f.redisClient.RecordProxyFailure(ctx, proxy, f.maxProxyFails)
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			continue
		}

		var explorer explorerResponse
		if err := json.Unmarshal(resp.Body, &explorer); err != nil {
			f.httpClient.RemoveProxy(proxy)
			_ = f.redisClient.RecordProxyFailure(ctx, proxy, f.maxProxyFails)
			lastErr = fmt.Errorf("invalid json from proxy: %w", err)
			f.logger.Warn("fetch failed with bad json, trying next proxy", "proxy", proxy)
			continue
		}

		if len(explorer.Rows) == 0 {
			f.logger.Info("explorer returned empty rows (valid state, no new matches)", "proxy", proxy)
			_ = f.redisClient.RecordProxySuccess(ctx, proxy)
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
		return matchIDs, nil
	}

	if lastErr == nil {
		lastErr = errors.New("no attempts made")
	}
	return nil, fmt.Errorf("all attempts failed: %w", lastErr)
}
