package worker

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/user-for-download/go-dota/internal/httpx"
	"github.com/user-for-download/go-dota/internal/storage/redis"
)

const (
	maxProviderResponseSize = 10 << 20 // 10MB limit for provider responses
)

// ---------------------------------------------------------------------
// ProxyManager
// ---------------------------------------------------------------------

type ProxyManager struct {
	redisClient      *redis.Client
	proxyProviderURL string
	healthCheckURL   string
	refreshInterval  time.Duration
	logger           *slog.Logger
	localProxyFile   string
	checkSemaphore   *semaphore.Weighted
	transportPool    *httpx.TransportPool
}

func NewProxyManager(
	redisClient *redis.Client,
	proxyProviderURL string,
	healthCheckURL string,
	refreshInterval time.Duration,
	logger *slog.Logger,
) *ProxyManager {
	return &ProxyManager{
		redisClient:      redisClient,
		proxyProviderURL: proxyProviderURL,
		healthCheckURL:   healthCheckURL,
		refreshInterval:  refreshInterval,
		logger:           logger,
		checkSemaphore:   semaphore.NewWeighted(50),
		transportPool:    httpx.NewTransportPool(httpx.DefaultOptions()),
	}
}

func NewProxyManagerWithConfig(
	redisClient *redis.Client,
	proxyProviderURL string,
	healthCheckURL string,
	refreshInterval time.Duration,
	logger *slog.Logger,
	localProxyFile string,
) *ProxyManager {
	pm := NewProxyManager(redisClient, proxyProviderURL, healthCheckURL, refreshInterval, logger)
	pm.localProxyFile = localProxyFile
	return pm
}

// ---------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------

// Run drives the refresh loop. The first iteration bootstraps from the local
// file (if present) so downstream workers can start as soon as possible.
func (pm *ProxyManager) Run(ctx context.Context) error {
	pm.logger.Info("proxy manager started",
		"refresh_interval", pm.refreshInterval,
		"local_file", pm.localProxyFile,
		"provider_url", pm.proxyProviderURL,
	)

	// Bootstrap phase — don't fail hard, just log.
	if err := pm.bootstrap(ctx); err != nil {
		pm.logger.Warn("bootstrap did not publish a pool", "error", err)
	}

	ticker := time.NewTicker(pm.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			pm.logger.Info("proxy manager shutting down")
			return nil
		case <-ticker.C:
			if err := pm.refreshCycle(ctx); err != nil {
				pm.logger.Warn("refresh cycle failed", "error", err)
			}
		}
	}
}

// ---------------------------------------------------------------------
// Bootstrap (first run)
// ---------------------------------------------------------------------
//
// Strategy:
//   1. Read local file.
//      a. If >=1 alive proxy → seed Redis immediately, then fetch provider
//         list through one of those local proxies.
//      b. Otherwise → fetch provider list directly so the pool can be
//         populated at all.
//   2. Merge local + provider, health-check, publish to Redis.

func (pm *ProxyManager) bootstrap(ctx context.Context) error {
	pm.logger.Info("bootstrap: loading local proxy file")
	locals, localErr := pm.readLocalProxies(ctx)

	var providerProxies []string
	var providerErr error

	if len(locals) > 0 {
		pm.logger.Info("bootstrap: local proxies loaded", "count", len(locals))

		// Seed Redis immediately so fetchers can start draining the queue
		// while we talk to the provider.
		if err := pm.redisClient.AddProxies(ctx, locals); err != nil {
			pm.logger.Warn("bootstrap: seed redis with local proxies failed", "error", err)
		} else {
			pm.logger.Info("bootstrap: seeded redis with local proxies", "count", len(locals))
		}

		// Update provider list through a local proxy first.
		if pm.proxyProviderURL != "" {
			providerProxies, providerErr = pm.fetchFromProviderViaProxies(ctx, locals)
			if providerErr != nil {
				pm.logger.Warn("bootstrap: provider fetch via local proxies failed, falling back to direct",
					"error", providerErr)
				providerProxies, providerErr = pm.fetchFromProviderDirect(ctx)
			}
		}
	} else {
		if localErr != nil {
			pm.logger.Warn("bootstrap: local file unusable", "error", localErr)
		} else {
			pm.logger.Info("bootstrap: local file missing or empty")
		}

		// No locals — go direct.
		if pm.proxyProviderURL != "" {
			providerProxies, providerErr = pm.fetchFromProviderDirect(ctx)
		}
	}

	if providerErr != nil {
		pm.logger.Warn("bootstrap: provider unreachable", "error", providerErr)
	} else if len(providerProxies) > 0 {
		pm.logger.Info("bootstrap: fetched provider proxies", "count", len(providerProxies))
	}

	merged := dedupMerge(locals, providerProxies)
	if len(merged) == 0 {
		return fmt.Errorf("no proxies available from any source")
	}

	valid := pm.healthCheckProxies(ctx, merged)
	pm.logger.Info("bootstrap: health check complete",
		"valid", len(valid), "total", len(merged))

	if len(valid) == 0 {
		// Don't overwrite the seeded local set if nothing passed checks.
		return fmt.Errorf("no proxies passed health checks")
	}

	if err := pm.redisClient.AddProxies(ctx, valid); err != nil {
		return fmt.Errorf("publish proxies: %w", err)
	}
	pm.logger.Info("bootstrap: pool published", "count", len(valid))
	return nil
}

// ---------------------------------------------------------------------
// Periodic refresh
// ---------------------------------------------------------------------
//
// Strategy:
//   - Re-read local file each cycle (allows hot updates).
//   - Fetch provider through the current Redis pool if possible, else direct.
//   - Merge + health-check + publish.

func (pm *ProxyManager) refreshCycle(ctx context.Context) error {
	pm.logger.Info("refresh cycle starting")

	locals, err := pm.readLocalProxies(ctx)
	if err != nil {
		pm.logger.Warn("refresh: local file unusable", "error", err)
	}

	pool, err := pm.currentPool(ctx)
	if err != nil {
		pm.logger.Warn("refresh: read current redis pool failed", "error", err)
	}

	var providerProxies []string
	if pm.proxyProviderURL != "" {
		if len(pool) > 0 {
			providerProxies, err = pm.fetchFromProviderViaProxies(ctx, pool)
			if err != nil {
				pm.logger.Warn("refresh: provider via pool failed, trying direct", "error", err)
				providerProxies, err = pm.fetchFromProviderDirect(ctx)
			}
		} else {
			providerProxies, err = pm.fetchFromProviderDirect(ctx)
		}
		if err != nil {
			pm.logger.Warn("refresh: provider unreachable", "error", err)
		}
	}

	merged := dedupMerge(locals, providerProxies)
	if len(merged) == 0 {
		return fmt.Errorf("no proxies available this cycle")
	}

	valid := pm.healthCheckProxies(ctx, merged)
	pm.logger.Info("refresh: health check",
		"valid", len(valid), "total", len(merged))
	if len(valid) == 0 {
		return fmt.Errorf("no proxies passed health checks")
	}

	if err := pm.redisClient.AddProxies(ctx, valid); err != nil {
		return fmt.Errorf("publish proxies: %w", err)
	}
	pm.logger.Info("refresh: pool updated", "count", len(valid))
	return nil
}

// ---------------------------------------------------------------------
// Provider fetch
// ---------------------------------------------------------------------

type proxyscrapeAPI struct {
	ShownRecords int `json:"shown_records"`
	TotalRecords int `json:"total_records"`
	Proxies      []struct {
		Alive          bool    `json:"alive"`
		IP             string  `json:"ip"`
		Port           int     `json:"port"`
		Protocol       string  `json:"protocol"`
		Proxy          string  `json:"proxy"`
		Timeout        float64 `json:"timeout"`
		SSL            bool    `json:"ssl"`
		Uptime         float64 `json:"uptime"`
		Anonymity      string  `json:"anonymity"`
		AverageTimeout float64 `json:"average_timeout"`
		FirstSeen      float64 `json:"first_seen"`
		LastSeen       float64 `json:"last_seen"`
		TimesAlive     int     `json:"times_alive"`
		TimesDead      int     `json:"times_dead"`
	} `json:"proxies"`
}

// fetchFromProviderDirect contacts the provider without any proxy
// (still honors HTTP_PROXY env if set).
func (pm *ProxyManager) fetchFromProviderDirect(ctx context.Context) ([]string, error) {
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment}
	return pm.fetchFromProviderWithTransport(ctx, transport)
}

// fetchFromProviderViaProxies tries each candidate proxy (shuffled, capped at 5)
// until one succeeds.
func (pm *ProxyManager) fetchFromProviderViaProxies(ctx context.Context, candidates []string) ([]string, error) {
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no candidate proxies")
	}

	shuffled := append([]string(nil), candidates...)
	rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	const maxAttempts = 5
	if len(shuffled) > maxAttempts {
		shuffled = shuffled[:maxAttempts]
	}

	var lastErr error
	for _, proxyURL := range shuffled {
		transport, err := pm.transportPool.GetOrCreate(proxyURL)
		if err != nil {
			lastErr = err
			continue
		}
		proxies, err := pm.fetchFromProviderWithTransport(ctx, transport)
		if err == nil && len(proxies) > 0 {
			pm.logger.Info("provider fetched via proxy",
				"proxy", proxyURL, "count", len(proxies))
			return proxies, nil
		}
		lastErr = err
		pm.logger.Debug("provider fetch via proxy failed",
			"proxy", proxyURL, "error", err)
	}
	return nil, fmt.Errorf("all proxy attempts failed: %w", lastErr)
}

func (pm *ProxyManager) fetchFromProviderWithTransport(ctx context.Context, transport http.RoundTripper) ([]string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, pm.proxyProviderURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("provider status %d", resp.StatusCode)
	}

	limitedReader := io.LimitReader(resp.Body, maxProviderResponseSize)
	body, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	// Try JSON first.
	var apiResp proxyscrapeAPI
	if jsonErr := json.Unmarshal(body, &apiResp); jsonErr == nil && len(apiResp.Proxies) > 0 {
		out := make([]string, 0, len(apiResp.Proxies))
		for _, p := range apiResp.Proxies {
			if p.Alive && p.Proxy != "" {
				if isValidProxyScheme(p.Proxy) {
					out = append(out, p.Proxy)
				}
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}

	// Fall back to plain text.
	if list := pm.parseTextProxies(body); len(list) > 0 {
		return list, nil
	}
	return nil, fmt.Errorf("provider response contained no usable proxies")
}

func isValidProxyScheme(proxyURL string) bool {
	u, err := url.Parse(proxyURL)
	if err != nil {
		return false
	}
	scheme := strings.ToLower(u.Scheme)
	return scheme == "http" || scheme == "https" || scheme == "socks5" || scheme == "socks4"
}

func (pm *ProxyManager) parseTextProxies(body []byte) []string {
	var proxies []string
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "{") || strings.HasPrefix(line, "[") {
			continue
		}
		if !strings.HasPrefix(line, "http://") &&
			!strings.HasPrefix(line, "https://") &&
			!strings.HasPrefix(line, "socks5://") &&
			!strings.HasPrefix(line, "socks4://") {
			line = "http://" + line
		}
		if isValidProxyScheme(line) {
			proxies = append(proxies, line)
		}
	}
	return proxies
}

// ---------------------------------------------------------------------
// Local file
// ---------------------------------------------------------------------

func (pm *ProxyManager) readLocalProxies(ctx context.Context) ([]string, error) {
	if pm.localProxyFile == "" {
		return nil, nil
	}

	data, err := os.ReadFile(pm.localProxyFile)
	if err != nil {
		if os.IsNotExist(err) {
			pm.logger.Info("local proxy file not found", "path", pm.localProxyFile)
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", pm.localProxyFile, err)
	}

	proxies, err := pm.parseLocalProxies(data)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", pm.localProxyFile, err)
	}
	return proxies, nil
}

func (pm *ProxyManager) parseLocalProxies(data []byte) ([]string, error) {
	type proxyEntry struct {
		Alive bool   `json:"alive"`
		Proxy string `json:"proxy"`
	}
	type localProxyJSON struct {
		Proxies []proxyEntry `json:"proxies"`
	}

	var resp localProxyJSON
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal local file: %w", err)
	}

	proxies := make([]string, 0, len(resp.Proxies))
	for _, p := range resp.Proxies {
		if !p.Alive || p.Proxy == "" {
			continue
		}
		proxyURL := p.Proxy
		if !strings.HasPrefix(proxyURL, "http") && !strings.HasPrefix(proxyURL, "socks") {
			proxyURL = "http://" + proxyURL
		}
		if isValidProxyScheme(proxyURL) {
			proxies = append(proxies, proxyURL)
		}
	}
	return proxies, nil
}

// ---------------------------------------------------------------------
// Health checking
// ---------------------------------------------------------------------

func (pm *ProxyManager) healthCheckProxies(ctx context.Context, proxies []string) []string {
	const maxToCheck = 500
	if len(proxies) > maxToCheck {
		proxies = proxies[:maxToCheck]
		pm.logger.Info("capped proxy list for health check", "count", len(proxies))
	}

	var (
		valid []string
		mu    sync.Mutex
		wg    sync.WaitGroup
	)
	for _, p := range proxies {
		wg.Add(1)
		go func(proxyURL string) {
			defer wg.Done()
			if err := pm.checkSemaphore.Acquire(ctx, 1); err != nil {
				return
			}
			defer pm.checkSemaphore.Release(1)

			if pm.checkProxyIsolated(ctx, proxyURL) {
				mu.Lock()
				valid = append(valid, proxyURL)
				mu.Unlock()
			}
		}(p)
	}
	wg.Wait()
	return valid
}

func (pm *ProxyManager) checkProxy(ctx context.Context, proxyURL string) bool {
	transport, err := pm.transportPool.GetOrCreate(proxyURL)
	if err != nil {
		pm.logger.Warn("invalid proxy URL", "proxy", proxyURL, "error", err)
		return false
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}

	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(checkCtx, http.MethodGet, pm.healthCheckURL, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		pm.logger.Debug("proxy health check failed", "proxy", proxyURL, "error", err)
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (pm *ProxyManager) checkProxyIsolated(ctx context.Context, proxyURL string) bool {
	proxyParsed, err := url.Parse(proxyURL)
	if err != nil {
		pm.logger.Warn("invalid proxy URL", "proxy", proxyURL, "error", err)
		return false
	}
	transport := &http.Transport{
		Proxy:             http.ProxyURL(proxyParsed),
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}

	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(checkCtx, http.MethodGet, pm.healthCheckURL, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		pm.logger.Debug("proxy health check failed", "proxy", proxyURL, "error", err)
		return false
	}
	resp.Body.Close()
	transport.CloseIdleConnections()
	return resp.StatusCode == http.StatusOK
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

// currentPool returns the current Redis pool (proxy_pool set) as a slice.
func (pm *ProxyManager) currentPool(ctx context.Context) ([]string, error) {
	return pm.redisClient.Instance().SMembers(ctx, "proxy_pool").Result()
}

// dedupMerge returns the union of two slices preserving order (first source first).
func dedupMerge(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	appendUnique := func(s string) {
		if s == "" {
			return
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	for _, s := range a {
		appendUnique(s)
	}
	for _, s := range b {
		appendUnique(s)
	}
	return out
}
