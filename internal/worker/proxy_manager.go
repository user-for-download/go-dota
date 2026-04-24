package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/user-for-download/go-dota/internal/httpx"
	"github.com/user-for-download/go-dota/internal/storage/redis"
)

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
		checkSemaphore:  semaphore.NewWeighted(50),
		transportPool:   httpx.NewTransportPool(httpx.DefaultOptions()),
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

func (pm *ProxyManager) Run(ctx context.Context) error {
	pm.logger.Info("proxy manager started", "refresh_interval", pm.refreshInterval)

	initialRetry := 0
	for {
		select {
		case <-ctx.Done():
			pm.logger.Info("proxy manager shutting down")
			return nil
		default:
		}

		if err := pm.refreshProxies(ctx); err != nil {
			initialRetry++
			if initialRetry <= 3 {
				pm.logger.Warn("initial proxy refresh failed, retrying...", "attempt", initialRetry, "error", err)
				time.Sleep(time.Duration(initialRetry) * time.Second)
				continue
			}
			pm.logger.Error("proxy refresh failed", "error", err)
		}
		initialRetry = 0

		select {
		case <-ctx.Done():
			pm.logger.Info("proxy manager shutting down")
			return nil
		case <-time.After(pm.refreshInterval):
		}
	}
}

func (pm *ProxyManager) refreshProxies(ctx context.Context) error {
	pm.logger.Info("refreshing proxy pool")

	proxies, err := pm.fetchProxies(ctx)
	if err != nil {
		return fmt.Errorf("fetch proxies: %w", err)
	}
	pm.logger.Info("fetched proxies", "count", len(proxies))

	validProxies, err := pm.healthCheckProxies(ctx, proxies)
	if err != nil {
		pm.logger.Warn("health check failed, using all proxies", "error", err)
		validProxies = proxies
	}
	pm.logger.Info("proxies health checked", "total", len(proxies), "valid", len(validProxies))

	if len(validProxies) == 0 {
		pm.logger.Warn("no proxies passed health checks")
		return nil
	}

	if err := pm.redisClient.AddProxies(ctx, validProxies); err != nil {
		return fmt.Errorf("add proxies to redis: %w", err)
	}
	pm.logger.Info("proxy pool updated", "count", len(validProxies))
	return nil
}

func (pm *ProxyManager) healthCheckProxies(ctx context.Context, proxies []string) ([]string, error) {
	pm.transportPool.CloseAll()

	if len(proxies) > 200 {
		proxies = proxies[:200]
		pm.logger.Info("capped proxy list for health check", "count", len(proxies))
	}

	var validProxies []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, p := range proxies {
		wg.Add(1)
		go func(proxyURL string) {
			defer wg.Done()
			if err := pm.checkSemaphore.Acquire(ctx, 1); err != nil {
				return
			}
			defer pm.checkSemaphore.Release(1)
			if pm.checkProxy(ctx, proxyURL) {
				mu.Lock()
				validProxies = append(validProxies, proxyURL)
				mu.Unlock()
			}
		}(p)
	}
	wg.Wait()

	return validProxies, nil
}

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

func (pm *ProxyManager) fetchProxies(ctx context.Context) ([]string, error) {
	localProxies, localErr := pm.readLocalProxies(ctx)
	if localErr == nil && len(localProxies) > 0 {
		pm.logger.Info("loaded proxies from local file", "count", len(localProxies))
		return localProxies, nil
	}

	if pm.proxyProviderURL != "" {
		httpProxies, httpErr := pm.fetchFromProvider(ctx)
		if httpErr == nil && len(httpProxies) > 0 {
			pm.logger.Info("fetched proxies from provider", "count", len(httpProxies))
			return httpProxies, nil
		}
		if httpErr != nil {
			pm.logger.Warn("provider fetch failed, using local fallback", "error", httpErr)
			if len(localProxies) > 0 {
				return localProxies, nil
			}
			return nil, fmt.Errorf("provider fetch: %w", httpErr)
		}
	}

	if len(localProxies) > 0 {
		return localProxies, nil
	}
	return nil, fmt.Errorf("no proxy source available (PROXY_PROVIDER_URL and PROXY_LOCAL_FILE both unavailable)")
}

func (pm *ProxyManager) fetchFromProvider(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pm.proxyProviderURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	transport := &http.Transport{Proxy: http.ProxyFromEnvironment}
	client := &http.Client{Transport: transport, Timeout: 60 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch proxy list: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("proxy provider returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read proxy list body: %w", err)
	}

	var apiResp proxyscrapeAPI
	if err := json.Unmarshal(body, &apiResp); err != nil {
		textProxies := pm.parseTextProxies(body)
		if len(textProxies) > 0 {
			return textProxies, nil
		}
		return nil, fmt.Errorf("failed to parse provider response as json or text: %w", err)
	}

	var proxies []string
	for _, p := range apiResp.Proxies {
		if !p.Alive {
			continue
		}
		if p.Proxy == "" {
			continue
		}
		proxies = append(proxies, p.Proxy)
	}
	return proxies, nil
}

func (pm *ProxyManager) parseTextProxies(body []byte) []string {
	var proxies []string
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "{") || strings.HasPrefix(line, "[") {
			continue
		}

		if !strings.HasPrefix(line, "http://") && !strings.HasPrefix(line, "https://") && !strings.HasPrefix(line, "socks5://") && !strings.HasPrefix(line, "socks4://") {
			line = "http://" + line
		}
		proxies = append(proxies, line)
	}
	return proxies
}

func (pm *ProxyManager) readLocalProxies(ctx context.Context) ([]string, error) {
	if pm.localProxyFile == "" {
		return nil, nil
	}

	data, err := os.ReadFile(pm.localProxyFile)
	if err != nil {
		return nil, nil
	}

	proxies, err := pm.parseLocalProxies(data)
	if err != nil {
		return nil, fmt.Errorf("parse local proxies: %w", err)
	}
	return proxies, nil
}

func (pm *ProxyManager) parseLocalProxies(data []byte) ([]string, error) {
	type localProxyJSON struct {
		ShownRecords int `json:"shown_records"`
		Proxies     []struct {
			Alive  bool   `json:"alive"`
			Proxy  string `json:"proxy"`
			IP     string `json:"ip"`
			Port   int    `json:"port"`
		} `json:"proxies"`
	}

	var resp localProxyJSON
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal local file: %w", err)
	}

	var proxies []string
	for _, p := range resp.Proxies {
		if !p.Alive {
			continue
		}
		if p.Proxy != "" {
			proxies = append(proxies, p.Proxy)
		}
	}

	return proxies, nil
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
	resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}