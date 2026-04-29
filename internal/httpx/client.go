package httpx

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	MaxBodySize = 50 << 20 // 50MB limit for API responses
)

type ProxiedClient struct {
	pool    *TransportPool
	timeout time.Duration
}

func NewProxiedClient(pool *TransportPool, timeout time.Duration) *ProxiedClient {
	return &ProxiedClient{pool: pool, timeout: timeout}
}

func (c *ProxiedClient) RemoveProxy(proxyURL string) {
	c.pool.Remove(proxyURL)
}

func (c *ProxiedClient) CloseIdleConnections(proxyURL string) {
	c.pool.CloseIdle(proxyURL)
}

type Response struct {
	Body       []byte
	StatusCode int
}

func (c *ProxiedClient) Get(ctx context.Context, targetURL, proxyURL string) (*Response, error) {
	transport, err := c.pool.GetOrCreate(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("transport: %w", err)
	}
	client := &http.Client{Transport: transport, Timeout: c.timeout}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		c.pool.CloseIdle(proxyURL)
		return nil, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()

	limitedReader := io.LimitReader(resp.Body, MaxBodySize+1)
	body, err := io.ReadAll(limitedReader)
	if err != nil {
		return &Response{StatusCode: resp.StatusCode}, fmt.Errorf("read body: %w", err)
	}
	if len(body) > MaxBodySize {
		return &Response{StatusCode: resp.StatusCode}, fmt.Errorf("response body exceeds %d bytes", MaxBodySize)
	}
	return &Response{Body: body, StatusCode: resp.StatusCode}, nil
}
