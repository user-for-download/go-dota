package httpx

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type Options struct {
	SkipTLSVerify       bool
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration
	DialTimeout         time.Duration
}

func DefaultOptions() Options {
	return Options{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DialTimeout:         10 * time.Second,
	}
}

type TransportPool struct {
	opts       Options
	mu         sync.RWMutex
	transports map[string]*http.Transport
	// NOTE: No eviction — transport map grows as proxies are added via RemoveProxy.
	// Acceptable if proxy set is stable; unbounded growth if proxies churn frequently.
}

func NewTransportPool(opts Options) *TransportPool {
	return &TransportPool{
		opts:       opts,
		transports: make(map[string]*http.Transport),
	}
}

func (p *TransportPool) GetOrCreate(proxyURL string) (*http.Transport, error) {
	p.mu.RLock()
	if t, ok := p.transports[proxyURL]; ok {
		p.mu.RUnlock()
		return t, nil
	}
	p.mu.RUnlock()

	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.transports[proxyURL]; ok {
		return t, nil
	}

	t := &http.Transport{
		Proxy:               http.ProxyURL(parsed),
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: p.opts.SkipTLSVerify},
		MaxIdleConns:        p.opts.MaxIdleConns,
		MaxIdleConnsPerHost: p.opts.MaxIdleConnsPerHost,
		IdleConnTimeout:     p.opts.IdleConnTimeout,
	}
	p.transports[proxyURL] = t
	return t, nil
}

func (p *TransportPool) Remove(proxyURL string) {
	p.mu.Lock()
	t, ok := p.transports[proxyURL]
	delete(p.transports, proxyURL)
	p.mu.Unlock()
	if ok && t != nil {
		t.CloseIdleConnections()
	}
}

func (p *TransportPool) CloseIdle(proxyURL string) {
	p.mu.RLock()
	t := p.transports[proxyURL]
	p.mu.RUnlock()
	if t != nil {
		t.CloseIdleConnections()
	}
}

func (p *TransportPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range p.transports {
		t.CloseIdleConnections()
	}
	p.transports = make(map[string]*http.Transport)
}