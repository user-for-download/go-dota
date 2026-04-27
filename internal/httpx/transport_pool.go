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
	DialTimeout        time.Duration
	MaxPoolSize        int // 0 = unlimited
}

const defaultMaxPoolSize = 500

func DefaultOptions() Options {
	return Options{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DialTimeout:        10 * time.Second,
		MaxPoolSize:        defaultMaxPoolSize,
	}
}

type TransportPool struct {
	opts       Options
	mu         sync.RWMutex
	transports map[string]*http.Transport
	lastUsed  map[string]time.Time
}

func NewTransportPool(opts Options) *TransportPool {
	if opts.MaxPoolSize == 0 {
		opts.MaxPoolSize = defaultMaxPoolSize
	}
	return &TransportPool{
		opts:       opts,
		transports: make(map[string]*http.Transport),
		lastUsed:  make(map[string]time.Time),
	}
}

func (p *TransportPool) GetOrCreate(proxyURL string) (*http.Transport, error) {
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if t, ok := p.transports[proxyURL]; ok {
		p.lastUsed[proxyURL] = time.Now()
		return t, nil
	}

	if p.opts.MaxPoolSize > 0 && len(p.transports) >= p.opts.MaxPoolSize {
		p.evictOldestLocked()
	}

	t := &http.Transport{
		Proxy:              http.ProxyURL(parsed),
		TLSClientConfig:    &tls.Config{InsecureSkipVerify: p.opts.SkipTLSVerify},
		MaxIdleConns:       p.opts.MaxIdleConns,
		MaxIdleConnsPerHost: p.opts.MaxIdleConnsPerHost,
		IdleConnTimeout:    p.opts.IdleConnTimeout,
	}
	p.transports[proxyURL] = t
	p.lastUsed[proxyURL] = time.Now()
	return t, nil
}

func (p *TransportPool) evictOldestLocked() {
	var oldest string
	var oldestTime = time.Now()
	for url, t := range p.lastUsed {
		if t.Before(oldestTime) {
			oldestTime = t
			oldest = url
		}
	}
	if oldest != "" {
		if t, ok := p.transports[oldest]; ok {
			t.CloseIdleConnections()
		}
		delete(p.transports, oldest)
		delete(p.lastUsed, oldest)
	}
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