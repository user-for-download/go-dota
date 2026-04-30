package worker

import (
	"context"
	"strings"
	"testing"

	"github.com/user-for-download/go-dota/internal/config"
)

func TestNewStreamFetcher(t *testing.T) {
	cfg := &config.Config{MaxQueueSize: 10000, MaxProxyFails: 3}
	fetcher := NewStreamFetcher(nil, nil, "/path/to/teams.sql", nil, cfg)
	if fetcher.sqlPath != "/path/to/teams.sql" {
		t.Errorf("sqlPath = %s, want %s", fetcher.sqlPath, "/path/to/teams.sql")
	}
}

func TestStreamFetcherInvalidPath(t *testing.T) {
	cfg := &config.Config{MaxQueueSize: 10000, MaxProxyFails: 3}
	fetcher := NewStreamFetcher(nil, nil, "/invalid/path.sql", nil, cfg)
	err := fetcher.Run(context.Background())
	if err == nil {
		t.Error("Run() should return error for invalid path")
	}
	if !strings.Contains(err.Error(), "load query") {
		t.Errorf("error = %v", err)
	}
}

func TestStreamFetcherPaths(t *testing.T) {
	cfg := &config.Config{MaxQueueSize: 10000, MaxProxyFails: 3}
	paths := []string{"/path/to/teams.sql", "/path/to/players.sql", "/path/to/leagues.sql"}
	for _, p := range paths {
		fetcher := NewStreamFetcher(nil, nil, p, nil, cfg)
		if fetcher.sqlPath != p {
			t.Errorf("sqlPath = %s, want %s", fetcher.sqlPath, p)
		}
	}
}