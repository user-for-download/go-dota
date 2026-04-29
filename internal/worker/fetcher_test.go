package worker

import (
	"context"
	"strings"
	"testing"
)

func TestNewFetcher(t *testing.T) {
	fetcher := NewFetcher(nil, nil, "/path/to/teams.sql", nil, 0, 0)
	if fetcher.sqlPath != "/path/to/teams.sql" {
		t.Errorf("sqlPath = %s, want /path/to/teams.sql", fetcher.sqlPath)
	}

	fetcher = NewFetcher(nil, nil, "/path/to/players.sql", nil, 0, 0)
	if fetcher.sqlPath != "/path/to/players.sql" {
		t.Errorf("sqlPath = %s, want /path/to/players.sql", fetcher.sqlPath)
	}
}

func TestFetcherInvalidPath(t *testing.T) {
	fetcher := NewFetcher(nil, nil, "/invalid/path.sql", nil, 0, 0)
	err := fetcher.Run(context.Background())
	if err == nil {
		t.Error("Run() should return error for invalid path")
	}
	if !strings.Contains(err.Error(), "load query") {
		t.Errorf("error = %v", err)
	}
}

func TestFetcherPaths(t *testing.T) {
	paths := []string{"/path/to/teams.sql", "/path/to/players.sql", "/path/to/leagues.sql"}
	for _, p := range paths {
		fetcher := NewFetcher(nil, nil, p, nil, 0, 0)
		if fetcher.sqlPath != p {
			t.Errorf("sqlPath = %s, want %s", fetcher.sqlPath, p)
		}
	}
}