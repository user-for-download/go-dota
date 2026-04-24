package worker

import (
	"context"
	"strings"
	"testing"
)

func TestNewFetcher(t *testing.T) {
	fetcher := NewFetcher(nil, nil, "teams", "", nil, 0, 0)
	if fetcher.key != "teams" {
		t.Errorf("key = %s, want teams", fetcher.key)
	}

	fetcher = NewFetcher(nil, nil, "players", "", nil, 0, 0)
	if fetcher.key != "players" {
		t.Errorf("key = %s, want players", fetcher.key)
	}

	fetcher = NewFetcher(nil, nil, "leagues", "", nil, 0, 0)
	if fetcher.key != "leagues" {
		t.Errorf("key = %s, want leagues", fetcher.key)
	}

	fetcher = NewFetcher(nil, nil, "default", "", nil, 0, 0)
	if fetcher.key != "default" {
		t.Errorf("key = %s, want default", fetcher.key)
	}
}

func TestFetcherInvalidKey(t *testing.T) {
	fetcher := NewFetcher(nil, nil, "invalid_key", "", nil, 0, 0)
	err := fetcher.Run(context.Background())
	if err == nil {
		t.Error("Run() should return error for invalid key")
	}
	if !strings.Contains(err.Error(), "load query") {
		t.Errorf("error = %v", err)
	}
}

func TestFetcherKeyURLs(t *testing.T) {
	keys := []string{"teams", "players", "leagues", "default"}
	for _, key := range keys {
		fetcher := NewFetcher(nil, nil, key, "", nil, 0, 0)
		if fetcher.key != key {
			t.Errorf("key = %s, want %s", fetcher.key, key)
		}
	}
}