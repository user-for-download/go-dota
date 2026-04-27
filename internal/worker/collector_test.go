package worker

import (
	"testing"
)

func TestNewCollector(t *testing.T) {
	collector := NewCollector(nil, "https://example.com", 5, nil, false, 3, 5, 20)
	if collector.numWorkers != 5 {
		t.Errorf("numWorkers = %d, want %d", collector.numWorkers, 5)
	}
	if collector.targetAPIURL != "https://example.com" {
		t.Errorf("targetAPIURL = %s, want %s", collector.targetAPIURL, "https://example.com")
	}
	if collector.maxProxyFails != 3 {
		t.Errorf("maxProxyFails = %d, want %d", collector.maxProxyFails, 3)
	}
}

func TestNewCollector_DefaultMaxProxyFails(t *testing.T) {
	// Passing 0 should fall back to the default.
	collector := NewCollector(nil, "https://example.com", 5, nil, false, 0, 0, 0)
	if collector.maxProxyFails == 0 {
		t.Error("maxProxyFails = 0, expected default fallback")
	}
}
