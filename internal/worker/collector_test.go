package worker

import (
	"testing"
)

func TestNewCollector(t *testing.T) {
	collector := NewCollector(nil, "https://example.com", 5, nil, false, 3)
	if collector.numWorkers != 5 {
		t.Errorf("numWorkers = %d, want %d", collector.numWorkers, 5)
	}
	if collector.targetAPIURL != "https://example.com" {
		t.Errorf("targetAPIURL = %s, want %s", collector.targetAPIURL, "https://example.com")
	}
}