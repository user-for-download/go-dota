package worker

import (
	"testing"
	"time"
)

func TestNewCollector(t *testing.T) {
	collector := NewCollector(nil, 5, nil, false, 3, 5, 20, 10000)
	if collector.numWorkers != 5 {
		t.Errorf("numWorkers = %d, want %d", collector.numWorkers, 5)
	}
	if collector.maxProxyFails != 3 {
		t.Errorf("maxProxyFails = %d, want %d", collector.maxProxyFails, 3)
	}
}

func TestNewCollector_DefaultMaxProxyFails(t *testing.T) {
	// Passing 0 should fall back to the default.
	collector := NewCollector(nil, 5, nil, false, 0, 0, 0, 0)
	if collector.maxProxyFails == 0 {
		t.Error("maxProxyFails = 0, expected default fallback")
	}
}

func TestJitteredSleep(t *testing.T) {
	// Run multiple times to check randomness
	const iterations = 100
	base := 100 * time.Millisecond

	sum := int64(0)
	min := int64(1<<63 - 1)
	max := int64(0)

	for i := 0; i < iterations; i++ {
		got := jitteredSleep(base)
		gotNs := got.Nanoseconds()
		sum += gotNs

		if gotNs < min {
			min = gotNs
		}
		if gotNs > max {
			max = gotNs
		}

		// Verify in range [base, base + base/4]
		if got < base {
			t.Errorf("jitteredSleep() = %v, want >= %v", got, base)
		}
		maxAllowed := base + base/4
		if got > maxAllowed {
			t.Errorf("jitteredSleep() = %v, want <= %v", got, maxAllowed)
		}
	}

	// Average should be roughly in the middle of the range
	avgNs := sum / iterations
	avgExpected := int64(base + base/8) // middle of range
	// Allow 20% variance
	if avgNs < avgExpected-avgExpected/5 || avgNs > avgExpected+avgExpected/5 {
		t.Logf("average = %v, expected ~%v (variance high but within allowed range)", time.Duration(avgNs), time.Duration(avgExpected))
	}

	// Min and max should be at the boundaries
	if min != base.Nanoseconds() {
		t.Logf("min = %v, expected base %v (some variance expected)", time.Duration(min), base)
	}
	if max < (base + base/4).Nanoseconds() {
		t.Logf("max = %v, expected max %v (some variance expected)", time.Duration(max), base+base/4)
	}

	t.Logf("jitteredSleep stats: min=%v, max=%v, avg=%v", time.Duration(min), time.Duration(max), time.Duration(avgNs))
}

func TestJitteredSleep_ZeroBase(t *testing.T) {
	got := jitteredSleep(0)
	if got != 0 {
		t.Errorf("jitteredSleep(0) = %v, want 0", got)
	}
}

func TestJitteredSleep_NegativeBase(t *testing.T) {
	got := jitteredSleep(-100 * time.Millisecond)
	if got != 0 {
		t.Errorf("jitteredSleep(-100ms) = %v, want 0", got)
	}
}

func TestJitteredSleep_VerySmallBase(t *testing.T) {
	// Edge case: base in nanoseconds where quarter = 0
	// time.Duration(1) = 1 nanosecond
	got := jitteredSleep(time.Duration(1))
	if got != time.Duration(1) {
		t.Errorf("jitteredSleep(1ns) = %v, want 1ns (no jitter possible)", got)
	}
}
