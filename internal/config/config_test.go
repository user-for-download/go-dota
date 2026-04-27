package config

import (
	"os"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	os.Unsetenv("REDIS_URL")
	os.Unsetenv("POSTGRES_URL")
	os.Unsetenv("COLLECTOR_WORKERS")
	os.Unsetenv("PARSER_WORKERS")
	os.Unsetenv("PROXY_REFRESH_MIN")
	os.Unsetenv("PROXY_LOCAL_FILE")
	os.Unsetenv("SKIP_TLS_VERIFY")
	os.Unsetenv("MONITOR_PORT")
	os.Unsetenv("DLQ_BATCH_SIZE")
	os.Unsetenv("DLQ_MAX_PER_TICK")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	tests := []struct {
		got  any
		want any
	}{
		{cfg.RedisURL, "redis://localhost:6379/0"},
		{cfg.PostgresURL, "postgres://postgres:postgres@localhost:5432/pipeline?sslmode=disable&pool_max_conns=20"},
		{cfg.CollectorWorkers, 10},
		{cfg.ParserWorkers, 5},
		{cfg.ProxyRefreshMin, 15},
		{cfg.ProxyLocalFile, "deployments/proxy.json"},
		{cfg.SkipTLSVerify, false},
		{cfg.MonitorPort, 8080},
		{cfg.DLQBatchSize, 100},
		{cfg.DLQMaxPerTick, 500},
	}

	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("got %v, want %v", tc.got, tc.want)
		}
	}
}

func TestLoadWithEnvOverrides(t *testing.T) {
	envs := map[string]string{
		"REDIS_URL":         "redis://custom:6379/1",
		"POSTGRES_URL":      "postgres://user:pass@custom:5432/db",
		"COLLECTOR_WORKERS": "20",
		"PARSER_WORKERS":    "8",
		"PROXY_REFRESH_MIN": "30",
		"PROXY_LOCAL_FILE":  "/custom/path/proxy.json",
		"SKIP_TLS_VERIFY":   "true",
		"MONITOR_PORT":      "9090",
		"DLQ_BATCH_SIZE":    "200",
		"DLQ_MAX_PER_TICK":  "1000",
	}
	for k, v := range envs {
		os.Setenv(k, v)
		defer os.Unsetenv(k)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	tests := []struct {
		got  any
		want any
	}{
		{cfg.RedisURL, "redis://custom:6379/1"},
		{cfg.PostgresURL, "postgres://user:pass@custom:5432/db"},
		{cfg.CollectorWorkers, 20},
		{cfg.ParserWorkers, 8},
		{cfg.ProxyRefreshMin, 30},
		{cfg.ProxyLocalFile, "/custom/path/proxy.json"},
		{cfg.SkipTLSVerify, true},
		{cfg.MonitorPort, 9090},
		{cfg.DLQBatchSize, 200},
		{cfg.DLQMaxPerTick, 1000},
	}

	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("got %v, want %v", tc.got, tc.want)
		}
	}
}
