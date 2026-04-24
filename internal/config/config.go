package config

import "github.com/ilyakaznacheev/cleanenv"

type Config struct {
	RedisURL          string `env:"REDIS_URL" env-default:"redis://localhost:6379/0"`
	PostgresURL       string `env:"POSTGRES_URL" env-default:"postgres://postgres:postgres@localhost:5432/pipeline?sslmode=disable&pool_max_conns=20"`
	LegacyPostgresURL string `env:"LEGACY_POSTGRES_URL" env-default:"postgres://postgres:postgres@localhost:5433/legacy?sslmode=disable&pool_max_conns=10"`
	TargetAPIURL      string `env:"TARGET_API_URL" env-default:"https://httpbin.org/json"`
	ProxyProviderURL  string `env:"PROXY_PROVIDER_URL" env-default:""`
	ProxyLocalFile    string `env:"PROXY_LOCAL_FILE" env-default:"deployments/proxy.json"`
	SQLDir            string `env:"SQL_DIR" env-default:"deployments/queries"`
	CollectorWorkers  int    `env:"COLLECTOR_WORKERS" env-default:"10"`
	ParserWorkers     int    `env:"PARSER_WORKERS" env-default:"5"`
	FetchIntervalSec  int    `env:"FETCH_INTERVAL_SEC" env-default:"5"`
	ProxyRefreshMin   int    `env:"PROXY_REFRESH_MIN" env-default:"15"`
	HealthCheckURL    string `env:"HEALTH_CHECK_URL" env-default:"https://httpbin.org/ip"`
	SkipTLSVerify     bool   `env:"SKIP_TLS_VERIFY" env-default:"false"`
	MonitorPort       int    `env:"MONITOR_PORT" env-default:"8080"`
	DLQBatchSize      int    `env:"DLQ_BATCH_SIZE" env-default:"100"`
	DLQMaxPerTick     int    `env:"DLQ_MAX_PER_TICK" env-default:"500"`
	MaxRetries        int    `env:"MAX_RETRIES" env-default:"3"`
	MaxProxyFails     int    `env:"MAX_PROXY_FAILS" env-default:"3"`
	MaxProxyReqPerMin int    `env:"MAX_PROXY_REQ_MIN" env-default:"60"`
	MaxProxyReqPerDay int    `env:"MAX_PROXY_REQ_DAY" env-default:"3000"`
	MaxQueueSize      int64  `env:"MAX_QUEUE_SIZE" env-default:"10000"`
}

func Load() (*Config, error) {
	var cfg Config
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
