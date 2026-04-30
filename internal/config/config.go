package config

import "github.com/ilyakaznacheev/cleanenv"

type Config struct {
	RedisURL          string `env:"REDIS_URL" env-default:"redis://localhost:6379/0"`
	PostgresURL       string `env:"POSTGRES_URL" env-default:"postgres://postgres:postgres@localhost:5432/pipeline?sslmode=disable&pool_max_conns=20"`
	ProxyProviderURL  string `env:"PROXY_PROVIDER_URL" env-default:""`
	ProxyLocalFile    string `env:"PROXY_LOCAL_FILE" env-default:"deployments/proxy.json"`
	SQLDir            string `env:"SQL_DIR" env-default:"deployments/queries"`
	CollectorWorkers  int    `env:"COLLECTOR_WORKERS" env-default:"10"`
	ParserWorkers     int    `env:"PARSER_WORKERS" env-default:"5"`
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

	// Collector retry budgets
	CollectorMaxRetries          int `env:"COLLECTOR_MAX_RETRIES" env-default:"5"`
	CollectorMaxRateLimitRetries int `env:"COLLECTOR_MAX_RATE_LIMIT_RETRIES" env-default:"20"`

	// Transport pool
	MaxPoolSize int `env:"MAX_POOL_SIZE" env-default:"500"`

	// Raw data TTL in seconds (default 2 hours)
	RawDataTTLSeconds int `env:"RAW_DATA_TTL_SECONDS" env-default:"7200"`

	// Redis Streams configuration
	StreamBatchSize int `env:"STREAM_BATCH_SIZE" env-default:"10"`
	StreamBlockMS   int `env:"STREAM_BLOCK_MS" env-default:"5000"`

	// Payload storage backend (redis or postgres)
	PayloadStore string `env:"PAYLOAD_STORE" env-default:"redis"`

	// Enricher endpoints
	EnricherHeroesURL     string `env:"ENRICHER_HEROES_URL" env-default:"https://api.opendota.com/api/heroes"`
	EnricherLeaguesURL    string `env:"ENRICHER_LEAGUES_URL" env-default:"https://api.opendota.com/api/leagues"`
	EnricherTeamsURL      string `env:"ENRICHER_TEAMS_URL" env-default:"https://api.opendota.com/api/teams"`
	EnricherItemsURL      string `env:"ENRICHER_ITEMS_URL" env-default:"https://api.opendota.com/api/constants/items"`
	EnricherGameModesURL  string `env:"ENRICHER_GAME_MODES_URL" env-default:"https://api.opendota.com/api/constants/game_mode"`
	EnricherLobbyTypesURL string `env:"ENRICHER_LOBBY_TYPES_URL" env-default:"https://api.opendota.com/api/constants/lobby_type"`
	EnricherPatchesURL    string `env:"ENRICHER_PATCHES_URL" env-default:"https://api.opendota.com/api/constants/patch"`
	EnricherProPlayersURL string `env:"ENRICHER_PRO_PLAYERS_URL" env-default:"https://api.opendota.com/api/proPlayers"`
	EnricherAbilitiesURL  string `env:"ENRICHER_ABILITIES_URL" env-default:"https://raw.githubusercontent.com/odota/dotaconstants/refs/heads/master/build/abilities.json"`
	EnricherAbilityIDsURL string `env:"ENRICHER_ABILITY_IDS_URL" env-default:"https://raw.githubusercontent.com/odota/dotaconstants/refs/heads/master/build/ability_ids.json"`
	EnricherHeroStatsURL  string `env:"ENRICHER_HERO_STATS_URL" env-default:"https://api.opendota.com/api/heroStats"`

	MigrationsDir string `env:"MIGRATIONS_DIR" env-default:"/app/migrations"`
}

func Load() (*Config, error) {
	var cfg Config
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
