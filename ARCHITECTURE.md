# Architecture - Data Pipeline

A distributed data collection pipeline using Go, Redis, and PostgreSQL.

## Overview

```
[SQL] → [Fetcher] → [Redis: fetch_queue]
                         ↓
                   [Collector] → [Redis: parse_queue]
                         ↓
                    [Parser] → [PostgreSQL]
```

## Components

### Fetcher (`cmd/fetcher/main.go`)
- One-shot CLI with `--key` flag (`leagues`, `players`, `teams`, `default`)
- Loads SQL from `deployments/queries/{key}.sql`
- Pushes FetchTask{ID, URL} to `fetch_queue`
- Respects `MAX_QUEUE_SIZE` (default 10000)
- Exits after all tasks or queue at capacity

### Collector (`cmd/collector/main.go`)
- Workers consume from `fetch_queue` via BLPOP (5s timeout)
- HTTP client with SOCKS5 proxy rotation
- Circuit breaker per proxy: closed → open → halfopen
- Exponential backoff: starts 1s, doubles per failure, max 60s
- Re-enqueues tasks on HTTP 429 (rate limit)

### Parser (`cmd/parser/main.go`)
- Consumes from `parse_queue` via BLPOP
- Loads raw JSON from `raw_data:{taskID}`
- Extracts ID from `id` field (fallback to `match_id` as json.Number)
- Upserts to PostgreSQL via repository
- On failure: INCR retry count, LPUSH to `failed_queue`
- After max retries: RPUSH to `permanent_failed_queue`
- DLQ recovery on startup (batch 100)
- Periodic drain every 5min (batch 500)

### ProxyManager (`cmd/proxy-manager/main.go`)
- Fetches proxies from `PROXY_PROVIDER_URL` or reads `PROXY_LOCAL_FILE`
- Health checks via `HEALTH_CHECK_URL` (semaphore 50 concurrent)
- Updates `proxy_pool` SET and `proxy_ranking` ZSET
- Periodic refresh (default 15min)

### Monitor (`cmd/monitor/main.go`)
- HTTP server on port `MONITOR_PORT` (default 8080)
- GET `/health` - liveness check (Redis + Postgres ping)
- GET `/metrics` - queue lengths, error counts

## Data Flow

```
1. Fetcher:
   Load SQL → create FetchTask{ID, URL} → RPUSH to fetch_queue

2. Collector:
   BLPOP → HTTP GET URL → RPUSH taskID to parse_queue
        → SET raw_data:{taskID} with TTL 7200s

3. Parser:
   BLPOP → GET raw_data → parse JSON → PG upsert
        → DEL raw_data

4. On error:
   INCR retry_count → LPUSH to failed_queue

5. DLQ drain:
   LPop from failed_queue → check retry_count
   → if < max: RPUSH to parse_queue
   → else: RPUSH to permanent_failed_queue
```

## Circuit Breaker (`internal/worker/collector.go`)

Three states:
- **Closed**: Normal operation, requests go through
- **Open**: Proxy failing, reject immediately (wait resetTimeout)
- **Halfopen**: Testing recovery, one probe allowed

Transitions:
- closed → open: `failureCount >= maxFailures` (default 5)
- open → halfopen: `time.Since(lastFailure) > resetTimeout` (default 30s)
- halfopen → closed: `successCount >= minSuccesses` (default 3)
- halfopen → open: probe fails

Backoff: exponential `resetTimeout × 2^failures`, capped at maxBackoff (60s)

## Proxy Scoring (`internal/storage/redis/proxy.go`)

- Initial: 1000
- Success: `+10` (capped at 10000)
- Failure: `-penalty × fails` (penalty = 100, capped at 0)
- After max fails (≥3): score = -1000, removed from pool
- Selection: weighted random (fraction = 0.95)

Lua script `weightedRandomProxyScript` handles scoring + selection.

## Rate Limiting (`internal/storage/redis/ratelimit.go`)

Per-proxy limits via Lua script atomically:
- `proxy_req_min:{IP}` - INCR, EXPIRE 60s
- `proxy_req_day:{IP}` - INCR, EXPIRE 86400s

Skips limiting if max = 0 (unlimited).

## Config (`internal/config/config.go`)

```go
type Config struct {
    RedisURL           string  // redis://localhost:6379/0
    PostgresURL        string  // postgres://...
    TargetAPIURL      string  // https://httpbin.org/json
    ProxyProviderURL  string  // ""
    ProxyLocalFile     string  // deployments/proxy.json
    SQLDir             string  // deployments/queries
    CollectorWorkers  int     // 10
    ParserWorkers      int      // 5
    FetchIntervalSec  int     // 5
    ProxyRefreshMin    int     // 15
    HealthCheckURL    string  // https://httpbin.org/ip
    SkipTLSVerify     bool     // false
    MonitorPort      int      // 8080
    DLQBatchSize     int     // 100
    DLQMaxPerTick    int     // 500
    MaxRetries       int     // 3
    MaxProxyFails    int     // 3
    MaxProxyReqPerMin int    // 60
    MaxProxyReqPerDay int     // 3000
    MaxQueueSize      int64   // 10000
}
```

## Key Files

| File | Purpose |
|------|---------|
| `cmd/fetcher/main.go` | Entry point |
| `cmd/collector/main.go` | Entry point |
| `cmd/parser/main.go` | Entry point, migration |
| `cmd/monitor/main.go` | HTTP server |
| `cmd/proxy-manager/main.go` | Entry point |
| `internal/worker/collector.go` | CircuitBreaker, worker loop |
| `internal/worker/fetcher.go` | Queue capacity checks |
| `internal/worker/parser.go` | DLQ drain |
| `internal/worker/proxy_manager.go` | Proxy refresh |
| `internal/storage/redis/client.go` | Client + config |
| `internal/storage/redis/proxy.go` | Pool + scoring |
| `internal/storage/redis/ratelimit.go` | Rate limit Lua |
| `internal/storage/redis/queue.go` | Queue ops |
| `internal/storage/postgres/client.go` | PG client |
| `internal/storage/postgres/repository.go` | PG operations |
| `internal/storage/postgres/migrate.go` | Migration runner |
| `internal/models/models.go` | FetchTask, APIResponse |
| `internal/config/config.go` | Env config |
| `internal/httpx/client.go` | HTTP client |
| `internal/httpx/transport_pool.go` | SOCKS5 transport |

## Database Schema (`internal/storage/postgres/repository.go`)

```sql
CREATE TABLE IF NOT EXISTS parsed_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(255) UNIQUE NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Embedded migrations in `internal/storage/postgres/migrations/*.sql`.

## Docker

```bash
docker compose -f deployments/docker-compose.yml --profile all up -d
```

Services: redis, postgres, fetcher, collector, parser, proxy-manager, monitor