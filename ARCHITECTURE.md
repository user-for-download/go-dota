# Architecture - Data Pipeline

A distributed data collection pipeline using Go, Redis, and PostgreSQL.

## Overview

```
[SQL] → [Fetcher] → [Redis: fetch_queue]
                         ↓
                   [Collector] → [Redis: parse_queue]
                         ↓
                    [Parser] → [PostgreSQL: parsed_data]
```

## Components

### Fetcher (`cmd/fetcher/main.go`)
- One-shot CLI with `--key` flag (`leagues`, `players`, `teams`, `default`)
- Loads SQL from `deployments/queries/{key}.sql`
- Filters IDs against legacy PostgreSQL (`parsed_data` table)
- Pushes FetchTask{MatchID, URL} to `fetch_queue`
- Respects `MAX_QUEUE_SIZE` (default 10000)
- Exits after all tasks or queue at capacity

### Collector (`cmd/collector/main.go`)
- Workers consume from `fetch_queue` via BLPOP (5s timeout)
- HTTP client with SOCKS5 proxy rotation via weighted random selection
- Records proxy success/failure in Redis (adjusts scoring)
- Re-enqueues tasks on HTTP 429 (rate limit)

### Parser (`cmd/parser/main.go`)
- Consumes from `parse_queue` via BLPOP
- Loads raw JSON from `raw_data:{taskID}`
- Extracts ID from `id` field (fallback to `match_id`)
- Upserts to PostgreSQL via LegacyRepository
- On failure: INCR retry count, LPUSH to `failed_queue`
- After max retries: RPUSH to `permanent_failed_queue`
- DLQ recovery on startup (batch 100)
- Periodic drain every 5min (batch 500)

### ProxyManager (`cmd/proxy-manager/main.go`)
- Fetches proxies from `PROXY_PROVIDER_URL` or reads `PROXY_LOCAL_FILE`
- Health checks via `HEALTH_CHECK_URL` (semaphore 50 concurrent)
- Merges local + provider proxies
- Updates `proxy_pool` SET and `proxy_ranking` ZSET
- Periodic refresh (default 15min)

### Monitor (`cmd/monitor/main.go`)
- HTTP server on port `MONITOR_PORT` (default 8080)
- GET `/health` - liveness check (Redis + Postgres ping)
- GET `/metrics` - queue lengths, error counts

## Data Flow

```
1. Fetcher:
   Load SQL → fetch IDs from OpenDota API → filter against DB → RPUSH to fetch_queue

2. Collector:
   BLPOP → HTTP GET URL → SET raw_data:{taskID} → RPUSH taskID to parse_queue

3. Parser:
   BLPOP → GET raw_data → parse JSON → PG upsert → DEL raw_data

4. On error:
   INCR retry_count → Extend raw_data TTL → LPUSH to failed_queue

5. DLQ drain:
   LPop from failed_queue → check retry_count
   → if < max: RPUSH to parse_queue
   → else: RPUSH to permanent_failed_queue
```

## Proxy Scoring (`internal/storage/redis/proxy.go`)

- **Initial**: 1000 (from ZADD on AddProxies)
- **Success**: `+1` (capped at 10000)
- **Failure**: `-penalty × fails` (penalty = 10, capped at 0)
- After max fails (≥3): score = -1000, removed from pool
- **Selection**: weighted random via Lua script

Lua script `weightedRandomProxyScript` handles scoring + selection atomically.

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
    LegacyPostgresURL  string  // postgres://...:5433/legacy
    TargetAPIURL      string  // https://httpbin.org/json
    ProxyProviderURL  string  // ""
    ProxyLocalFile    string  // deployments/proxy.json
    SQLDir            string  // deployments/queries
    CollectorWorkers  int     // 10
    ParserWorkers     int      // 5
    ProxyRefreshMin   int     // 15
    HealthCheckURL   string   // https://httpbin.org/ip
    SkipTLSVerify    bool    // false
    MonitorPort      int     // 8080
    DLQBatchSize     int     // 100
    DLQMaxPerTick   int     // 500
    MaxRetries      int     // 3
    MaxProxyFails   int     // 3
    MaxProxyReqPerMin int   // 60
    MaxProxyReqPerDay int   // 3000
    MaxQueueSize    int64    // 10000
}
```

## Key Files

| File | Purpose |
|------|---------|
| `cmd/fetcher/main.go` | Entry point |
| `cmd/collector/main.go` | Entry point |
| `cmd/parser/main.go` | Entry point |
| `cmd/monitor/main.go` | HTTP server |
| `cmd/proxy-manager/main.go` | Entry point |
| `internal/worker/collector.go` | Worker loop, proxy rotation |
| `internal/worker/fetcher.go` | Queue capacity, ID filtering |
| `internal/worker/parser.go` | DLQ drain |
| `internal/worker/proxy_manager.go` | Proxy refresh |
| `internal/storage/redis/client.go` | Client + config |
| `internal/storage/redis/proxy.go` | Pool + scoring |
| `internal/storage/redis/ratelimit.go` | Rate limit Lua |
| `internal/storage/redis/queue.go` | Queue ops |
| `internal/storage/postgres/client.go` | PG client |
| `internal/storage/postgres/repository.go` | PG repository |
| `internal/storage/postgres/legacy_repository.go` | Legacy parsed_data |
| `internal/models/models.go` | FetchTask, APIResponse |
| `internal/config/config.go` | Env config |
| `internal/httpx/client.go` | HTTP client |
| `internal/httpx/transport_pool.go` | SOCKS5 transport |
| `deployments/queries/*.sql` | Fetch SQL queries |

## Database Schema

```sql
CREATE TABLE IF NOT EXISTS parsed_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(255) UNIQUE NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

## Redis Keys

| Key | Type | Purpose | TTL |
|-----|------|---------|-----|
| `proxy_pool` | SET | Active proxies |
| `proxy_ranking` | ZSET | Proxy scores (0-10000) |
| `fetch_queue` | LIST | Tasks to collect (RPUSH) |
| `parse_queue` | LIST | Tasks to parse (RPUSH) |
| `failed_queue` | LIST | Retryable DLQ (LPUSH - LIFO) |
| `permanent_failed_queue` | LIST | Max retries exceeded |
| `raw_data:{TaskID}` | STRING | JSON payload | 7200s |
| `seen_fetch_ids` | SET | Fetch deduplication | 86400s |
| `seen_parse_ids` | SET | Parse deduplication | 86400s |
| `proxy_req_min:{IP}` | STRING | Per-minute rate limit | 60s |
| `proxy_req_day:{IP}` | STRING | Per-day rate limit | 86400s |
| `proxy_failures:{Proxy}` | STRING | Consecutive failure count |
| `retry_count:{TaskID}` | STRING | Retry count | 86400s |

## Docker

```bash
docker compose -f deployments/docker-compose.yml --profile all up -d
```

Services: redis, postgres, fetcher, collector, parser, proxy-manager, monitor