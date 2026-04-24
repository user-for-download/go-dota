# AGENTS.md - Data Pipeline

Quick reference for AI agents working in this Go data pipeline repo.

## Run Commands

```bash
# All services
docker compose -f deployments/docker-compose.yml --profile all up -d

# Individual services
docker compose -f deployments/docker-compose.yml run --rm --build fetcher --key=teams

# Metrics
curl -s localhost:8080/metrics | jq
curl localhost:8080/health
```

## Env Vars

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_URL` | Redis connection | `redis://localhost:6379/0` |
| `POSTGRES_URL` | PostgreSQL connection | `postgres://postgres:postgres@localhost:5432/pipeline?sslmode=disable&pool_max_conns=20` |
| `LEGACY_POSTGRES_URL` | Legacy PostgreSQL (port 5433) | `postgres://postgres:postgres@localhost:5433/legacy?sslmode=disable&pool_max_conns=10` |
| `TARGET_API_URL` | Collector target URL | `https://httpbin.org/json` |
| `PROXY_PROVIDER_URL` | Proxy provider API | `` |
| `PROXY_LOCAL_FILE` | Local proxy list | `deployments/proxy.json` |
| `SQL_DIR` | SQL query files | `deployments/queries` |
| `COLLECTOR_WORKERS` | Collector goroutines | `10` |
| `PARSER_WORKERS` | Parser goroutines | `5` |
| `PROXY_REFRESH_MIN` | Proxy refresh (min) | `15` |
| `HEALTH_CHECK_URL` | Proxy health check URL | `https://httpbin.org/ip` |
| `SKIP_TLS_VERIFY` | Skip TLS verification | `false` |
| `MONITOR_PORT` | HTTP port | `8080` |
| `DLQ_BATCH_SIZE` | DLQ batch size | `100` |
| `DLQ_MAX_PER_TICK` | DLQ max per tick | `500` |
| `MAX_RETRIES` | Max retry attempts | `3` |
| `MAX_PROXY_FAILS` | Consecutive failures before penalty | `3` |
| `MAX_PROXY_REQ_MIN` | Max requests per proxy/min | `60` |
| `MAX_PROXY_REQ_DAY` | Max requests per proxy/day | `3000` |
| `MAX_QUEUE_SIZE` | Max fetch queue size | `10000` |

## Services

| Service | Type | Description |
|----------|------|-------------|
| `fetcher` | CLI (one-shot) | Loads SQL, pushes fetch tasks to queue |
| `collector` | Daemon | BLPOP from fetch_queue, HTTP fetch via proxy |
| `parser` | Daemon | BLPOP from parse_queue, PG upsert |
| `proxy-manager` | Daemon | Refreshes proxy pool, health checks |
| `monitor` | Daemon | `/health` + `/metrics` on port 8080 |

## Architecture

```
[SQL] → [Fetcher] → [Redis: fetch_queue]
                   ↓
             [Collector] → [Redis: parse_queue]
                   ↓
               [Parser] → [PostgreSQL: parsed_data]
```

- **Fetcher**: CLI `--key` flag (`default`, `teams`, `players`, `leagues`), runs once, filters IDs against DB, respects `MAX_QUEUE_SIZE`
- **Collector**: BLPOP from fetch_queue, proxy rotation with weighted random, re-enqueues on rate limit (HTTP 429)
- **Parser**: BLPOP from parse_queue, extracts `id` or `match_id` from payload, upserts to PG, DLQ on failure
- **ProxyManager**: Periodic refresh (15min), health check (semaphore 50 concurrent), merges local + provider
- **Monitor**: `/health` (Redis + Postgres ping), `/metrics` (queue lengths)

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

## Proxy Scoring (Lua)

- **Initial**: 1000 (from ZADD on AddProxies)
- **Success**: `+1` (via `recordSuccessScript`)
- **Failure**: penalty per fail count (via `recordFailureScript`), score = -1000 at maxFails, removed from pool
- **Selection**: weighted random via `weightedRandomProxyScript` (fraction = random float)

## Rate Limiting (Lua)

- Atomic via `rateLimitScript`:
  - `proxy_req_min:{IP}` - INCR, EXPIRE 60s
  - `proxy_req_day:{IP}` - INCR, EXPIRE 86400s
- Skips if max = 0 (unlimited)
- Removes proxy from pool if day limit exceeded

## Key Files

| Path | Purpose |
|------|---------|
| `cmd/fetcher/main.go` | Fetcher entry |
| `cmd/collector/main.go` | Collector entry |
| `cmd/parser/main.go` | Parser entry |
| `cmd/monitor/main.go` | HTTP server |
| `cmd/proxy-manager/main.go` | Proxy manager |
| `internal/config/config.go` | Env config |
| `internal/worker/collector.go` | Worker loop, proxy rotation |
| `internal/worker/fetcher.go` | Queue capacity, ID filtering |
| `internal/worker/parser.go` | DLQ drain, parse/upsert |
| `internal/worker/proxy_manager.go` | Proxy refresh |
| `internal/storage/redis/client.go` | Redis client |
| `internal/storage/redis/proxy.go` | Pool, scoring |
| `internal/storage/redis/ratelimit.go` | Rate limit Lua |
| `internal/storage/redis/queue.go` | Queue ops |
| `internal/storage/postgres/client.go` | PG client |
| `internal/storage/postgres/repository.go` | PG repository |
| `internal/storage/postgres/legacy_repository.go` | Legacy parsed_data |
| `internal/models/models.go` | Data models |
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

## Testing

```bash
go test ./...
go build ./...
```

## Build

```bash
# Cached rebuild
docker compose -f deployments/docker-compose.yml --profile all build
docker compose -f deployments/docker-compose.yml --profile all up -d

# Full rebuild from scratch
cd deployments
docker buildx bake --no-cache
```

## Key Dependencies

- `github.com/jackc/pgx/v5` - PostgreSQL
- `github.com/redis/go-redis/v9` - Redis
- `github.com/ilyakaznacheev/cleanenv` - Config from env
- `golang.org/x/sync/semaphore` - Weighted semaphore
- `github.com/google/uuid` - UUID generation