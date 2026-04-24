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
| `TARGET_API_URL` | Collector target URL | `https://httpbin.org/json` |
| `PROXY_PROVIDER_URL` | Proxy provider API | `` |
| `PROXY_LOCAL_FILE` | Local proxy list | `deployments/proxy.json` |
| `SQL_DIR` | SQL query files | `deployments/queries` |
| `COLLECTOR_WORKERS` | Collector goroutines | `10` |
| `PARSER_WORKERS` | Parser goroutines | `5` |
| `FETCH_INTERVAL_SEC` | Fetch interval | `5` |
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
| `fetcher` | CLI (one-shot) | Loads SQL, pushes fetch tasks |
| `collector` | Daemon | BLPOP from fetch_queue, HTTP fetch |
| `parser` | Daemon | BLPOP from parse_queue, PG upsert |
| `proxy-manager` | Daemon | Refreshes proxy pool periodically |
| `monitor` | Daemon | `/health` + `/metrics` on port 8080 |

## Architecture

```
[SQL] → [Fetcher] → [Redis: fetch_queue]
                   ↓
             [Collector] → [Redis: parse_queue]
                   ↓
               [Parser] → [PostgreSQL]
```

- **Fetcher**: CLI `--key` flag, runs once, exits. Checks queue capacity inline.
- **Collector**: BLPOP from fetch_queue, circuit breaker with exponential backoff (1s-60s), re-enqueues on rate limit.
- **Parser**: BLPOP from parse_queue, DLQ recovery on startup, periodic drain every 5min.
- **ProxyManager**: Periodic refresh, health check with semaphore (50 concurrent).
- **Monitor**: `/health` (liveness), `/metrics` (queue lengths).

## Redis Keys

| Key | Type | Purpose | TTL |
|-----|------|---------|-----|
| `proxy_pool` | SET | Active proxies | - |
| `proxy_ranking` | ZSET | Proxy scores (0-10000) | - |
| `fetch_queue` | LIST | Tasks to collect | - |
| `parse_queue` | LIST | Tasks to parse | - |
| `failed_queue` | LIST | Retryable DLQ (LIFO) | - |
| `permanent_failed_queue` | LIST | Max retries exceeded | - |
| `raw_data:{TaskID}` | STRING | JSON payload | 7200s |
| `seen_fetch_ids` | SET | Fetch deduplication | 86400s |
| `seen_parse_ids` | SET | Parse deduplication | 86400s |
| `proxy_req_min:{IP}` | STRING | Per-minute rate limit | 60s |
| `proxy_req_day:{IP}` | STRING | Per-day rate limit | 86400s |
| `proxy_fail:{Proxy}` | STRING | Consecutive failure count | - |

## Circuit Breaker

States: `closed` → `open` → `halfopen`

- **Closed**: Normal operation
- **Open**: After 5 failures, wait reset timeout (30s)
- **Halfopen**: Test recovery, one probe allowed (`probeInFlight` guard)
- **Backoff**: Exponential (1s × 2^failures, max 60s)

## Proxy Scoring

- Initial: 1000
- Success: `+10` (capped at 10000)
- Failure: `-penalty × fails` (penalty = 100)
- Max fails reached: score = -1000, removed from pool
- Selection: Weighted random (fraction = 0.95)

## Rate Limiting

Lua script atomically checks/enforces per-proxy limits:
- `proxy_req_min:{IP}` - expires 60s
- `proxy_req_day:{IP}` - expires 86400s

Skips limiting if max = 0.

## Key Files

| Path | Purpose |
|------|---------|
| `cmd/fetcher/main.go` | Fetcher entry |
| `cmd/collector/main.go` | Collector entry |
| `cmd/parser/main.go` | Parser entry + migrations |
| `cmd/monitor/main.go` | HTTP server |
| `cmd/proxy-manager/main.go` | Proxy manager |
| `internal/config/config.go` | Env config |
| `internal/worker/collector.go` | Circuit breaker, worker loop |
| `internal/worker/fetcher.go` | Queue capacity |
| `internal/worker/parser.go` | DLQ drain |
| `internal/worker/proxy_manager.go` | Proxy refresh |
| `internal/storage/redis/client.go` | Client config |
| `internal/storage/redis/proxy.go` | Pool, scoring |
| `internal/storage/redis/ratelimit.go` | Rate limit Lua |
| `internal/storage/redis/queue.go` | Queue ops |
| `internal/storage/postgres/repository.go` | PG operations |
| `internal/storage/postgres/migrate.go` | Migration runner |
| `internal/storage/postgres/client.go` | PG client |
| `internal/models/` | Data models |
| `internal/httpx/` | HTTP client + socks5 |
| `deployments/queries/*.sql` | Fetch SQL queries |

## Database Schema

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

Migrations in `internal/storage/postgres/migrations/*.sql` run via `repo.Migrate(ctx)`.

## Testing

```bash
go test ./...
go build ./...
```

## Build

```bash
# Full rebuild
cd deployments && docker buildx bake --no-cache && cd ..

# Cached
docker compose -f deployments/docker-compose.yml --profile all build
docker compose -f deployments/docker-compose.yml --profile all up -d
```

## Key Dependencies

- `github.com/jackc/pgx/v5` - PostgreSQL
- `github.com/redis/go-redis/v9` - Redis
- `github.com/ilyakaznacheev/cleanenv` - Config from env
- `github.com/testcontainers/testcontainers-go` - Containerized tests
- `golang.org/x/sync/semaphore` - Weighted semaphore
- `github.com/google/uuid` - UUID generation

docker buildx prune -f

# 3. Rebuild from scratch
cd /home/ubuntu/git/od/deployments
docker buildx bake --no-cache


❯ docker compose -f deployments/docker-compose.yml --profile all up
[+] up 8/8
 ✔ Network deployments_default           Created                                                                                                  0.0s
 ✔ Container deployments-postgres-1      Created                                                                                                  0.1s
 ✔ Container deployments-redis-1         Created                                                                                                  0.1s
 ✔ Container deployments-collector-1     Created                                                                                                  0.0s
 ✔ Container deployments-parser-1        Created                                                                                                  0.0s
 ✔ Container deployments-proxy-manager-1 Created                                                                                                  0.0s
 ✔ Container deployments-monitor-1       Created                                                                                                  0.0s
 ✔ Container deployments-fetcher-1       Created                                                                                                  0.0s
Attaching to collector-1, fetcher-1, monitor-1, parser-1, postgres-1, proxy-manager-1, redis-1
Container deployments-redis-1 Waiting
Container deployments-redis-1 Waiting
redis-1  | 1:C 23 Apr 2026 13:54:31.365 * oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo
redis-1  | 1:C 23 Apr 2026 13:54:31.365 * Redis version=7.4.8, bits=64, commit=00000000, modified=0, pid=1, just started
redis-1  | 1:C 23 Apr 2026 13:54:31.365 # Warning: no config file specified, using the default config. In order to specify a config file use redis-server /path/to/redis.conf
redis-1  | 1:M 23 Apr 2026 13:54:31.365 * Increased maximum number of open files to 10032 (it was originally set to 1024).
redis-1  | 1:M 23 Apr 2026 13:54:31.365 * monotonic clock: POSIX clock_gettime
redis-1  | 1:M 23 Apr 2026 13:54:31.366 * Running mode=standalone, port=6379.
redis-1  | 1:M 23 Apr 2026 13:54:31.366 * Server initialized
redis-1  | 1:M 23 Apr 2026 13:54:31.366 * Loading RDB produced by version 7.4.8
redis-1  | 1:M 23 Apr 2026 13:54:31.366 * RDB age 1030 seconds
redis-1  | 1:M 23 Apr 2026 13:54:31.366 * RDB memory usage when created 3.10 Mb
Container deployments-postgres-1 Waiting
Container deployments-redis-1 Waiting
Container deployments-redis-1 Waiting
Container deployments-postgres-1 Waiting
redis-1  | 1:M 23 Apr 2026 13:54:31.375 * Done loading RDB, keys loaded: 349, keys expired: 166.
redis-1  | 1:M 23 Apr 2026 13:54:31.375 * DB loaded from disk: 0.009 seconds
redis-1  | 1:M 23 Apr 2026 13:54:31.375 * Ready to accept connections tcp
postgres-1  |
postgres-1  | PostgreSQL Database directory appears to contain a database; Skipping initialization
postgres-1  |
postgres-1  | 2026-04-23 13:54:31.410 UTC [1] LOG:  starting PostgreSQL 15.17 on x86_64-pc-linux-musl, compiled by gcc (Alpine 15.2.0) 15.2.0, 64-bit
postgres-1  | 2026-04-23 13:54:31.411 UTC [1] LOG:  listening on IPv4 address "0.0.0.0", port 5432
postgres-1  | 2026-04-23 13:54:31.411 UTC [1] LOG:  listening on IPv6 address "::", port 5432
postgres-1  | 2026-04-23 13:54:31.412 UTC [1] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
postgres-1  | 2026-04-23 13:54:31.415 UTC [28] LOG:  database system was shut down at 2026-04-23 13:37:21 UTC
postgres-1  | 2026-04-23 13:54:31.420 UTC [1] LOG:  database system is ready to accept connections
Container deployments-redis-1 Healthy
Container deployments-redis-1 Healthy
Container deployments-redis-1 Healthy
Container deployments-redis-1 Healthy
Container deployments-postgres-1 Healthy
Container deployments-postgres-1 Healthy
parser-1    | {"time":"2026-04-23T13:54:34.051775341Z","level":"INFO","msg":"starting parser"}
collector-1  | {"time":"2026-04-23T13:54:34.060991422Z","level":"INFO","msg":"starting collector"}
collector-1  | {"time":"2026-04-23T13:54:34.062655258Z","level":"INFO","msg":"collector starting workers","count":10}
collector-1  | {"time":"2026-04-23T13:54:34.062748939Z","level":"INFO","msg":"collector worker started","worker_id":9}
collector-1  | {"time":"2026-04-23T13:54:34.062788318Z","level":"INFO","msg":"collector worker started","worker_id":4}
collector-1  | {"time":"2026-04-23T13:54:34.06280301Z","level":"INFO","msg":"collector worker started","worker_id":0}
collector-1  | {"time":"2026-04-23T13:54:34.062810119Z","level":"INFO","msg":"collector worker started","worker_id":1}
collector-1  | {"time":"2026-04-23T13:54:34.062825234Z","level":"INFO","msg":"collector worker started","worker_id":2}
collector-1  | {"time":"2026-04-23T13:54:34.062833518Z","level":"INFO","msg":"collector worker started","worker_id":3}
collector-1  | {"time":"2026-04-23T13:54:34.06284444Z","level":"INFO","msg":"collector worker started","worker_id":6}
collector-1  | {"time":"2026-04-23T13:54:34.062851406Z","level":"INFO","msg":"collector worker started","worker_id":5}
collector-1  | {"time":"2026-04-23T13:54:34.062866149Z","level":"INFO","msg":"collector worker started","worker_id":7}
collector-1  | {"time":"2026-04-23T13:54:34.06287387Z","level":"INFO","msg":"collector worker started","worker_id":8}
parser-1     | {"time":"2026-04-23T13:54:34.071954776Z","level":"INFO","msg":"parser starting workers","count":5}
parser-1     | {"time":"2026-04-23T13:54:34.071981079Z","level":"INFO","msg":"attempting DLQ recovery on startup"}
parser-1     | {"time":"2026-04-23T13:54:34.072193631Z","level":"INFO","msg":"parser worker started","worker_id":4}
parser-1     | {"time":"2026-04-23T13:54:34.072236724Z","level":"INFO","msg":"parser worker started","worker_id":1}
parser-1     | {"time":"2026-04-23T13:54:34.072268975Z","level":"INFO","msg":"parser worker started","worker_id":0}
parser-1     | {"time":"2026-04-23T13:54:34.072277528Z","level":"INFO","msg":"parser worker started","worker_id":2}
parser-1     | {"time":"2026-04-23T13:54:34.072288261Z","level":"INFO","msg":"parser worker started","worker_id":3}
proxy-manager-1  | {"time":"2026-04-23T13:54:34.087208414Z","level":"INFO","msg":"starting proxy manager"}
proxy-manager-1  | {"time":"2026-04-23T13:54:34.088998236Z","level":"INFO","msg":"proxy manager started","refresh_interval":900000000000}
proxy-manager-1  | {"time":"2026-04-23T13:54:34.08901692Z","level":"INFO","msg":"refreshing proxy pool"}
Container deployments-postgres-1 Waiting
Container deployments-redis-1 Waiting
monitor-1        | {"time":"2026-04-23T13:54:34.098288035Z","level":"INFO","msg":"starting monitor","port":8080}
proxy-manager-1  | {"time":"2026-04-23T13:54:34.505996356Z","level":"INFO","msg":"fetched proxies from provider","count":66}
proxy-manager-1  | {"time":"2026-04-23T13:54:34.506021291Z","level":"INFO","msg":"fetched proxies","count":66}
Container deployments-postgres-1 Healthy
Container deployments-redis-1 Healthy
fetcher-1        | {"time":"2026-04-23T13:54:34.764988631Z","level":"INFO","msg":"starting fetcher","key":"default"}
fetcher-1        | {"time":"2026-04-23T13:54:34.77542948Z","level":"INFO","msg":"fetcher started","key":"default"}
fetcher-1        | {"time":"2026-04-23T13:54:34.775580235Z","level":"INFO","msg":"proxy pool ready","count":37}
fetcher-1        | {"time":"2026-04-23T13:54:34.775591166Z","level":"INFO","msg":"starting fetchMatchIDs loop","target":"https://api.opendota.com/api/explorer?sql=SELECT+ARRAY_AGG%28match_id+ORDER+BY+start_time+DESC%29+AS+match_ids+FROM+matches+WHERE+start_time+%3E%3D+%28EXTRACT%28EPOCH+FROM+NOW%28%29+-+INTERVAL+%271+day%27%29%29%3A%3ABIGINT%3B"}
fetcher-1        | {"time":"2026-04-23T13:54:34.776102926Z","level":"INFO","msg":"fetching with proxy","attempt":0,"proxy":"socks5://84.21.166.105:1080"}
proxy-manager-1  | {"time":"2026-04-23T13:54:40.716694442Z","level":"INFO","msg":"proxies health checked","total":66,"valid":35}
proxy-manager-1  | {"time":"2026-04-23T13:54:40.717114518Z","level":"INFO","msg":"proxy pool updated","count":35}
fetcher-1        | {"time":"2026-04-23T13:54:49.891903674Z","level":"WARN","msg":"fetch failed, trying next proxy","proxy":"socks5://84.21.166.105:1080","error":"do: Get \"https://api.opendota.com/api/explorer?sql=SELECT+ARRAY_AGG%28match_id+ORDER+BY+start_time+DESC%29+AS+match_ids+FROM+matches+WHERE+start_time+%3E%3D+%28EXTRACT%28EPOCH+FROM+NOW%28%29+-+INTERVAL+%271+day%27%29%29%3A%3ABIGINT%3B\": EOF"}
fetcher-1        | {"time":"2026-04-23T13:54:49.89219372Z","level":"INFO","msg":"fetching with proxy","attempt":1,"proxy":"socks5://158.160.21.167:1080"}
fetcher-1        | {"time":"2026-04-23T13:54:50.167422338Z","level":"INFO","msg":"fetch succeeded","proxy":"socks5://158.160.21.167:1080","match_count":89}
fetcher-1        | {"time":"2026-04-23T13:54:50.167444467Z","level":"INFO","msg":"discovered match IDs","count":89}
fetcher-1        | {"time":"2026-04-23T13:54:50.172946032Z","level":"INFO","msg":"queue capacity check passed, starting push"}
fetcher-1        | {"time":"2026-04-23T13:54:50.177984625Z","level":"INFO","msg":"tasks pushed to queue","count":13}
fetcher-1        | {"time":"2026-04-23T13:54:50.178000293Z","level":"INFO","msg":"fetcher completed successfully"}
fetcher-1 exited with code 0
collector-1      | {"time":"2026-04-23T13:54:50.795261342Z","level":"INFO","msg":"task fetched and queued","task_id":"68306968-2b50-4683-a37a-427f2cf5f373","proxy":"socks5://152.53.144.223:1080","worker_id":6}
parser-1         | {"time":"2026-04-23T13:54:50.824038281Z","level":"INFO","msg":"task parsed and stored","task_id":"68306968-2b50-4683-a37a-427f2cf5f373","external_id":"8782782376","worker_id":4}
collector-1      | {"time":"2026-04-23T13:54:52.169012054Z","level":"INFO","msg":"task fetched and queued","task_id":"83d75c92-d7bf-4e78-a8ce-2fcb061e3c5a","proxy":"socks5://85.143.254.38:1080","worker_id":3}
parser-1         | {"time":"2026-04-23T13:54:52.172475389Z","level":"INFO","msg":"task parsed and stored","task_id":"83d75c92-d7bf-4e78-a8ce-2fcb061e3c5a","external_id":"8782773236","worker_id":4}
collector-1      | {"time":"2026-04-23T13:54:53.730805279Z","level":"INFO","msg":"task fetched and queued","task_id":"dc79bac9-9250-48c0-9b72-a37303d1d2be","proxy":"socks5://58.19.55.4:15067","worker_id":1}
parser-1         | {"time":"2026-04-23T13:54:53.734121261Z","level":"INFO","msg":"task parsed and stored","task_id":"dc79bac9-9250-48c0-9b72-a37303d1d2be","external_id":"8782867414","worker_id":4}
collector-1      | {"time":"2026-04-23T13:54:55.611404528Z","level":"INFO","msg":"task fetched and queued","task_id":"c6f7f2f9-e788-4047-90c6-bcb6127da836","proxy":"socks5://58.19.55.4:15067","worker_id":6}
parser-1         | {"time":"2026-04-23T13:54:55.637677987Z","level":"INFO","msg":"task parsed and stored","task_id":"c6f7f2f9-e788-4047-90c6-bcb6127da836","external_id":"8782722935","worker_id":0}
collector-1      | {"time":"2026-04-23T13:54:57.397225235Z","level":"INFO","msg":"task fetched and queued","task_id":"9b1102c0-2c0a-4366-a4e3-be04648e8188","proxy":"socks5://58.19.55.88:15142","worker_id":0}
parser-1         | {"time":"2026-04-23T13:54:57.431989036Z","level":"INFO","msg":"task parsed and stored","task_id":"9b1102c0-2c0a-4366-a4e3-be04648e8188","external_id":"8782750796","worker_id":4}
collector-1      | {"time":"2026-04-23T13:54:58.618025745Z","level":"INFO","msg":"task fetched and queued","task_id":"f29c9251-93fe-4f72-b0d0-e879c4552d6d","proxy":"socks5://58.19.55.88:15142","worker_id":1}
parser-1         | {"time":"2026-04-23T13:54:58.646243866Z","level":"INFO","msg":"task parsed and stored","task_id":"f29c9251-93fe-4f72-b0d0-e879c4552d6d","external_id":"8782706180","worker_id":0}
collector-1      | {"time":"2026-04-23T13:55:07.330969739Z","level":"INFO","msg":"task fetched and queued","task_id":"a107c5bc-48e7-432e-9fa8-7d1d5841fb63","proxy":"socks5://213.21.233.242:1080","worker_id":8}
parser-1         | {"time":"2026-04-23T13:55:07.3568756Z","level":"INFO","msg":"task parsed and stored","task_id":"a107c5bc-48e7-432e-9fa8-7d1d5841fb63","external_id":"8782781890","worker_id":4}
collector-1      | {"time":"2026-04-23T13:55:08.804868322Z","level":"INFO","msg":"task fetched and queued","task_id":"07984b49-7d4b-4602-90e2-7dfc3308a3bd","proxy":"socks5://89.169.10.171:8081","worker_id":7}
parser-1         | {"time":"2026-04-23T13:55:08.826075161Z","level":"INFO","msg":"task parsed and stored","task_id":"07984b49-7d4b-4602-90e2-7dfc3308a3bd","external_id":"8782776059","worker_id":1}
collector-1      | {"time":"2026-04-23T13:55:09.005951967Z","level":"INFO","msg":"task fetched and queued","task_id":"17543821-a235-492e-8c94-b1e5cacf29cb","proxy":"socks5://62.60.231.71:33089","worker_id":3}
parser-1         | {"time":"2026-04-23T13:55:09.03258113Z","level":"INFO","msg":"task parsed and stored","task_id":"17543821-a235-492e-8c94-b1e5cacf29cb","external_id":"8782704043","worker_id":3}
collector-1      | {"time":"2026-04-23T13:55:09.1812731Z","level":"INFO","msg":"task fetched and queued","task_id":"2bd068a7-80ae-4a55-8856-583d57226417","proxy":"socks5://89.169.10.171:8081","worker_id":4}
parser-1         | {"time":"2026-04-23T13:55:09.204996527Z","level":"INFO","msg":"task parsed and stored","task_id":"2bd068a7-80ae-4a55-8856-583d57226417","external_id":"8782773613","worker_id":2}
collector-1      | {"time":"2026-04-23T13:55:09.501588136Z","level":"INFO","msg":"task fetched and queued","task_id":"d291e03c-e686-4f4a-9272-7b40e0a66dcc","proxy":"socks5://89.208.106.138:10808","worker_id":5}
parser-1         | {"time":"2026-04-23T13:55:09.528037015Z","level":"INFO","msg":"task parsed and stored","task_id":"d291e03c-e686-4f4a-9272-7b40e0a66dcc","external_id":"8782723524","worker_id":0}
collector-1      | {"time":"2026-04-23T13:55:20.498536266Z","level":"INFO","msg":"task fetched and queued","task_id":"7ef2e2c0-139f-4ce2-a65e-7544554e4888","proxy":"socks5://89.169.10.171:8081","worker_id":9}
parser-1         | {"time":"2026-04-23T13:55:20.522954489Z","level":"INFO","msg":"task parsed and stored","task_id":"7ef2e2c0-139f-4ce2-a65e-7544554e4888","external_id":"8782786363","worker_id":0}