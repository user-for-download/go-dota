1. **Fetcher** queries the OpenDota Explorer SQL, deduplicates against PostgreSQL/Redis, pushes IDs to `fetch_queue`. Runs as daemon with configurable interval (default 24h).
2. **Collector** workers pull from `fetch_queue`, fetch match JSON via rotating proxy pool, push to `parse_queue`. Tracks network/rate-limit attempts separately for retry budget.
3. **Parser** deserialises, validates, ingests into PostgreSQL using partitioned tables + advisory locks. On transient lock conflicts, retries via DLQ with 5-minute budget.
4. **Enricher** periodically syncs lookup data (heroes, items, patches, leagues, teams) from OpenDota API.
5. **Partition Manager** creates future quarterly partitions and optionally detaches/drops old data.
6. **Proxy Manager** maintains healthy proxy pool in Redis, health-checks against configurable endpoint.
7. **Migrate** is a one-shot service that runs pending schema migrations on startup.
8. **Monitor** exposes `/health` and `/metrics` (queue depths, DLQ age, retry stats).

## Component Map
| Service              | Entrypoint           | Role                                                      |
|----------------------|----------------------|-----------------------------------------------------------|
| **migrate**          | `cmd/migrate`        | One-shot schema migration runner                         |
| **proxy‑manager**   | `cmd/proxy‑manager`  | Populates Redis proxy pool (local file + provider API)   |
| **fetcher**          | `cmd/fetcher`        | Discovers new match IDs (daemon with --interval)         |
| **collector**        | `cmd/collector`      | Downloads match JSON via proxies                         |
| **parser**           | `cmd/parser`         | Ingests matches into PostgreSQL with DLQ retries         |
| **enricher**         | `cmd/enricher`       | Refreshes lookup tables                                  |
| **monitor**          | `cmd/monitor`        | HTTP health & metrics                                    |
| **partition‑manager**| `cmd/partition‑manager`| Manages quarterly match partitions                      |

## Technology Stack
- **Language:** Go 1.24+
- **Message Queue / Cache:** Redis (go‑redis) – queues, proxy pool, raw data storage
- **Database:** PostgreSQL 16+ with **declarative partitioning** (RANGE on `start_time`, quarterly partitions)
- **External API:** OpenDota (matches, explorer, constants)
- **Deployment:** Docker Compose, separate images per service

### Service Readiness
All long-running services use `readiness.WaitAll()` to wait for dependencies before starting:
- **Redis**: `service_healthy` condition
- **PostgreSQL**: `service_healthy` condition
- **Schema migrations**: waits for `001_init.sql` via `readiness.SchemaApplied()`
- **Proxy pool**: waits for at least 5 proxies in Redis via `readiness.ProxyPool()`

This ensures services don't crash on startup if dependencies aren't ready—they loop until all probes pass.

## Key Design Decisions

### 1. Queue‑Driven, At‑Least‑Once Processing
Each step uses Redis lists (`fetch_queue`, `parse_queue`) with idempotent downstream operations:
- Fetcher marks discovered IDs in a Redis seen‑set to avoid re‑push.
- Collector atomically stores raw data and queues the task ID. Tracks `network_attempts` and `rate_limit_retries` separately for independent budget checking.
- Parser uses `ON CONFLICT DO UPDATE` and advisory locks (`pg_try_advisory_xact_lock`) to make re‑ingestion safe.
- **Dead Letter Queue**: Failed parse tasks go to `failed_queue` with retry metadata. Parser tracks per-task budget (5-minute total timeout). Metrics exposed: `parser_retry_count_avg`, `dlq_oldest_age_seconds`.

### 2. Proxy Rotation & Rate Limiting
The **collector** uses a weighted random proxy selection implemented as a Redis Lua script.  
Per‑proxy failure counters temporarily penalise proxies; persistent failures evict them.  
Per‑proxy rate limits (req/min, req/day) are enforced atomically in Redis.

### 3. Partitioned Matches
The `matches` table is partitioned by **quarter** (`start_time`) to keep scans efficient as data grows.  
The primary key includes `start_time` to allow partition pruning.  
Lookup tables (`heroes`, `items`, …) are not partitioned – they are small.

### 4. Lookup Table Stubs
To decouple match ingestion from enrichment, stub rows are inserted `ON CONFLICT DO NOTHING` before inserting match data.  
This satisfies foreign‑key constraints without requiring the enricher to have run.

### 5. Validation & Poison Message Handling
Match payloads are fully validated (positive IDs, player count, slot uniqueness).  
Invalid messages are sent to `permanent_failed_queue` and never retried.

## Database Schema Overview
- **`matches`** – core match metadata, partitioned by `start_time` (quarterly).
- **`player_matches`** – one row per player per match (hot path, columns for dashboards).
- **`player_match_details`** – JSONB blobs with cold analytics (damage, wards, etc.).
- **`match_objectives`, `match_chat`, `match_teamfights`** – event tables.
- **`player_timeseries`** – per‑minute gold/xp/LH/DN for parsed matches.
- **`teams`, `players`, `heroes`, `items`, `leagues`, `patches`** – enrichment data.

Migrations are embedded (`internal/storage/postgres/migrations/`) and applied by the dedicated `migrate` service at startup.

### Schema Migrations
Migrations are managed by the dedicated `migrate` service (`cmd/migrate`):
- **One-shot**: Runs on startup with `restart: no`, depends on `postgres:service_healthy`
- **Embedded**: SQL files are bundled into the binary via `go:embed`.
- **Advisory lock**: Uses `pg_advisory_lock($1)` with 60-second session-level `lock_timeout` (not `SET LOCAL`, as there's no transaction).
- **Idempotent**: Each migration is recorded in `schema_migrations` table; duplicates are skipped.
- **Cleanup**: Resets `lock_timeout` to DEFAULT before returning connection to pool.

## Deployment
All services are built from a shared base image (`Dockerfile.base`) and deployed via `docker‑compose.yml`.  
Configuration is provided through environment variables (see `internal/config/config.go`).  
The `monitor` service exposes a simple HTTP endpoint for liveness and metrics.

## Future Improvements
- Add a bloom filter in Redis for fast match‑ID deduplication before PostgreSQL.
- Use a reference‑counted transport pool to avoid closing connections in use.
- Add DLQ for fetch tasks (collector failures) to match parser DLQ.
- Implement per‑quarter partition retention policies.