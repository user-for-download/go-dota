1. **Fetcher** queries the OpenDota Explorer SQL, deduplicates against PostgreSQL/Redis, pushes IDs to `fetch_tasks` stream. Checks queue capacity before pushing to prevent Redis memory exhaustion. Runs as daemon with configurable interval (default 24h).
2. **Collector** workers consume from `fetch_tasks` stream via Redis consumer groups, fetch match JSON (max 10MB) via rotating proxy pool, push to `parse_tasks` stream. Tracks network/rate-limit attempts separately for retry budget. Uses leased proxy pattern with automatic release on context cancellation.
3. **Parser** consumes from `parse_tasks` stream via consumer groups, deserialises, validates, ingests into PostgreSQL using partitioned tables + advisory locks. Transient errors (serialization failure, deadlock, advisory lock conflict, other) retry via DLQ with 5-minute budget. Permanent errors (FK violation, check violation, not-null, unique, validation) go directly to DLQ.
4. **Enricher** periodically syncs lookup data (heroes, items, patches, leagues, teams) from OpenDota API.
5. **Partition Manager** creates future quarterly partitions aligned to quarter boundaries and optionally detaches/drops old data.
6. **Proxy Manager** maintains healthy proxy pool in Redis, health-checks against configurable endpoint.
7. **Migrate** is a one-shot service that runs pending schema migrations on startup.
8. **Monitor** exposes `/health` and `/metrics` (queue depths, DLQ age, retry stats, ingest success/failure by kind).

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
- **Language:** Go 1.25+
- **Message Queue / Cache:** Redis (go‑redis) – queues, proxy pool, raw data storage, metrics
- **Database:** PostgreSQL 16+ with **declarative partitioning** (RANGE on `start_time`, quarterly partitions)
- **External API:** OpenDota (matches, explorer, constants)
- **Deployment:** Docker Compose, separate images per service

### Service Readiness
All long-running services use `readiness.WaitAll()` to wait for dependencies before starting:
- **Redis**: `service_healthy` condition
- **PostgreSQL**: `service_healthy` condition
- **Schema migrations**: waits for `001_init.sql` via `readiness.SchemaApplied()`
- **Proxy pool**: waits for at least 5 proxies in Redis via `readiness.ProxyPool()`
- **Enricher bootstrap**: waits for `enricher:bootstrapped` marker (critical enricher steps must succeed)

This ensures services don't crash on startup if dependencies aren't ready—they loop until all probes pass.

## Key Design Decisions

### 1. Queue‑Driven, At‑Least‑Once Processing
Each step uses Redis Streams (`fetch_tasks`, `parse_tasks`) with consumer groups for scalable parallel processing:
- Fetcher marks discovered IDs in a Redis seen‑set to avoid re‑push. Checks stream length against `MAX_QUEUE_SIZE` before pushing to prevent Redis memory exhaustion.
- Collector atomically stores raw data (max 10MB) and queues the task ID. Uses leased proxy pattern with defer-release to prevent leaks on panic. Tracks `network_attempts` and `rate_limit_retries` separately for independent budget checking.
- Parser uses `ON CONFLICT DO UPDATE` and advisory locks (`pg_try_advisory_xact_lock`) to make re‑ingestion safe.
- **Dead Letter Queue**: Failed parse tasks go to `failed_queue` with retry metadata. Transient errors (serialization, deadlock, locked, other) retry; permanent errors (FK, check, not-null, unique, validation) go to DLQ. Parser tracks per-task budget (5-minute total timeout). Metrics exposed: `parser_retry_count_avg`, `dlq_oldest_age_seconds`, `ingest_failed_by_kind`.

### 2. Enricher Bootstrap Gate
The **enricher** populates lookup tables before parser runs:
- **Critical steps** (patches, heroes, items, game_modes, lobby_types, leagues) must succeed before `enricher:bootstrapped` marker is set in Redis.
- **Soft steps** (teams) don't block bootstrap - matches with new teams will FK fail and retry after next enricher pass.
- Parser/Fetcher wait on the bootstrap marker via `readiness.EnricherBootstrapped()`.
- This eliminates the need for stub inserts in the ingest path.

### 3. Error Classification & Retry Logic
Errors are classified by PostgreSQL error code:
- **Retry (transient)**: `serialization_failure` (40001), `deadlock` (40P01), advisory lock conflict, network/connection errors (`IngestErrOther`).
- **DLQ (permanent)**: FK violation (23503), check violation (23514), not-null violation (23502), unique violation (23505), validation failure, unmarshal failure.
- Missing teams/heroes/leagues/patches cause FK errors; tasks retry after next enricher pass.

### 4. Proxy Rotation & Rate Limiting
The **collector** uses a weighted random proxy selection implemented as a Redis Lua script.  
Per‑proxy failure counters temporarily penalise proxies; persistent failures evict them.  
Per‑proxy rate limits (req/min, req/day) are enforced atomically in Redis.  
Malformed proxy URLs are validated and removed from the pool.

### 5. Partitioned Matches
The `matches` table is partitioned by **quarter** (`start_time`) to keep scans efficient as data grows.  
The primary key includes `start_time` to allow partition pruning.  
Lookup tables (`heroes`, `items`, …) are not partitioned – they are small.

### 6. Bulk Upsert Batching
All bulk upserts are chunked to avoid PostgreSQL's 65,535 parameter limit:
- Teams, heroes, leagues, items, game_modes, lobby_types, patches all use 1000-row batches.
- Each batch stays well under the parameter limit.

### 7. Validation & Poison Message Handling
Match payloads are fully validated (positive IDs, player count, slot uniqueness).  
Invalid messages are sent to `permanent_failed_queue` and never retried.  
Metrics track failures by kind (`unmarshal`, `validation`, `fk_violation`, `match_locked`, etc.).

## Database Schema Overview
- **`matches`** – core match metadata, partitioned by `start_time` (quarterly).
- **`player_matches`** – one row per player per match (hot path, columns for dashboards). Timeseries columns (`gold_t`, `xp_t`, `lh_t`, `dn_t`, `times`) use native PostgreSQL `INTEGER[]` arrays.
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
- Add structured tracing / correlation IDs for distributed debugging.
- Add unit tests for critical paths (models validation, timeseries building).