1. **Fetcher** queries the explorer API, deduplicates against PostgreSQL, and pushes `FetchTask` messages onto `fetch_queue`.
2. **Collector** workers pull tasks, fetch the full match JSON through a rotating proxy pool, and atomically store raw data + push a task ID to `parse_queue`.
3. **Parser** workers deserialise, validate, and upsert matches into PostgreSQL using partitioned tables with advisory locks for idempotency.
4. **Enricher** periodically syncs lookup data (heroes, items, patches, leagues, teams) from the OpenDota constants API.
5. **Partition Manager** creates future quarterly partitions and optionally detaches/drops old data.
6. **Monitor** exposes a health & metrics endpoint (queue lengths, DB counts).

## Component Map
| Service              | Entrypoint       | Role                                                      |
|----------------------|------------------|-----------------------------------------------------------|
| **proxy‑manager**    | `cmd/proxy‑manager`  | Populates Redis proxy pool (local file + provider API)   |
| **fetcher**          | `cmd/fetcher`       | Discovers new match IDs and enqueues them                |
| **collector**        | `cmd/collector`     | Downloads match JSON via proxies                         |
| **parser**           | `cmd/parser`        | Ingests matches into PostgreSQL                          |
| **enricher**         | `cmd/enricher`      | Refreshes lookup tables                                  |
| **monitor**          | `cmd/monitor`       | HTTP health & metrics                                    |
| **partition‑manager**| `cmd/partition‑manager` | Manages quarterly match partitions                  |

## Technology Stack
- **Language:** Go 1.24+
- **Message Queue / Cache:** Redis (go‑redis) – queues, proxy pool, raw data storage
- **Database:** PostgreSQL 16+ with **declarative partitioning** (RANGE on `start_time`, quarterly partitions)
- **External API:** OpenDota (matches, explorer, constants)
- **Deployment:** Docker Compose, separate images per service

## Key Design Decisions

### 1. Queue‑Driven, At‑Least‑Once Processing
Each step uses Redis lists (`fetch_queue`, `parse_queue`) with idempotent downstream operations:
- Fetcher marks discovered IDs in a Redis seen‑set to avoid re‑push.
- Collector atomically stores raw data and queues the task ID.
- Parser uses `ON CONFLICT DO UPDATE` and advisory locks (`pg_try_advisory_xact_lock`) to make re‑ingestion safe.

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

Migrations are embedded (`internal/storage/postgres/migrations/`) and applied by the parser/fetcher startup.

## Deployment
All services are built from a shared base image (`Dockerfile.base`) and deployed via `docker‑compose.yml`.  
Configuration is provided through environment variables (see `internal/config/config.go`).  
The `monitor` service exposes a simple HTTP endpoint for liveness and metrics.

## Future Improvements
- Add a bloom filter in Redis for fast match‑ID deduplication before PostgreSQL.
- Use a reference‑counted transport pool to avoid closing connections in use.
- Implement a dead‑letter queue for fetch tasks to match the parser DLQ.