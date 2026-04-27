# AGENTS.md – Guidelines for AI Coding Agents

## Repository Structure
- `cmd/` – one main per service (`cmd/collector/main.go`, `cmd/fetcher/main.go`, …)
- `internal/` – core logic, organised by domain:
  - `config` – environment‑based configuration (cleanenv)
  - `logger` – structured JSON logger (slog)
  - `httpx` – proxy‑aware HTTP client with connection pooling
  - `models` – typed DTOs matching the OpenDota API (Match, Player, etc.)
  - `storage`
    - `postgres` – repository layer (pgxpool, migrations, partitioned queries)
    - `redis` – Lua‑scripted proxy pool, queues, rate limiting
  - `worker` – service logic (collector, fetcher, parser, enricher, etc.)
- `deployments/` – Dockerfiles, docker‑compose, SQL query files, proxy sample
- `testdata/` – test fixtures
- `dump/` – not tracked (scratch space)

## Build & Run
Each service is a standalone binary.  
Build example: `go build -o bin/collector ./cmd/collector`

Docker images are built with `docker buildx bake` using `deployments/docker‑bake.hcl`.  
To run locally without Docker, start Redis + PostgreSQL, then launch each service in separate terminals with the required env vars (see `internal/config/config.go`).

## Coding Conventions
- **Logging**: Use `slog.Default()`; pass a logger instance from `main()`. Log keys are lowercase snake_case (e.g. `worker_id`).
- **Error handling**: Return wrapped errors with context (`fmt.Errorf("…: %w", err)`). No panics in goroutines – always handle or log.
- **Concurrency**: Long‑running workers use `sync.WaitGroup`, signal context, and select loops. Use `semaphore.Weighted` for bounded parallelism.
- **Database**: All PostgreSQL writes go through `Repository` methods, which use `pgxpool.Pool`. Migrations are embedded and idempotent (advisory lock). Use `ON CONFLICT` for upserts.
- **Redis scripts**: Complex atomic operations (proxy selection, rate limiting, requeue) are implemented as Redis Lua scripts stored in the `redis` package and executed via `goredis.Script`.
- **Models**: The `Match` struct uses pointer fields for nullable API values, `json.RawMessage` for cold JSONB, and typed enums (e.g., `PlayerSlot`). Validate before ingestion.

## Working with this Codebase
1. **Adding a new service**: Create a `cmd/<name>/main.go` that loads config, clients, and runs a worker loop. Implement the worker logic in `internal/worker/<name>.go`. Add a Dockerfile and service entry in `docker‑compose.yml`.
2. **Extending the data model**:
   - If the OpenDota API changes, update `internal/models/match.go` (new fields must be nullable/pointer unless always present).
   - Update the matching upsert queries in `internal/storage/postgres/repository_*.go`.
   - If adding a new table, create a migration in `internal/storage/postgres/migrations/` (prefix with a sequence number).
   - When modifying existing tables, write idempotent migration SQL (e.g., `ALTER TABLE ... DROP CONSTRAINT IF EXISTS`). Never use CHECK constraints on API-derived columns (OpenDota tier values have expanded beyond the original enum).
3. **Adding a new queue**: Define the Redis keys in `internal/storage/redis/queue.go`, add relevant push/pop methods, and wire them into workers. Use atomic Lua scripts for consistency where needed.
4. **Testing**: Currently limited. When adding tests, place them next to the package under test (e.g., `internal/worker/collector_test.go`). Use `testcontainers‑go` for integration tests with real Redis/Postgres.
5. **Configuration**: All tunables come from environment variables. Never hard‑code hostnames, ports, or credentials. The `config.Config` struct is the single source of truth.

## Important Invariants
- **Match ingestion is idempotent** – re‑ingesting the same match must not create duplicates. This relies on `ON CONFLICT` and advisory locks.
- **Proxy pool is reset atomically** – `AddProxies` deletes the old pool and bulk‑inserts new proxies in one pipeline.
- **Fetch queue capacity check is non‑atomic** – a TOCTOU race may cause slight overshoot; this is acceptable but do not rely on exact cap.
- **Parser DLQ recovery** – tasks in `failed_queue` have their retry count stored in Redis. Never manually delete retry counts unless you also delete the task.
- **Partition management** – the `matches` table primary key is `(match_id, start_time)`. All inserts MUST provide `start_time` to avoid scanning all partitions. The partition manager only works on `matches`; child tables (`player_matches`) are not yet partitioned.