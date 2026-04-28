# OpenDota Pipeline

Dota 2 match data ingestion pipeline. Fetches, collects, parses, and stores match data from the OpenDota API into PostgreSQL.

## Services

    Fetcher (cmd/fetcher): Queries the OpenDota Explorer using SQL to discover new match IDs, deduplicates them against Postgres/Redis, and pushes them to the fetch_queue.

    Collector (cmd/collector): Consumes the fetch_queue, rotates through a pool of proxies (with rate limiting and failure tracking via Lua scripts), fetches the raw JSON match data, and pushes it to the parse_queue.

    Parser (cmd/parser): Consumes raw match JSON from the parse_queue, unmarshals and validates it, and safely ingests the massive relational payload into Postgres using transactions, advisory locks, and bulk copies.

    Enricher (cmd/enricher): Periodically updates static/metadata (heroes, items, leagues, teams, patches) from the OpenDota API into the database.

    Proxy Manager (cmd/proxy-manager): Maintains a healthy pool of HTTP/SOCKS proxies (from a provider and/or local file), testing them against a health-check endpoint and updating Redis.

    Partition Manager (cmd/partition-manager): Automates PostgreSQL table partitioning for the matches table (creating future partitions and enforcing retention policies on old ones).

    Monitor (cmd/monitor): Exposes an HTTP server for basic /health and /metrics (queue depths, match counts, retry averages).

Key Patterns & Tech:

    Postgres (pgx/v5): Heavy use of advanced Postgres features like table partitioning, JSONB for cold data, COPY protocol for fast bulk inserts, and advisory locks to prevent concurrent processing of the same match.

    Redis (go-redis/v9): Advanced use of Lua scripts for atomic operations (rate limiting, weighted random proxy selection, DLQ retry logic).

    Resilience: Comprehensive Dead Letter Queue (DLQ) mechanisms, proxy rotation, jittered backoffs, and strict connection pooling timeouts.
## Quick Start

```bash
# Run a service
make rebuild
make up-init
make up
make fetch
```

## Environment

Required env vars (see `internal/config/config.go`):
- `POSTGRES_DSN` – PostgreSQL connection string
- `REDIS_URL` – Redis address

## Tech Stack

- Go 1.24+
- PostgreSQL 16+ (partitioned tables)
- Redis

## Schema

- `matches` – partitioned by quarter (`start_time`)
- `player_matches` – per-player stats (hot data)
- `player_match_details` – JSONB blobs (cold data)
- Lookup tables: `heroes`, `items`, `leagues`, `teams`, `patches`

See `ARCHITECTURE.md` for full details.