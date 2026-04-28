# OpenDota Pipeline

Dota 2 match data ingestion pipeline. Fetches, collects, parses, and stores match data from the OpenDota API into PostgreSQL.

## Services

| Service | Description |
|---------|-------------|
| **fetcher** | Queries OpenDota Explorer SQL to discover new match IDs, dedupes against Postgres/Redis, pushes to fetch_queue. Runs as daemon with configurable interval (default 24h). |
| **collector** | Consumes fetch_queue, rotates through proxy pool (Lua-scripted rate limiting + failure tracking), fetches raw JSON match data, pushes to parse_queue. |
| **parser** | Consumes parse_queue, validates JSON, ingests into Postgres using transactions, advisory locks, bulk COPY. Handles retries via DLQ on transient failures. |
| **enricher** | Periodically updates static/metadata (heroes, items, leagues, teams, patches) from OpenDota API. |
| **proxy-manager** | Maintains healthy HTTP/SOCKS proxy pool (provider + local file), health-checks against configurable endpoint, updates Redis. |
| **partition-manager** | Automates PostgreSQL table partitioning for matches table (future partitions + retention). |
| **monitor** | HTTP server with `/health` and `/metrics` (queue depths, match counts, DLQ age, retry averages). |
| **migrate** | One-shot migration runner using advisory locks to ensure single-instance execution. |

## Key Patterns

- **PostgreSQL**: Table partitioning by quarter (`start_time`), JSONB for cold data, COPY protocol for bulk inserts, advisory locks to prevent concurrent match processing.
- **Redis**: Lua scripts for atomic proxy selection, rate limiting, and DLQ retry logic.
- **Resilience**: DLQ with retry budget (5min timeout), proxy rotation, jittered backoffs, idempotent ingestion.

## Quick Start

```bash
# Build and start all services
make rebuild
make up

# View logs
make logs

# Check metrics
make metrics

# Stop
make down
```

Run specific profiles:
```bash
make up profile=go      # All Go services (no DB)
make up profile=db      # Database only
make up profile=all     # Everything
```

## Environment

All config via environment variables (see `internal/config/config.go`):
- `POSTGRES_URL` – PostgreSQL connection string
- `REDIS_URL` – Redis address
- `PROXY_LOCAL_FILE` – Path to proxy list JSON
- `PROXY_PROVIDER_URL` – Optional proxy provider API

## Tech Stack

- Go 1.24+
- PostgreSQL 16+ (partitioned tables)
- Redis 7+

## Schema

- `matches` – partitioned by quarter (`start_time`)
- `player_matches` – per-player stats (hot data)
- `player_match_details` – JSONB blobs (cold data)
- Lookup tables: `heroes`, `items`, `leagues`, `teams`, `patches`

See `ARCHITECTURE.md` for full details.