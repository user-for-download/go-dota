# OpenDota Pipeline

Dota 2 match data ingestion pipeline. Fetches, collects, parses, and stores match data from the OpenDota API into PostgreSQL.

## Services

| Service | Description |
|---------|-------------|
| fetcher | Discovers new match IDs from OpenDota explorer API |
| collector | Downloads match JSON via rotating proxy pool |
| parser | Ingests matches into PostgreSQL (partitioned) |
| enricher | Syncs lookup tables (heroes, items, patches, leagues, teams) |
| proxy-manager | Manages Redis proxy pool |
| partition-manager | Creates/drops quarterly partitions |
| monitor | Health & metrics HTTP endpoint |

## Quick Start

```bash
# Run a service
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