package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LegacyRepository manages the legacy `parsed_data` table, which stores raw
// JSON payloads keyed by external ID. It lives on a separate PostgreSQL
// instance from the normalized match schema to keep cold data physically
// isolated from hot OLTP traffic.
type LegacyRepository struct {
	pool *pgxpool.Pool
}

// NewLegacyRepository wraps a Client's pool.
func NewLegacyRepository(client *Client) *LegacyRepository {
	return &LegacyRepository{pool: client.Pool()}
}

// NewLegacyRepositoryFromPool is a convenience constructor used in tests.
func NewLegacyRepositoryFromPool(pool *pgxpool.Pool) *LegacyRepository {
	return &LegacyRepository{pool: pool}
}

// Pool exposes the underlying pgx pool.
func (r *LegacyRepository) Pool() *pgxpool.Pool {
	return r.pool
}

// Ping verifies database connectivity.
func (r *LegacyRepository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// EnsureSchema creates the parsed_data table and its index if missing.
func (r *LegacyRepository) EnsureSchema(ctx context.Context) error {
	const schema = `
		CREATE TABLE IF NOT EXISTS parsed_data (
			id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			external_id VARCHAR(255) UNIQUE NOT NULL,
			payload     JSONB NOT NULL,
			created_at  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_parsed_data_external_id
			ON parsed_data(external_id);
	`
	if _, err := r.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("ensure legacy schema: %w", err)
	}
	return nil
}

// UpsertParsedData stores a raw JSON blob keyed by external_id.
func (r *LegacyRepository) UpsertParsedData(ctx context.Context, externalID string, payload json.RawMessage) error {
	const q = `
		INSERT INTO parsed_data (external_id, payload)
		VALUES ($1, $2)
		ON CONFLICT (external_id) DO UPDATE SET payload = EXCLUDED.payload`
	if _, err := r.pool.Exec(ctx, q, externalID, payload); err != nil {
		return fmt.Errorf("upsert parsed_data: %w", err)
	}
	return nil
}

// CountUniqueExternalIDs returns the number of rows in parsed_data.
func (r *LegacyRepository) CountUniqueExternalIDs(ctx context.Context) (int64, error) {
	var count int64
	if err := r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM parsed_data").Scan(&count); err != nil {
		return 0, fmt.Errorf("count external_ids: %w", err)
	}
	return count, nil
}

// FilterNewIDs returns the subset of ids that are NOT yet present in parsed_data.
func (r *LegacyRepository) FilterNewIDs(ctx context.Context, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := r.pool.Query(ctx,
		"SELECT external_id FROM parsed_data WHERE external_id = ANY($1)", ids)
	if err != nil {
		return nil, fmt.Errorf("filter ids: %w", err)
	}
	defer rows.Close()

	existing := make(map[string]struct{}, len(ids))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan id: %w", err)
		}
		existing[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iter: %w", err)
	}

	newIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := existing[id]; !ok {
			newIDs = append(newIDs, id)
		}
	}
	return newIDs, nil
}
