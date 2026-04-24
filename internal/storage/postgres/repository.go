package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(client *Client) *Repository {
	return &Repository{pool: client.Pool()}
}

func NewRepositoryFromPool(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) EnsureSchema(ctx context.Context) error {
	schema := `
		CREATE TABLE IF NOT EXISTS parsed_data (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			external_id VARCHAR(255) UNIQUE NOT NULL,
			payload JSONB NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);`
	if _, err := r.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	return nil
}

func (r *Repository) UpsertParsedData(ctx context.Context, externalID string, payload json.RawMessage) error {
	const query = `
		INSERT INTO parsed_data (external_id, payload)
		VALUES ($1, $2)
		ON CONFLICT (external_id) DO UPDATE SET payload = EXCLUDED.payload`
	_, err := r.pool.Exec(ctx, query, externalID, payload)
	if err != nil {
		return fmt.Errorf("upsert parsed_data: %w", err)
	}
	return nil
}

func (r *Repository) CountUniqueExternalIDs(ctx context.Context) (int64, error) {
	var count int64
	err := r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM parsed_data").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count external_ids: %w", err)
	}
	return count, nil
}

func (r *Repository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r *Repository) FilterNewIDs(ctx context.Context, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := r.pool.Query(ctx, "SELECT external_id FROM parsed_data WHERE external_id = ANY($1)", ids)
	if err != nil {
		return nil, fmt.Errorf("filter ids: %w", err)
	}
	defer rows.Close()

	existing := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan id: %w", err)
		}
		existing[id] = true
	}

	newIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		if !existing[id] {
			newIDs = append(newIDs, id)
		}
	}
	return newIDs, nil
}
