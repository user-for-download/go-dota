package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository provides access to all persistence operations. It is split across
// multiple files by domain (matches, players, draft, events, teams, ingest).
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository wraps a Client's pool.
func NewRepository(client *Client) *Repository {
	return &Repository{pool: client.Pool()}
}

// NewRepositoryFromPool is a convenience constructor used in tests.
func NewRepositoryFromPool(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Pool exposes the underlying pgx pool (useful for ad-hoc queries / tests).
func (r *Repository) Pool() *pgxpool.Pool {
	return r.pool
}

// Ping verifies database connectivity.
func (r *Repository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// WithTransaction runs fn inside a pgx transaction. Rollback is automatic on
// error or panic; commit is automatic on success.
func (r *Repository) WithTransaction(ctx context.Context, fn func(pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, r.pool, fn)
}
