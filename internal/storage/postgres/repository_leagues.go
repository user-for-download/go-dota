package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// upsertLeagueStubTx creates a stub league row if it doesn't exist.
// Used to satisfy FK constraints before match ingestion.
// Takes an advisory lock to prevent deadlocks between concurrent transactions.
// leagueid <= 0 is treated as "no league" and skipped.
func upsertLeagueStubTx(ctx context.Context, tx pgx.Tx, leagueID *int32) error {
	if leagueID == nil || *leagueID <= 0 {
		return nil
	}
	// Namespace 3 for league locks.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(3, $1)`, *leagueID); err != nil {
		return fmt.Errorf("advisory lock league %d: %w", *leagueID, err)
	}
	const q = `
		INSERT INTO leagues (leagueid) VALUES ($1)
		ON CONFLICT (leagueid) DO NOTHING`
	_, err := tx.Exec(ctx, q, *leagueID)
	return err
}
