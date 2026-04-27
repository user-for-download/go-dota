package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// upsertLeagueStubTx creates a stub league row if it doesn't exist.
// Used to satisfy FK constraints before match ingestion.
// leagueid <= 0 is treated as "no league" and skipped.
func upsertLeagueStubTx(ctx context.Context, tx pgx.Tx, leagueID *int32) error {
	if leagueID == nil || *leagueID <= 0 {
		return nil
	}
	const q = `
		INSERT INTO leagues (leagueid) VALUES ($1)
		ON CONFLICT (leagueid) DO NOTHING`
	_, err := tx.Exec(ctx, q, *leagueID)
	return err
}
