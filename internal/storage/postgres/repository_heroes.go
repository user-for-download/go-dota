package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/user-for-download/go-dota/internal/models"
)

// upsertHeroStubTx creates a stub hero row if it doesn't exist.
// Used to satisfy FK constraints before player_matches ingestion.
// Allows heroID = 0 for picks/bans compatibility.
func upsertHeroStubTx(ctx context.Context, tx pgx.Tx, heroID int16) error {
	if heroID < 0 {
		return nil
	}
	const q = `
		INSERT INTO heroes (id, name, localized_name) VALUES ($1, $2, $3)
		ON CONFLICT (id) DO NOTHING`
	_, err := tx.Exec(ctx, q, heroID, "stub_hero", "Stub Hero")
	return err
}

// upsertHeroStubsTx creates stub hero rows for all players in a match.
func upsertHeroStubsTx(ctx context.Context, tx pgx.Tx, m *models.Match) error {
	if len(m.Players) == 0 {
		return nil
	}

	seen := make(map[int16]struct{}, len(m.Players))
	for i := range m.Players {
		hid := m.Players[i].HeroID
		if _, ok := seen[hid]; ok {
			continue
		}
		seen[hid] = struct{}{}
		if err := upsertHeroStubTx(ctx, tx, hid); err != nil {
			return err
		}
	}
	return nil
}