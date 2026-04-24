package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/user-for-download/go-dota/internal/models"
)

// upsertPlayerStubsTx creates stub player rows for all account_ids in the match.
// Used to track which players have been seen, enabling future enrichment.
func upsertPlayerStubsTx(ctx context.Context, tx pgx.Tx, m *models.Match) error {
	if len(m.Players) == 0 {
		return nil
	}

	accounts := make([]int64, 0, len(m.Players))
	seen := make(map[int64]struct{}, len(m.Players))

	for i := range m.Players {
		if m.Players[i].AccountID == nil {
			continue
		}
		id := *m.Players[i].AccountID
		if id <= 0 {
			continue // anonymous account
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		accounts = append(accounts, id)
	}

	if len(accounts) == 0 {
		return nil
	}

	const q = `
		INSERT INTO players (account_id)
		SELECT unnest($1::bigint[])
		ON CONFLICT (account_id) DO NOTHING`
	_, err := tx.Exec(ctx, q, accounts)
	if err != nil {
		return fmt.Errorf("upsert player stubs: %w", err)
	}
	return nil
}

// upsertPlayerMatchDetailsTx upserts cold player detail columns (JSONB only).
// This table stores rarely-accessed data separately from player_matches (hot).
func upsertPlayerMatchDetailsTx(ctx context.Context, tx pgx.Tx, m *models.Match) error {
	if len(m.Players) == 0 {
		return nil
	}

	for _, p := range m.Players {
		const q = `
			INSERT INTO player_match_details (match_id, player_slot, damage, purchase_log)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (match_id, player_slot) DO UPDATE SET
				damage = COALESCE(EXCLUDED.damage, player_match_details.damage),
				purchase_log = COALESCE(EXCLUDED.purchase_log, player_match_details.purchase_log)`

		_, err := tx.Exec(ctx, q,
			m.MatchID, p.PlayerSlot,
			rawOrNil(p.Damage), rawOrNil(p.PurchaseLog),
		)
		if err != nil {
			return fmt.Errorf("player_match_details slot=%d: %w", p.PlayerSlot, err)
		}
	}
	return nil
}

// rawOrNil converts an empty RawMessage to nil so pgx writes SQL NULL.
func rawOrNil(r json.RawMessage) any {
	if len(r) == 0 || string(r) == "null" {
		return nil
	}
	return []byte(r)
}

// insertPlayerTimeseriesTx inserts per-minute timeseries for parsed matches.
func insertPlayerTimeseriesTx(ctx context.Context, tx pgx.Tx, m *models.Match) error {
	if !m.IsParsed() {
		return nil
	}

	if _, err := tx.Exec(ctx,
		`DELETE FROM player_timeseries WHERE match_id = $1`, m.MatchID,
	); err != nil {
		return fmt.Errorf("delete player_timeseries: %w", err)
	}

	log := slog.Default()
	var rows [][]any
	for i := range m.Players {
		p := &m.Players[i]
		series, err := models.BuildPlayerTimeseries(m.MatchID, m.PatchID, p)
		if err != nil {
			log.Warn("skipping timeseries for player",
				"match_id", m.MatchID, "slot", p.PlayerSlot, "error", err)
			continue
		}
		for _, s := range series {
			rows = append(rows, []any{
				s.MatchID, s.PlayerSlot, s.Minute, s.HeroID,
				s.AccountID, s.PatchID, s.Gold, s.XP, s.LH, s.DN,
			})
		}
	}
	if len(rows) == 0 {
		return nil
	}

	_, err := tx.CopyFrom(ctx,
		pgx.Identifier{"player_timeseries"},
		[]string{"match_id", "player_slot", "minute", "hero_id",
			"account_id", "patch_id", "gold", "xp", "lh", "dn"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy player_timeseries: %w", err)
	}
	return nil
}