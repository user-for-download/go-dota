package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

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

	cols := []string{
		"match_id", "player_slot",
		"damage", "damage_taken", "damage_inflictor", "damage_inflictor_received", "damage_targets",
		"hero_hits", "max_hero_hit", "ability_uses", "ability_targets", "item_uses",
		"gold_reasons", "xp_reasons",
		"killed", "killed_by", "kill_streaks", "multi_kills", "life_state",
		"lane_pos", "obs", "sen", "actions", "pings", "runes", "purchase",
		"obs_log", "sen_log", "obs_left_log", "sen_left_log",
		"purchase_log", "kills_log", "buyback_log", "runes_log", "connection_log",
		"permanent_buffs", "neutral_tokens_log", "neutral_item_history", "additional_units",
	}

	for _, p := range m.Players {
		args := []interface{}{
			m.MatchID, p.PlayerSlot,
			rawOrNil(p.Damage), rawOrNil(p.DamageTaken), rawOrNil(p.DamageInflictor), rawOrNil(p.DamageInflictorReceived), rawOrNil(p.DamageTargets),
			rawOrNil(p.HeroHits), rawOrNil(p.MaxHeroHit), rawOrNil(p.AbilityUses), rawOrNil(p.AbilityTargets), rawOrNil(p.ItemUses),
			rawOrNil(p.GoldReasons), rawOrNil(p.XPReasons),
			rawOrNil(p.Killed), rawOrNil(p.KilledBy), rawOrNil(p.KillStreaks), rawOrNil(p.MultiKills), rawOrNil(p.LifeState),
			rawOrNil(p.LanePos), rawOrNil(p.Obs), rawOrNil(p.Sen), rawOrNil(p.Actions), rawOrNil(p.Pings), rawOrNil(p.Runes), rawOrNil(p.Purchase),
			rawOrNil(p.ObsLog), rawOrNil(p.SenLog), rawOrNil(p.ObsLeftLog), rawOrNil(p.SenLeftLog),
			rawOrNil(p.PurchaseLog), rawOrNil(p.KillsLog), rawOrNil(p.BuybackLog), rawOrNil(p.RunesLog), rawOrNil(p.ConnectionLog),
			rawOrNil(p.PermanentBuffs), rawOrNil(p.NeutralTokensLog), rawOrNil(p.NeutralItemHistory), rawOrNil(p.AdditionalUnits),
		}

		setClauses := make([]string, 0, len(cols)-2)
		for _, c := range cols[2:] {
			setClauses = append(setClauses, fmt.Sprintf("%s = COALESCE(EXCLUDED.%s, player_match_details.%s)", c, c, c))
		}

		q := fmt.Sprintf(`
			INSERT INTO player_match_details (%s)
			VALUES (%s)
			ON CONFLICT (match_id, player_slot) DO UPDATE SET
				%s`,
			strings.Join(cols, ", "),
			strings.Join(makePlaceholders(len(cols)), ", "),
			strings.Join(setClauses, ", "),
		)

		_, err := tx.Exec(ctx, q, args...)
		if err != nil {
			return fmt.Errorf("player_match_details slot=%d: %w", p.PlayerSlot, err)
		}
	}
	return nil
}

func makePlaceholders(n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("$%d", i+1)
	}
	return out
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
