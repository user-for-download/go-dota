package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/user-for-download/go-dota/internal/models"
)

// replacePlayerMatchesTx deletes existing rows for the match and bulk-inserts
// fresh ones via COPY. The delete+copy pattern is required because pgx.CopyFrom
// does not support ON CONFLICT.
func replacePlayerMatchesTx(ctx context.Context, tx pgx.Tx, m *models.Match) error {
	if len(m.Players) == 0 {
		return nil
	}

	if _, err := tx.Exec(ctx,
		`DELETE FROM player_matches WHERE match_id = $1 AND start_time = $2`,
		m.MatchID, m.StartTime,
	); err != nil {
		return fmt.Errorf("delete player_matches: %w", err)
	}

	rows := make([][]any, 0, len(m.Players))
	for i := range m.Players {
		p := &m.Players[i]
		rows = append(rows, []any{
			m.MatchID,
			p.PlayerSlot,
			m.StartTime, // required for partition routing
			p.AccountID,
			p.HeroID,
			p.HeroVariant,
			p.IsRadiantSide(),
			p.Won(m.RadiantWin),
			m.Duration,
			m.PatchID,
			m.LobbyType,
			m.GameMode,
			p.Kills, p.Deaths, p.Assists,
			p.Level, p.NetWorth, p.Gold, p.GoldSpent,
			p.GoldPerMin, p.XPPerMin, p.LastHits, p.Denies,
			p.HeroDamage, p.TowerDamage, p.HeroHealing,
			p.Item0, p.Item1, p.Item2, p.Item3, p.Item4, p.Item5,
			p.ItemNeutral,
			p.Backpack0, p.Backpack1, p.Backpack2, p.Backpack3,
			p.FinalItems,
			p.Lane, p.LaneRole, p.IsRoaming, p.PartyID, p.PartySize,
			p.Stuns, p.ObsPlaced, p.SenPlaced,
			p.CreepsStacked, p.CampsStacked, p.RunePickups,
			intBoolPtr(p.FirstbloodClaimed),
			p.TeamfightParticipation,
			p.TowersKilled, p.RoshansKilled,
			p.ObserversPlaced, p.LeaverStatus,
			p.Times, p.GoldT, p.XPT, p.LHT, p.DNT,
			p.AbilityUpgradesArr,
		})
	}

	columns := []string{
		"match_id", "player_slot", "start_time", "account_id",
		"hero_id", "hero_variant", "is_radiant", "win", "duration",
		"patch_id", "lobby_type", "game_mode",
		"kills", "deaths", "assists",
		"level", "net_worth", "gold", "gold_spent",
		"gold_per_min", "xp_per_min", "last_hits", "denies",
		"hero_damage", "tower_damage", "hero_healing",
		"item_0", "item_1", "item_2", "item_3", "item_4", "item_5",
		"item_neutral",
		"backpack_0", "backpack_1", "backpack_2", "backpack_3",
		"final_items",
		"lane", "lane_role", "is_roaming", "party_id", "party_size",
		"stuns", "obs_placed", "sen_placed",
		"creeps_stacked", "camps_stacked", "rune_pickups",
		"firstblood_claimed", "teamfight_participation",
		"towers_killed", "roshans_killed",
		"observers_placed", "leaver_status",
		"times", "gold_t", "xp_t", "lh_t", "dn_t",
		"ability_upgrades_arr",
	}

	n, err := tx.CopyFrom(ctx,
		pgx.Identifier{"player_matches"},
		columns,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy player_matches: %w", err)
	}
	if int(n) != len(rows) {
		return fmt.Errorf("copy player_matches: wrote %d of %d rows", n, len(rows))
	}
	return nil
}

// replacePlayerDetailsTx stores per-player JSONB blobs (damage, logs, etc.).
// Uses a batched INSERT ... ON CONFLICT because the row count is fixed (10 per
// match) and idempotent upserts are required.
func replacePlayerDetailsTx(ctx context.Context, tx pgx.Tx, m *models.Match) error {
	if len(m.Players) == 0 {
		return nil
	}

	const q = `
		INSERT INTO player_match_details (
			match_id, player_slot,
			damage, damage_taken, damage_inflictor, damage_inflictor_received,
			damage_targets, hero_hits, max_hero_hit,
			ability_uses, ability_targets, item_uses,
			gold_reasons, xp_reasons,
			killed, killed_by, kill_streaks, multi_kills,
			life_state, lane_pos, obs, sen,
			actions, pings, runes, purchase,
			obs_log, sen_log, obs_left_log, sen_left_log,
			purchase_log, kills_log, buyback_log, runes_log, connection_log,
			permanent_buffs, neutral_tokens_log, neutral_item_history, additional_units
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
			$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
			$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
			$31,$32,$33,$34,$35,$36,$37,$38,$39
		)
		ON CONFLICT (match_id, player_slot) DO UPDATE SET
			damage                    = EXCLUDED.damage,
			damage_taken              = EXCLUDED.damage_taken,
			damage_inflictor          = EXCLUDED.damage_inflictor,
			damage_inflictor_received = EXCLUDED.damage_inflictor_received,
			damage_targets            = EXCLUDED.damage_targets,
			hero_hits                 = EXCLUDED.hero_hits,
			max_hero_hit              = EXCLUDED.max_hero_hit,
			ability_uses              = EXCLUDED.ability_uses,
			ability_targets           = EXCLUDED.ability_targets,
			item_uses                 = EXCLUDED.item_uses,
			gold_reasons              = EXCLUDED.gold_reasons,
			xp_reasons                = EXCLUDED.xp_reasons,
			killed                    = EXCLUDED.killed,
			killed_by                 = EXCLUDED.killed_by,
			kill_streaks              = EXCLUDED.kill_streaks,
			multi_kills               = EXCLUDED.multi_kills,
			life_state                = EXCLUDED.life_state,
			lane_pos                  = EXCLUDED.lane_pos,
			obs                       = EXCLUDED.obs,
			sen                       = EXCLUDED.sen,
			actions                   = EXCLUDED.actions,
			pings                     = EXCLUDED.pings,
			runes                     = EXCLUDED.runes,
			purchase                  = EXCLUDED.purchase,
			obs_log                   = EXCLUDED.obs_log,
			sen_log                   = EXCLUDED.sen_log,
			obs_left_log              = EXCLUDED.obs_left_log,
			sen_left_log              = EXCLUDED.sen_left_log,
			purchase_log              = EXCLUDED.purchase_log,
			kills_log                 = EXCLUDED.kills_log,
			buyback_log               = EXCLUDED.buyback_log,
			runes_log                 = EXCLUDED.runes_log,
			connection_log            = EXCLUDED.connection_log,
			permanent_buffs           = EXCLUDED.permanent_buffs,
			neutral_tokens_log        = EXCLUDED.neutral_tokens_log,
			neutral_item_history      = EXCLUDED.neutral_item_history,
			additional_units          = EXCLUDED.additional_units`

	batch := &pgx.Batch{}
	for i := range m.Players {
		p := &m.Players[i]
		batch.Queue(q,
			m.MatchID, p.PlayerSlot,
			rawOrNil(p.Damage), rawOrNil(p.DamageTaken),
			rawOrNil(p.DamageInflictor), rawOrNil(p.DamageInflictorReceived),
			rawOrNil(p.DamageTargets), rawOrNil(p.HeroHits), rawOrNil(p.MaxHeroHit),
			rawOrNil(p.AbilityUses), rawOrNil(p.AbilityTargets), rawOrNil(p.ItemUses),
			rawOrNil(p.GoldReasons), rawOrNil(p.XPReasons),
			rawOrNil(p.Killed), rawOrNil(p.KilledBy),
			rawOrNil(p.KillStreaks), rawOrNil(p.MultiKills),
			rawOrNil(p.LifeState), rawOrNil(p.LanePos),
			rawOrNil(p.Obs), rawOrNil(p.Sen),
			rawOrNil(p.Actions), rawOrNil(p.Pings), rawOrNil(p.Runes), rawOrNil(p.Purchase),
			rawOrNil(p.ObsLog), rawOrNil(p.SenLog),
			rawOrNil(p.ObsLeftLog), rawOrNil(p.SenLeftLog),
			rawOrNil(p.PurchaseLog), rawOrNil(p.KillsLog),
			rawOrNil(p.BuybackLog), rawOrNil(p.RunesLog), rawOrNil(p.ConnectionLog),
			rawOrNil(p.PermanentBuffs), rawOrNil(p.NeutralTokensLog),
			rawOrNil(p.NeutralItemHistory), rawOrNil(p.AdditionalUnits),
		)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < len(m.Players); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("upsert player_match_details[%d]: %w", i, err)
		}
	}
	return nil
}

// replacePlayerTimeseriesTx builds per-minute rows from each player's
// times/gold_t/xp_t arrays and bulk-inserts them via COPY.
//
// Skip policy: if BuildPlayerTimeseries returns an error (e.g., non-monotonic
// times array), that player's rows are skipped but the transaction continues
// with the other 9 players. This keeps a single corrupt series from failing
// the entire match ingest. Skips are logged for observability.
func replacePlayerTimeseriesTx(ctx context.Context, tx pgx.Tx, m *models.Match) error {
	if len(m.Players) == 0 {
		return nil
	}

	if _, err := tx.Exec(ctx,
		`DELETE FROM player_timeseries WHERE match_id = $1`,
		m.MatchID,
	); err != nil {
		return fmt.Errorf("delete player_timeseries: %w", err)
	}

	log := slog.Default()
	skipped := 0
	var all []models.PlayerTimeseries
	for i := range m.Players {
		p := &m.Players[i]
		rows, err := models.BuildPlayerTimeseries(m.MatchID, m.PatchID, p)
		if err != nil {
			skipped++
			log.Warn("skip player timeseries",
				"match_id", m.MatchID,
				"player_slot", p.PlayerSlot,
				"error", err)
			continue
		}
		all = append(all, rows...)
	}
	if skipped > 0 {
		log.Info("player timeseries skipped",
			"match_id", m.MatchID,
			"skipped", skipped,
			"total_players", len(m.Players))
	}
	if len(all) == 0 {
		return nil
	}

	data := make([][]any, 0, len(all))
	for _, row := range all {
		data = append(data, []any{
			row.MatchID, row.PlayerSlot, row.Minute, row.HeroID,
			row.AccountID, row.PatchID,
			row.Gold, row.XP, row.LH, row.DN,
		})
	}

	n, err := tx.CopyFrom(ctx,
		pgx.Identifier{"player_timeseries"},
		[]string{
			"match_id", "player_slot", "minute", "hero_id",
			"account_id", "patch_id",
			"gold", "xp", "lh", "dn",
		},
		pgx.CopyFromRows(data),
	)
	if err != nil {
		return fmt.Errorf("copy player_timeseries: %w", err)
	}
	if int(n) != len(data) {
		return fmt.Errorf("copy player_timeseries: wrote %d of %d rows", n, len(data))
	}
	return nil
}

// ---- Helpers --------------------------------------------------------------

// intBoolPtr converts *models.IntBool to *bool for pgx encoding.
func intBoolPtr(ib *models.IntBool) *bool {
	if ib == nil {
		return nil
	}
	b := bool(*ib)
	return &b
}

// rawOrNil returns nil for empty/null JSON so the column is stored as SQL NULL
// instead of the literal JSON `null`.
func rawOrNil(r json.RawMessage) any {
	if len(r) == 0 {
		return nil
	}
	if string(r) == "null" {
		return nil
	}
	return []byte(r)
}
