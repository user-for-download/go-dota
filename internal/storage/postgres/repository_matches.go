package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/user-for-download/go-dota/internal/models"
)

// intBoolPtr converts models.IntBool to *bool for pgx compatibility.
func intBoolPtr(ib *models.IntBool) *bool {
	if ib == nil {
		return nil
	}
	b := bool(*ib)
	return &b
}

// upsertMatchTx inserts or updates the core matches row. start_time is required
// in the INSERT so PostgreSQL can route to the correct partition.
func upsertMatchTx(ctx context.Context, tx pgx.Tx, m *models.Match) error {
	leagueID := m.LeagueID
	if leagueID != nil && *leagueID <= 0 {
		leagueID = nil
	}
	const q = `
		INSERT INTO matches (
			match_id, match_seq_num, start_time, duration, radiant_win,
			tower_status_radiant, tower_status_dire,
			barracks_status_radiant, barracks_status_dire,
			radiant_score, dire_score, first_blood_time,
			lobby_type, game_mode, cluster, engine, human_players,
			version, patch_id, positive_votes, negative_votes,
			leagueid, series_id, series_type,
			radiant_team_id, dire_team_id,
			radiant_captain, dire_captain,
			replay_salt, replay_url, is_parsed
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
			$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
			$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31
		)
		ON CONFLICT (match_id, start_time) DO UPDATE SET
			duration                = EXCLUDED.duration,
			radiant_win             = EXCLUDED.radiant_win,
			tower_status_radiant    = COALESCE(EXCLUDED.tower_status_radiant,    matches.tower_status_radiant),
			tower_status_dire       = COALESCE(EXCLUDED.tower_status_dire,       matches.tower_status_dire),
			barracks_status_radiant = COALESCE(EXCLUDED.barracks_status_radiant, matches.barracks_status_radiant),
			barracks_status_dire    = COALESCE(EXCLUDED.barracks_status_dire,    matches.barracks_status_dire),
			radiant_score           = COALESCE(EXCLUDED.radiant_score,           matches.radiant_score),
			dire_score              = COALESCE(EXCLUDED.dire_score,              matches.dire_score),
			first_blood_time        = COALESCE(EXCLUDED.first_blood_time,        matches.first_blood_time),
			version                 = COALESCE(EXCLUDED.version,                 matches.version),
			patch_id                = COALESCE(EXCLUDED.patch_id,                matches.patch_id),
			positive_votes          = COALESCE(EXCLUDED.positive_votes,          matches.positive_votes),
			negative_votes          = COALESCE(EXCLUDED.negative_votes,          matches.negative_votes),
			replay_salt             = COALESCE(EXCLUDED.replay_salt,             matches.replay_salt),
			replay_url              = COALESCE(EXCLUDED.replay_url,              matches.replay_url),
			is_parsed               = EXCLUDED.is_parsed OR matches.is_parsed,
			updated_at              = NOW()`
	_, err := tx.Exec(ctx, q,
		m.MatchID, m.MatchSeqNum, m.StartTime, m.Duration, m.RadiantWin,
		m.TowerStatusRadiant, m.TowerStatusDire,
		m.BarracksStatusRadiant, m.BarracksStatusDire,
		m.RadiantScore, m.DireScore, m.FirstBloodTime,
		m.LobbyType, m.GameMode, m.Cluster, m.Engine, m.HumanPlayers,
		m.Version, m.PatchID, m.PositiveVotes, m.NegativeVotes,
		leagueID, m.SeriesID, m.SeriesType,
		m.RadiantTeamID, m.DireTeamID,
		m.RadiantCaptain, m.DireCaptain,
		m.ReplaySalt, m.ReplayURL, m.IsParsed(),
	)
	if err != nil {
		return fmt.Errorf("upsert matches: %w", err)
	}
	return nil
}

// upsertMatchAdvantagesTx stores radiant gold/xp advantage arrays.
func upsertMatchAdvantagesTx(ctx context.Context, tx pgx.Tx, m *models.Match) error {
	if len(m.RadiantGoldAdv) == 0 && len(m.RadiantXPAdv) == 0 {
		return nil
	}
	const q = `
		INSERT INTO match_advantages (match_id, radiant_gold_adv, radiant_xp_adv)
		VALUES ($1, $2, $3)
		ON CONFLICT (match_id) DO UPDATE SET
			radiant_gold_adv = EXCLUDED.radiant_gold_adv,
			radiant_xp_adv   = EXCLUDED.radiant_xp_adv`
	if _, err := tx.Exec(ctx, q, m.MatchID, m.RadiantGoldAdv, m.RadiantXPAdv); err != nil {
		return fmt.Errorf("upsert match_advantages: %w", err)
	}
	return nil
}

// upsertMatchCosmeticsTx stores the raw cosmetics JSONB blob.
func upsertMatchCosmeticsTx(ctx context.Context, tx pgx.Tx, m *models.Match) error {
	if len(m.Cosmetics) == 0 || string(m.Cosmetics) == "null" {
		return nil
	}
	var probe any
	if err := json.Unmarshal(m.Cosmetics, &probe); err != nil {
		return fmt.Errorf("cosmetics not valid JSON: %w", err)
	}
	const q = `
		INSERT INTO match_cosmetics (match_id, cosmetics)
		VALUES ($1, $2)
		ON CONFLICT (match_id) DO UPDATE SET cosmetics = EXCLUDED.cosmetics`
	if _, err := tx.Exec(ctx, q, m.MatchID, []byte(m.Cosmetics)); err != nil {
		return fmt.Errorf("upsert match_cosmetics: %w", err)
	}
	return nil
}

// replacePlayerMatchesTx inserts all player rows for a match using a single
// multi-VALUES INSERT for efficiency. Uses ON CONFLICT DO UPDATE for idempotency.
// This version writes ALL columns to populate the hot path for MVs.
func replacePlayerMatchesTx(ctx context.Context, tx pgx.Tx, m *models.Match) error {
	if len(m.Players) == 0 {
		return nil
	}

	// Columns to write - note: not all MatchPlayer fields map to DB columns
	// See schema in 001_init.sql player_matches table
	constCols := []string{
		"match_id", "player_slot", "start_time", "account_id",
		"hero_id", "hero_variant", "is_radiant", "win", "duration",
		"patch_id", "lobby_type", "game_mode",
		"kills", "deaths", "assists", "level",
		"net_worth", "gold", "gold_spent", "gold_per_min", "xp_per_min",
		"last_hits", "denies", "hero_damage", "tower_damage", "hero_healing",
		"item_0", "item_1", "item_2", "item_3", "item_4", "item_5",
		"item_neutral", "backpack_0", "backpack_1", "backpack_2", "backpack_3",
		"lane", "lane_role", "is_roaming", "party_id", "party_size",
		"stuns", "obs_placed", "sen_placed", "creeps_stacked", "camps_stacked",
		"rune_pickups", "firstblood_claimed", "teamfight_participation",
		"towers_killed", "roshans_killed", "observers_placed", "leaver_status",
	}

	placeholders := make([]string, len(m.Players))
	args := make([]interface{}, 0, len(m.Players)*len(constCols))

	for i, p := range m.Players {
		isRadiant := p.PlayerSlot < 128
		win := m.RadiantWin == isRadiant
		base := i * len(constCols)

		// Build placeholders for this row
		row := make([]string, len(constCols))
		for j := range constCols {
			row[j] = fmt.Sprintf("$%d", base+j+1)
		}
		placeholders[i] = fmt.Sprintf("(%s)", strings.Join(row, ","))

		// Build args for this row - order must match constCols
		args = append(args,
			m.MatchID, p.PlayerSlot, m.StartTime, p.AccountID,
			p.HeroID, p.HeroVariant, isRadiant, win, m.Duration,
			m.PatchID, m.LobbyType, m.GameMode,
			p.Kills, p.Deaths, p.Assists, p.Level,
			p.NetWorth, p.Gold, p.GoldSpent, p.GoldPerMin, p.XPPerMin,
			p.LastHits, p.Denies, p.HeroDamage, p.TowerDamage, p.HeroHealing,
			p.Item0, p.Item1, p.Item2, p.Item3, p.Item4, p.Item5,
			p.ItemNeutral, p.Backpack0, p.Backpack1, p.Backpack2, p.Backpack3,
			p.Lane, p.LaneRole, p.IsRoaming, p.PartyID, p.PartySize,
			p.Stuns, p.ObsPlaced, p.SenPlaced, p.CreepsStacked, p.CampsStacked,
			p.RunePickups, intBoolPtr(p.FirstbloodClaimed), p.TeamfightParticipation,
			p.TowersKilled, p.RoshansKilled, p.ObserversPlaced, p.LeaverStatus,
		)
	}

	valuesClause := strings.Join(placeholders, ", ")
	cols := strings.Join(constCols, ", ")

	// Build the SET clause for ON CONFLICT - exclude PK columns from update
	setClauses := make([]string, 0, len(constCols)-3) // exclude match_id, player_slot, start_time
	for _, c := range constCols[3:] {                 // skip PK columns
		setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", c, c))
	}
	setClause := strings.Join(setClauses, ", ")

	q := fmt.Sprintf(`
		INSERT INTO player_matches (%s)
		VALUES %s
		ON CONFLICT (match_id, player_slot, start_time) DO UPDATE SET
			%s`,
		cols, valuesClause, setClause,
	)

	_, err := tx.Exec(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("player_matches: %w", err)
	}
	return nil
}

// MatchExists reports whether a match is already stored (in any partition).
func (r *Repository) MatchExists(ctx context.Context, matchID, startTime int64) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM matches WHERE match_id = $1 AND start_time = $2)`,
		matchID, startTime,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("match exists: %w", err)
	}
	return exists, nil
}

// FilterUnknownMatchIDs returns the subset of matchIDs NOT present in matches.
func (r *Repository) FilterUnknownMatchIDs(ctx context.Context, ids []int64) ([]int64, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := r.pool.Query(ctx,
		`SELECT match_id FROM matches WHERE match_id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("filter match ids: %w", err)
	}
	defer rows.Close()

	seen := make(map[int64]struct{}, len(ids))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		seen[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]int64, 0, len(ids)-len(seen))
	for _, id := range ids {
		if _, ok := seen[id]; !ok {
			out = append(out, id)
		}
	}
	return out, nil
}
