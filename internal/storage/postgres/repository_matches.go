package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/user-for-download/go-dota/internal/models"
)

// upsertMatchTx inserts or updates the core matches row. start_time is required
// in the INSERT so PostgreSQL can route to the correct partition.
func upsertMatchTx(ctx context.Context, tx pgx.Tx, m *models.Match) error {
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
		m.LeagueID, m.SeriesID, m.SeriesType,
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
