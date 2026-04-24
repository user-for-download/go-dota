package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// upsertTeamStubTx creates a stub team row if it doesn't exist.
// Used to satisfy FK constraints before match ingestion.
func upsertTeamStubTx(ctx context.Context, tx pgx.Tx, teamID *int64) error {
	if teamID == nil {
		return nil
	}
	const q = `
		INSERT INTO teams (team_id) VALUES ($1)
		ON CONFLICT (team_id) DO NOTHING`
	_, err := tx.Exec(ctx, q, *teamID)
	return err
}

// UpsertTeam creates or updates a team row. Called outside match ingestion
// (e.g., when bulk-syncing teams from the /teams endpoint).
func (r *Repository) UpsertTeam(ctx context.Context, teamID int64, name, tag, logoURL string) error {
	const q = `
		INSERT INTO teams (team_id, name, tag, logo_url)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (team_id) DO UPDATE SET
			name       = COALESCE(NULLIF(EXCLUDED.name, ''),     teams.name),
			tag        = COALESCE(NULLIF(EXCLUDED.tag,  ''),     teams.tag),
			logo_url   = COALESCE(NULLIF(EXCLUDED.logo_url, ''), teams.logo_url),
			updated_at = NOW()`
	if _, err := r.pool.Exec(ctx, q, teamID, name, tag, logoURL); err != nil {
		return fmt.Errorf("upsert team: %w", err)
	}
	return nil
}

// UpsertTeamRating replaces the current rating snapshot for a team.
func (r *Repository) UpsertTeamRating(
	ctx context.Context,
	teamID int64,
	rating float32,
	wins, losses int,
	lastMatchTime int64,
	lastMatchID int64,
) error {
	const q = `
		INSERT INTO team_rating (team_id, rating, wins, losses, last_match_time, last_match_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (team_id) DO UPDATE SET
			rating          = EXCLUDED.rating,
			wins            = EXCLUDED.wins,
			losses          = EXCLUDED.losses,
			last_match_time = EXCLUDED.last_match_time,
			last_match_id   = EXCLUDED.last_match_id,
			updated_at      = NOW()`
	_, err := r.pool.Exec(ctx, q, teamID, rating, wins, losses, lastMatchTime, lastMatchID)
	if err != nil {
		return fmt.Errorf("upsert team rating: %w", err)
	}
	return nil
}

// upsertTeamMatchTx links a team to a match within an existing transaction.
// Idempotent on re-ingest: updates is_radiant / win / leagueid if the row exists.
func upsertTeamMatchTx(
	ctx context.Context,
	tx pgx.Tx,
	matchID, startTime int64,
	teamID *int64,
	radiant, win bool,
	leagueID *int32,
) error {
	if teamID == nil {
		return nil
	}
	const q = `
		INSERT INTO team_matches (team_id, match_id, is_radiant, win, start_time, leagueid)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (team_id, match_id) DO UPDATE SET
			is_radiant = EXCLUDED.is_radiant,
			win        = EXCLUDED.win,
			leagueid   = COALESCE(EXCLUDED.leagueid, team_matches.leagueid)`
	if _, err := tx.Exec(ctx, q, *teamID, matchID, radiant, win, startTime, leagueID); err != nil {
		return fmt.Errorf("upsert team_match: %w", err)
	}
	return nil
}
