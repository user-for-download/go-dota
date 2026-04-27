package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/user-for-download/go-dota/internal/models"
)

// IngestMatch inserts a match with full FK compliance.
// All nested data (players, draft, events, timeseries) is upserted.
// Uses pg_try_advisory_xact_lock to avoid blocking when another transaction
// is already processing the same match.
func (r *Repository) IngestMatch(ctx context.Context, m *models.Match) error {
	if err := m.Validate(); err != nil {
		return fmt.Errorf("validate match: %w", err)
	}

	return r.WithTransaction(ctx, func(tx pgx.Tx) error {
		var got bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, m.MatchID).Scan(&got); err != nil {
			return fmt.Errorf("advisory lock: %w", err)
		}
		if !got {
			return nil // another tx is handling this match
		}

		// FK stubs - teams, leagues, and heroes must exist before player_matches
		if err := upsertTeamStubTx(ctx, tx, m.RadiantTeamID); err != nil {
			return fmt.Errorf("team stub (radiant): %w", err)
		}
		if err := upsertTeamStubTx(ctx, tx, m.DireTeamID); err != nil {
			return fmt.Errorf("team stub (dire): %w", err)
		}
		if err := upsertLeagueStubTx(ctx, tx, m.LeagueID); err != nil {
			return fmt.Errorf("league stub: %w", err)
		}

		// Core match row
		if err := upsertMatchTx(ctx, tx, m); err != nil {
			return fmt.Errorf("matches: %w", err)
		}

		// Player stubs for tracking seen accounts and hero stubs before any player data
		if err := upsertPlayerStubsTx(ctx, tx, m); err != nil {
			return fmt.Errorf("player stubs: %w", err)
		}
		if err := upsertHeroStubsTx(ctx, tx, m); err != nil {
			return fmt.Errorf("hero stubs: %w", err)
		}

		// Player data (hot)
		if err := replacePlayerMatchesTx(ctx, tx, m); err != nil {
			return fmt.Errorf("player matches: %w", err)
		}

		// Player cold details (JSONB)
		if err := upsertPlayerMatchDetailsTx(ctx, tx, m); err != nil {
			return fmt.Errorf("player_match_details: %w", err)
		}

		// Draft data
		if err := replacePicksBansTx(ctx, tx, m.MatchID, m.PicksBans); err != nil {
			return fmt.Errorf("picks_bans: %w", err)
		}
		if err := replaceDraftTimingsTx(ctx, tx, m.MatchID, m.DraftTimings); err != nil {
			return fmt.Errorf("draft_timings: %w", err)
		}

		// Event data
		if err := replaceObjectivesTx(ctx, tx, m.MatchID, m.Objectives); err != nil {
			return fmt.Errorf("objectives: %w", err)
		}
		if err := replaceChatTx(ctx, tx, m.MatchID, m.Chat); err != nil {
			return fmt.Errorf("chat: %w", err)
		}
		if err := replaceTeamfightsTx(ctx, tx, m.MatchID, m.Teamfights); err != nil {
			return fmt.Errorf("teamfights: %w", err)
		}

		// Timeseries (only for parsed matches)
		if err := insertPlayerTimeseriesTx(ctx, tx, m); err != nil {
			return fmt.Errorf("player_timeseries: %w", err)
		}

		// Metadata - advantages and cosmetics
		if err := upsertMatchAdvantagesTx(ctx, tx, m); err != nil {
			return fmt.Errorf("advantages: %w", err)
		}
		if err := upsertMatchCosmeticsTx(ctx, tx, m); err != nil {
			return fmt.Errorf("cosmetics: %w", err)
		}

		// Team links in team_matches
		if m.RadiantTeamID != nil {
			if err := upsertTeamMatchTx(ctx, tx, m.MatchID, m.StartTime, m.RadiantTeamID, true, m.RadiantWin, m.LeagueID); err != nil {
				return fmt.Errorf("radiant_team_match: %w", err)
			}
		}
		if m.DireTeamID != nil {
			if err := upsertTeamMatchTx(ctx, tx, m.MatchID, m.StartTime, m.DireTeamID, false, !m.RadiantWin, m.LeagueID); err != nil {
				return fmt.Errorf("dire_team_match: %w", err)
			}
		}

		return nil
	})
}