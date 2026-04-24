package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/user-for-download/go-dota/internal/models"
)

// IngestMatch atomically inserts a full match and all its sub-resources.
//
// Ordering:
//  1. advisory lock on match_id (serializes concurrent ingests of the same match)
//  2. matches (parent row; must exist for partition routing)
//  3. player_matches + player_match_details + player_timeseries
//  4. draft: picks_bans + draft_timings
//  5. events: objectives + chat + teamfights
//  6. metadata: advantages + cosmetics
//  7. team links: radiant + dire
//
// All steps run in a single pgx transaction; any failure rolls back the whole match.
func (r *Repository) IngestMatch(ctx context.Context, m *models.Match) error {
	if err := m.Validate(); err != nil {
		return fmt.Errorf("validate match: %w", err)
	}

	return r.WithTransaction(ctx, func(tx pgx.Tx) error {
		// 1. Serialize concurrent writers for the same match.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, m.MatchID); err != nil {
			return fmt.Errorf("advisory lock: %w", err)
		}

		// 2. Core match row.
		if err := upsertMatchTx(ctx, tx, m); err != nil {
			return fmt.Errorf("matches: %w", err)
		}

		// 3. Players + details + per-minute series.
		if len(m.Players) > 0 {
			if err := replacePlayerMatchesTx(ctx, tx, m); err != nil {
				return fmt.Errorf("player_matches: %w", err)
			}
			if err := replacePlayerDetailsTx(ctx, tx, m); err != nil {
				return fmt.Errorf("player_match_details: %w", err)
			}
			if err := replacePlayerTimeseriesTx(ctx, tx, m); err != nil {
				return fmt.Errorf("player_timeseries: %w", err)
			}
		}

		// 4. Draft.
		if len(m.PicksBans) > 0 {
			if err := replacePicksBansTx(ctx, tx, m.MatchID, m.PicksBans); err != nil {
				return fmt.Errorf("picks_bans: %w", err)
			}
		}
		if len(m.DraftTimings) > 0 {
			if err := replaceDraftTimingsTx(ctx, tx, m.MatchID, m.DraftTimings); err != nil {
				return fmt.Errorf("draft_timings: %w", err)
			}
		}

		// 5. Events.
		if len(m.Objectives) > 0 {
			if err := replaceObjectivesTx(ctx, tx, m.MatchID, m.Objectives); err != nil {
				return fmt.Errorf("objectives: %w", err)
			}
		}
		if len(m.Chat) > 0 {
			if err := replaceChatTx(ctx, tx, m.MatchID, m.Chat); err != nil {
				return fmt.Errorf("chat: %w", err)
			}
		}
		if len(m.Teamfights) > 0 {
			if err := replaceTeamfightsTx(ctx, tx, m.MatchID, m.Teamfights); err != nil {
				return fmt.Errorf("teamfights: %w", err)
			}
		}

		// 6. Metadata.
		if err := upsertMatchAdvantagesTx(ctx, tx, m); err != nil {
			return fmt.Errorf("advantages: %w", err)
		}
		if err := upsertMatchCosmeticsTx(ctx, tx, m); err != nil {
			return fmt.Errorf("cosmetics: %w", err)
		}

		// 7. Team links.
		if m.RadiantTeamID != nil {
			if err := upsertTeamMatchTx(
				ctx, tx,
				m.MatchID, m.StartTime,
				m.RadiantTeamID, true, m.RadiantWin, m.LeagueID,
			); err != nil {
				return fmt.Errorf("radiant_team_match: %w", err)
			}
		}
		if m.DireTeamID != nil {
			if err := upsertTeamMatchTx(
				ctx, tx,
				m.MatchID, m.StartTime,
				m.DireTeamID, false, !m.RadiantWin, m.LeagueID,
			); err != nil {
				return fmt.Errorf("dire_team_match: %w", err)
			}
		}

		return nil
	})
}
