package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/user-for-download/go-dota/internal/models"
)

// replacePicksBansTx deletes existing picks/bans for the match and bulk-inserts
// the new set via COPY.
func replacePicksBansTx(ctx context.Context, tx pgx.Tx, matchID int64, pb []models.PicksBan) error {
	if _, err := tx.Exec(ctx,
		`DELETE FROM picks_bans WHERE match_id = $1`, matchID,
	); err != nil {
		return fmt.Errorf("delete picks_bans: %w", err)
	}
	if len(pb) == 0 {
		return nil
	}

	// Deduplicate by ord in case the upstream payload has duplicates (the PK
	// is (match_id, ord) and COPY would otherwise fail).
	seen := make(map[int16]struct{}, len(pb))
	rows := make([][]any, 0, len(pb))
	for _, p := range pb {
		if _, dup := seen[p.Order]; dup {
			continue
		}
		seen[p.Order] = struct{}{}
		rows = append(rows, []any{
			matchID, p.Order, p.IsPick, p.HeroID, p.Team,
		})
	}

	n, err := tx.CopyFrom(ctx,
		pgx.Identifier{"picks_bans"},
		[]string{"match_id", "ord", "is_pick", "hero_id", "team"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy picks_bans: %w", err)
	}
	if int(n) != len(rows) {
		return fmt.Errorf("copy picks_bans: wrote %d of %d rows", n, len(rows))
	}
	return nil
}

// replaceDraftTimingsTx deletes existing draft timings for the match and
// bulk-inserts the new set via COPY.
func replaceDraftTimingsTx(ctx context.Context, tx pgx.Tx, matchID int64, dt []models.DraftTiming) error {
	if _, err := tx.Exec(ctx,
		`DELETE FROM draft_timings WHERE match_id = $1`, matchID,
	); err != nil {
		return fmt.Errorf("delete draft_timings: %w", err)
	}
	if len(dt) == 0 {
		return nil
	}

	seen := make(map[int16]struct{}, len(dt))
	rows := make([][]any, 0, len(dt))
	for _, d := range dt {
		if _, dup := seen[d.Order]; dup {
			continue
		}
		seen[d.Order] = struct{}{}
		rows = append(rows, []any{
			matchID, d.Order, d.Pick, d.ActiveTeam, d.HeroID,
			d.PlayerSlot, d.ExtraTime, d.TotalTimeTaken,
		})
	}

	n, err := tx.CopyFrom(ctx,
		pgx.Identifier{"draft_timings"},
		[]string{
			"match_id", "ord", "pick", "active_team", "hero_id",
			"player_slot", "extra_time", "total_time_taken",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy draft_timings: %w", err)
	}
	if int(n) != len(rows) {
		return fmt.Errorf("copy draft_timings: wrote %d of %d rows", n, len(rows))
	}
	return nil
}
