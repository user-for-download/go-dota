package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type PatchRef struct {
	ID           int16
	Name         string
	ReleaseDate  time.Time
	ReleaseEpoch int64
}

func (r *Repository) UpsertPatches(ctx context.Context, patches []PatchRef) error {
	if len(patches) == 0 {
		return nil
	}
	return r.WithTransaction(ctx, func(tx pgx.Tx) error {
		placeholders := make([]string, len(patches))
		args := make([]any, 0, len(patches)*4)
		for i, p := range patches {
			base := i * 4
			placeholders[i] = fmt.Sprintf("($%d,$%d,$%d,$%d)",
				base+1, base+2, base+3, base+4)
			args = append(args, p.ID, p.Name, p.ReleaseDate, p.ReleaseEpoch)
		}
		q := `
			INSERT INTO patches (id, name, release_date, release_epoch)
			VALUES ` + strings.Join(placeholders, ", ") + `
			ON CONFLICT (id) DO UPDATE SET
				name          = COALESCE(NULLIF(EXCLUDED.name, ''), patches.name),
				release_date  = EXCLUDED.release_date,
				release_epoch = EXCLUDED.release_epoch`
		if _, err := tx.Exec(ctx, q, args...); err != nil {
			return fmt.Errorf("bulk upsert patches: %w", err)
		}
		return nil
	})
}
