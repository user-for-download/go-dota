package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type AbilityRef struct {
	Key         string
	ID          *int
	DName       string
	Behavior    json.RawMessage
	TargetTeam  string
	Description string
	Img         string
	ManaCost    string
	Cooldown    string
	Attrib      json.RawMessage
	IsTalent    bool
}

func (r *Repository) UpsertAbilities(ctx context.Context, abilities []AbilityRef) error {
	if len(abilities) == 0 {
		return nil
	}
	const batchSize = 500
	for i := 0; i < len(abilities); i += batchSize {
		end := i + batchSize
		if end > len(abilities) {
			end = len(abilities)
		}
		if err := r.upsertAbilitiesChunk(ctx, abilities[i:end]); err != nil {
			return fmt.Errorf("abilities chunk [%d:%d]: %w", i, end, err)
		}
	}
	return nil
}

func (r *Repository) upsertAbilitiesChunk(ctx context.Context, abilities []AbilityRef) error {
	return r.WithTransaction(ctx, func(tx pgx.Tx) error {
		const cols = 11
		placeholders := make([]string, len(abilities))
		args := make([]any, 0, len(abilities)*cols)

		for i, a := range abilities {
			base := i * cols
			placeholders[i] = fmt.Sprintf(
				"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
				base+1, base+2, base+3, base+4, base+5,
				base+6, base+7, base+8, base+9, base+10, base+11,
			)
			args = append(args,
				a.Key, a.ID, a.DName,
				rawOrNil(a.Behavior), a.TargetTeam,
				a.Description, a.Img, a.ManaCost, a.Cooldown,
				rawOrNil(a.Attrib), a.IsTalent,
			)
		}

		q := `
			INSERT INTO abilities (
				key, id, dname, behavior, target_team,
				description, img, mana_cost, cooldown, attrib, is_talent
			) VALUES ` + strings.Join(placeholders, ", ") + `
			ON CONFLICT (key) DO UPDATE SET
				id          = COALESCE(EXCLUDED.id,                          abilities.id),
				dname       = COALESCE(NULLIF(EXCLUDED.dname, ''),           abilities.dname),
				behavior    = COALESCE(EXCLUDED.behavior,                    abilities.behavior),
				target_team = COALESCE(NULLIF(EXCLUDED.target_team, ''),     abilities.target_team),
				description = COALESCE(NULLIF(EXCLUDED.description, ''),     abilities.description),
				img         = COALESCE(NULLIF(EXCLUDED.img, ''),             abilities.img),
				mana_cost   = COALESCE(NULLIF(EXCLUDED.mana_cost, ''),       abilities.mana_cost),
				cooldown    = COALESCE(NULLIF(EXCLUDED.cooldown, ''),        abilities.cooldown),
				attrib      = COALESCE(EXCLUDED.attrib,                      abilities.attrib),
				is_talent   = EXCLUDED.is_talent,
				updated_at  = NOW()`

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			return fmt.Errorf("bulk upsert abilities: %w", err)
		}
		return nil
	})
}