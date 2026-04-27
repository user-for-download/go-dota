package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type HeroRef struct {
	ID            int16
	Name          string
	LocalizedName string
	PrimaryAttr   string
	AttackType    string
	Roles         []string
	Legs          *int16
}

func (r *Repository) UpsertHeroes(ctx context.Context, heroes []HeroRef) error {
	if len(heroes) == 0 {
		return nil
	}
	return r.WithTransaction(ctx, func(tx pgx.Tx) error {
		for _, h := range heroes {
			const q = `
				INSERT INTO heroes (id, name, localized_name, primary_attr, attack_type, roles, legs)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
				ON CONFLICT (id) DO UPDATE SET
					name           = COALESCE(NULLIF(EXCLUDED.name, ''),           heroes.name),
					localized_name = COALESCE(NULLIF(EXCLUDED.localized_name, ''), heroes.localized_name),
					primary_attr   = COALESCE(NULLIF(EXCLUDED.primary_attr, ''),   heroes.primary_attr),
					attack_type    = COALESCE(NULLIF(EXCLUDED.attack_type, ''),    heroes.attack_type),
					roles          = COALESCE(EXCLUDED.roles,                      heroes.roles),
					legs           = COALESCE(EXCLUDED.legs,                       heroes.legs)`
			if _, err := tx.Exec(ctx, q,
				h.ID, h.Name, h.LocalizedName, h.PrimaryAttr, h.AttackType, h.Roles, h.Legs,
			); err != nil {
				return fmt.Errorf("upsert hero %d: %w", h.ID, err)
			}
		}
		return nil
	})
}

type LeagueRef struct {
	ID     int32
	Name   string
	Ticket string
	Banner string
	Tier   string
}

func (r *Repository) UpsertLeagues(ctx context.Context, leagues []LeagueRef) error {
	if len(leagues) == 0 {
		return nil
	}
	placeholders := make([]string, len(leagues))
	args := make([]interface{}, 0, len(leagues)*5)
	for i, l := range leagues {
		base := i * 5
		placeholders[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d)", base+1, base+2, base+3, base+4, base+5)
		args = append(args, l.ID, l.Name, l.Ticket, l.Banner, l.Tier)
	}

	valuesClause := strings.Join(placeholders, ", ")
	q := `
		INSERT INTO leagues (leagueid, name, ticket, banner, tier)
		VALUES ` + valuesClause + `
		ON CONFLICT (leagueid) DO UPDATE SET
			name   = COALESCE(NULLIF(EXCLUDED.name, ''),   leagues.name),
			ticket = COALESCE(NULLIF(EXCLUDED.ticket, ''), leagues.ticket),
			banner = COALESCE(NULLIF(EXCLUDED.banner, ''), leagues.banner),
			tier   = COALESCE(NULLIF(EXCLUDED.tier, ''),   leagues.tier),
			updated_at = NOW()`

	_, err := r.pool.Exec(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("bulk upsert leagues: %w", err)
	}
	return nil
}