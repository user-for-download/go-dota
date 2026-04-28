package postgres

import (
	"context"
	"fmt"
	"strings"
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
	const batchSize = 1000
	for i := 0; i < len(heroes); i += batchSize {
		end := i + batchSize
		if end > len(heroes) {
			end = len(heroes)
		}
		if err := r.upsertHeroesChunk(ctx, heroes[i:end]); err != nil {
			return fmt.Errorf("heroes chunk [%d:%d]: %w", i, end, err)
		}
	}
	return nil
}

func (r *Repository) upsertHeroesChunk(ctx context.Context, heroes []HeroRef) error {
	placeholders := make([]string, len(heroes))
	args := make([]interface{}, 0, len(heroes)*7)
	for i, h := range heroes {
		base := i * 7
		placeholders[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7)
		args = append(args, h.ID, h.Name, h.LocalizedName, h.PrimaryAttr, h.AttackType, h.Roles, h.Legs)
	}

	valuesClause := strings.Join(placeholders, ", ")
	q := `
		INSERT INTO heroes (id, name, localized_name, primary_attr, attack_type, roles, legs)
		VALUES ` + valuesClause + `
		ON CONFLICT (id) DO UPDATE SET
			name           = COALESCE(NULLIF(EXCLUDED.name, ''),           heroes.name),
			localized_name = COALESCE(NULLIF(EXCLUDED.localized_name, ''), heroes.localized_name),
			primary_attr   = COALESCE(NULLIF(EXCLUDED.primary_attr, ''),   heroes.primary_attr),
			attack_type    = COALESCE(NULLIF(EXCLUDED.attack_type, ''),    heroes.attack_type),
			roles          = COALESCE(EXCLUDED.roles,                      heroes.roles),
			legs           = COALESCE(EXCLUDED.legs,                       heroes.legs),
			updated_at     = NOW()`

	_, err := r.pool.Exec(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("bulk upsert heroes: %w", err)
	}
	return nil
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
	const batchSize = 1000
	for i := 0; i < len(leagues); i += batchSize {
		end := i + batchSize
		if end > len(leagues) {
			end = len(leagues)
		}
		if err := r.upsertLeaguesChunk(ctx, leagues[i:end]); err != nil {
			return fmt.Errorf("leagues chunk [%d:%d]: %w", i, end, err)
		}
	}
	return nil
}

func (r *Repository) upsertLeaguesChunk(ctx context.Context, leagues []LeagueRef) error {
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
