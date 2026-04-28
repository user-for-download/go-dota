package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type ItemRef struct {
	ID            int
	Name          string
	LocalizedName string
	Cost          int
	SecretShop    bool
	SideShop      bool
	Recipe        bool
	Image         string
}

type GameModeRef struct {
	ID   int16
	Name string
}

type LobbyTypeRef struct {
	ID   int16
	Name string
}

func (r *Repository) UpsertItems(ctx context.Context, items []ItemRef) error {
	if len(items) == 0 {
		return nil
	}
	const batchSize = 1000
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		if err := r.upsertItemsChunk(ctx, items[i:end]); err != nil {
			return fmt.Errorf("items chunk [%d:%d]: %w", i, end, err)
		}
	}
	return nil
}

func (r *Repository) upsertItemsChunk(ctx context.Context, items []ItemRef) error {
	return r.WithTransaction(ctx, func(tx pgx.Tx) error {
		placeholders := make([]string, len(items))
		args := make([]interface{}, 0, len(items)*9)
		for i, it := range items {
			base := i * 9
			placeholders[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
				base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9)
			args = append(args, it.ID, it.Name, it.LocalizedName, it.Cost, it.SecretShop, it.SideShop, it.Recipe, it.Image, nil)
		}
		valuesClause := strings.Join(placeholders, ", ")
		q := `
			INSERT INTO items (id, name, localized_name, cost, secret_shop, side_shop, recipe, img, updated_at)
			VALUES ` + valuesClause + `
			ON CONFLICT (id) DO UPDATE SET
				name           = COALESCE(NULLIF(EXCLUDED.name, ''),       items.name),
				localized_name = COALESCE(NULLIF(EXCLUDED.localized_name, ''), items.localized_name),
				cost         = COALESCE(EXCLUDED.cost,         items.cost),
				secret_shop  = COALESCE(EXCLUDED.secret_shop,  items.secret_shop),
				side_shop   = COALESCE(EXCLUDED.side_shop,   items.side_shop),
				recipe     = COALESCE(EXCLUDED.recipe,     items.recipe),
				img        = COALESCE(NULLIF(EXCLUDED.img, ''),   items.img),
				updated_at  = NOW()`

		_, err := tx.Exec(ctx, q, args...)
		if err != nil {
			return fmt.Errorf("bulk upsert items: %w", err)
		}
		return nil
	})
}

func (r *Repository) UpsertGameModes(ctx context.Context, modes []GameModeRef) error {
	if len(modes) == 0 {
		return nil
	}
	const batchSize = 1000
	for i := 0; i < len(modes); i += batchSize {
		end := i + batchSize
		if end > len(modes) {
			end = len(modes)
		}
		if err := r.upsertGameModesChunk(ctx, modes[i:end]); err != nil {
			return fmt.Errorf("game_modes chunk [%d:%d]: %w", i, end, err)
		}
	}
	return nil
}

func (r *Repository) upsertGameModesChunk(ctx context.Context, modes []GameModeRef) error {
	return r.WithTransaction(ctx, func(tx pgx.Tx) error {
		placeholders := make([]string, len(modes))
		args := make([]interface{}, 0, len(modes)*2)
		for i, m := range modes {
			base := i * 2
			placeholders[i] = fmt.Sprintf("($%d,$%d)", base+1, base+2)
			args = append(args, m.ID, m.Name)
		}
		valuesClause := strings.Join(placeholders, ", ")
		q := `
			INSERT INTO game_modes (id, name)
			VALUES ` + valuesClause + `
			ON CONFLICT (id) DO UPDATE SET name = COALESCE(NULLIF(EXCLUDED.name, ''), game_modes.name)`

		_, err := tx.Exec(ctx, q, args...)
		if err != nil {
			return fmt.Errorf("bulk upsert game_modes: %w", err)
		}
		return nil
	})
}

func (r *Repository) UpsertLobbyTypes(ctx context.Context, types []LobbyTypeRef) error {
	if len(types) == 0 {
		return nil
	}
	const batchSize = 1000
	for i := 0; i < len(types); i += batchSize {
		end := i + batchSize
		if end > len(types) {
			end = len(types)
		}
		if err := r.upsertLobbyTypesChunk(ctx, types[i:end]); err != nil {
			return fmt.Errorf("lobby_types chunk [%d:%d]: %w", i, end, err)
		}
	}
	return nil
}

func (r *Repository) upsertLobbyTypesChunk(ctx context.Context, types []LobbyTypeRef) error {
	return r.WithTransaction(ctx, func(tx pgx.Tx) error {
		placeholders := make([]string, len(types))
		args := make([]interface{}, 0, len(types)*2)
		for i, t := range types {
			base := i * 2
			placeholders[i] = fmt.Sprintf("($%d,$%d)", base+1, base+2)
			args = append(args, t.ID, t.Name)
		}
		valuesClause := strings.Join(placeholders, ", ")
		q := `
			INSERT INTO lobby_types (id, name)
			VALUES ` + valuesClause + `
			ON CONFLICT (id) DO UPDATE SET name = COALESCE(NULLIF(EXCLUDED.name, ''), lobby_types.name)`

		_, err := tx.Exec(ctx, q, args...)
		if err != nil {
			return fmt.Errorf("bulk upsert lobby_types: %w", err)
		}
		return nil
	})
}
