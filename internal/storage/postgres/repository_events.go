package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/user-for-download/go-dota/internal/models"
)

// replaceObjectivesTx bulk-inserts match objectives (tower kills, Roshan, etc.).
func replaceObjectivesTx(ctx context.Context, tx pgx.Tx, matchID, startTime int64, objs []models.Objective) error {
	if _, err := tx.Exec(ctx,
		`DELETE FROM match_objectives WHERE match_id = $1`, matchID,
	); err != nil {
		return fmt.Errorf("delete match_objectives: %w", err)
	}
	if len(objs) == 0 {
		return nil
	}

	rows := make([][]any, 0, len(objs))
	for i := range objs {
		o := &objs[i]
		rows = append(rows, []any{
			matchID, startTime, o.Time, o.Type, o.Slot, o.PlayerSlot,
			o.Team, o.Key, o.Value, o.Unit,
		})
	}

	n, err := tx.CopyFrom(ctx,
		pgx.Identifier{"match_objectives"},
		[]string{
			"match_id", "start_time", "time", "type", "slot", "player_slot",
			"team", "key", "value", "unit",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy match_objectives: %w", err)
	}
	if int(n) != len(rows) {
		return fmt.Errorf("copy match_objectives: wrote %d of %d rows", n, len(rows))
	}
	return nil
}

// replaceChatTx bulk-inserts match chat events.
func replaceChatTx(ctx context.Context, tx pgx.Tx, matchID, startTime int64, chat []models.ChatEvent) error {
	if _, err := tx.Exec(ctx,
		`DELETE FROM match_chat WHERE match_id = $1`, matchID,
	); err != nil {
		return fmt.Errorf("delete match_chat: %w", err)
	}
	if len(chat) == 0 {
		return nil
	}

	rows := make([][]any, 0, len(chat))
	for i := range chat {
		c := &chat[i]
		rows = append(rows, []any{
			matchID, startTime, c.Time, c.Type, c.PlayerSlot, c.Unit, c.Key,
		})
	}

	n, err := tx.CopyFrom(ctx,
		pgx.Identifier{"match_chat"},
		[]string{"match_id", "start_time", "time", "type", "player_slot", "unit", "key"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy match_chat: %w", err)
	}
	if int(n) != len(rows) {
		return fmt.Errorf("copy match_chat: wrote %d of %d rows", n, len(rows))
	}
	return nil
}

// replaceTeamfightsTx bulk-inserts teamfight summaries.
// If the nested `players` JSON is malformed, the column is stored as NULL
// and a warning is logged so the data drift is visible in observability.
func replaceTeamfightsTx(ctx context.Context, tx pgx.Tx, matchID, startTime int64, tfs []models.Teamfight) error {
	if _, err := tx.Exec(ctx,
		`DELETE FROM match_teamfights WHERE match_id = $1`, matchID,
	); err != nil {
		return fmt.Errorf("delete match_teamfights: %w", err)
	}
	if len(tfs) == 0 {
		return nil
	}

	log := slog.Default()
	rows := make([][]any, 0, len(tfs))
	for i := range tfs {
		t := &tfs[i]

		var players any
		if len(t.Players) > 0 && string(t.Players) != "null" {
			var probe any
			if err := json.Unmarshal(t.Players, &probe); err != nil {
				log.Warn("invalid teamfight players JSON, storing NULL",
					"match_id", matchID,
					"start", t.Start,
					"error", err)
			} else {
				players = []byte(t.Players)
			}
		}

		rows = append(rows, []any{
			matchID, startTime, t.End, t.LastDeath, t.Deaths, players,
		})
	}

	n, err := tx.CopyFrom(ctx,
		pgx.Identifier{"match_teamfights"},
		[]string{"match_id", "start_time", "end_time", "last_death", "deaths", "players"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy match_teamfights: %w", err)
	}
	if int(n) != len(rows) {
		return fmt.Errorf("copy match_teamfights: wrote %d of %d rows", n, len(rows))
	}
	return nil
}
