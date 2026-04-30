package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type HeroStatsRef struct {
	ID              int16
	BaseHealth      int
	BaseMana        int
	BaseArmor       float32
	BaseMR          float32
	BaseAttackMin   int16
	BaseAttackMax   int16
	BaseStr         int16
	BaseAgi         int16
	BaseInt         int16
	StrGain         float32
	AgiGain         float32
	IntGain         float32
	AttackRange     int16
	ProjectileSpeed int16
	AttackRate      float32
	MoveSpeed       int16
	TurnRate        *float32
	CMEnabled       bool
	TurboPicks      int
	TurboWins       int
	ProPicks        int
	ProWins         int
	ProBans         int
	PubPicks        int
	PubWins         int
	PubWinRate      float32
	ProWinRate      float32
}

func (r *Repository) UpsertHeroStats(ctx context.Context, refs []HeroStatsRef) error {
	if len(refs) == 0 {
		return nil
	}
	const batchSize = 500
	for i := 0; i < len(refs); i += batchSize {
		end := i + batchSize
		if end > len(refs) {
			end = len(refs)
		}
		if err := r.upsertHeroStatsChunk(ctx, refs[i:end]); err != nil {
			return fmt.Errorf("hero_stats chunk [%d:%d]: %w", i, end, err)
		}
	}
	return nil
}

func (r *Repository) upsertHeroStatsChunk(ctx context.Context, refs []HeroStatsRef) error {
	return r.WithTransaction(ctx, func(tx pgx.Tx) error {
		const cols = 29
		placeholders := make([]string, len(refs))
		args := make([]any, 0, len(refs)*cols)
		now := time.Now()

		for i, h := range refs {
			base := i * cols
			ph := make([]string, cols)
			for j := 0; j < cols; j++ {
				ph[j] = fmt.Sprintf("$%d", base+j+1)
			}
			placeholders[i] = "(" + strings.Join(ph, ",") + ")"
			args = append(args,
				h.ID, h.BaseHealth, h.BaseMana, h.BaseArmor, h.BaseMR,
				h.BaseAttackMin, h.BaseAttackMax,
				h.BaseStr, h.BaseAgi, h.BaseInt,
				h.StrGain, h.AgiGain, h.IntGain,
				h.AttackRange, h.ProjectileSpeed, h.AttackRate,
				h.MoveSpeed, h.TurnRate, h.CMEnabled,
				h.TurboPicks, h.TurboWins,
				h.ProPicks, h.ProWins, h.ProBans,
				h.PubPicks, h.PubWins,
				h.PubWinRate, h.ProWinRate, now,
			)
		}

		q := `
			INSERT INTO hero_stats (
				id, base_health, base_mana, base_armor, base_mr,
				base_attack_min, base_attack_max,
				base_str, base_agi, base_int,
				str_gain, agi_gain, int_gain,
				attack_range, projectile_speed, attack_rate,
				move_speed, turn_rate, cm_enabled,
				turbo_picks, turbo_wins,
				pro_picks, pro_wins, pro_bans,
				pub_picks, pub_wins,
				pub_win_rate, pro_win_rate, updated_at
			) VALUES ` + strings.Join(placeholders, ", ") + `
			ON CONFLICT (id) DO UPDATE SET
				base_health      = EXCLUDED.base_health,
				base_mana        = EXCLUDED.base_mana,
				base_armor       = EXCLUDED.base_armor,
				base_mr          = EXCLUDED.base_mr,
				base_attack_min  = EXCLUDED.base_attack_min,
				base_attack_max  = EXCLUDED.base_attack_max,
				base_str         = EXCLUDED.base_str,
				base_agi         = EXCLUDED.base_agi,
				base_int         = EXCLUDED.base_int,
				str_gain         = EXCLUDED.str_gain,
				agi_gain         = EXCLUDED.agi_gain,
				int_gain         = EXCLUDED.int_gain,
				attack_range     = EXCLUDED.attack_range,
				projectile_speed = EXCLUDED.projectile_speed,
				attack_rate      = EXCLUDED.attack_rate,
				move_speed       = EXCLUDED.move_speed,
				turn_rate        = EXCLUDED.turn_rate,
				cm_enabled       = EXCLUDED.cm_enabled,
				turbo_picks      = EXCLUDED.turbo_picks,
				turbo_wins       = EXCLUDED.turbo_wins,
				pro_picks        = EXCLUDED.pro_picks,
				pro_wins         = EXCLUDED.pro_wins,
				pro_bans         = EXCLUDED.pro_bans,
				pub_picks        = EXCLUDED.pub_picks,
				pub_wins         = EXCLUDED.pub_wins,
				pub_win_rate     = EXCLUDED.pub_win_rate,
				pro_win_rate     = EXCLUDED.pro_win_rate,
				updated_at       = NOW()`

		if _, err := tx.Exec(ctx, q, args...); err != nil {
			return fmt.Errorf("bulk upsert hero_stats: %w", err)
		}
		return nil
	})
}