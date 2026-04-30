package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type PlayerRef struct {
	AccountID       int64
	SteamID         string
	Personaname     string
	Avatar          string
	AvatarMedium    string
	AvatarFull      string
	ProfileURL      string
	LocCountryCode  string
	Plus            bool
	Cheese          int
	FhUnavailable   bool
	LastLogin       *time.Time
	LastMatchTime   *time.Time
	FullHistoryTime *time.Time
	ProfileTime     *time.Time
	RankTierTime    *time.Time
}

type NotablePlayerRef struct {
	AccountID   int64
	Name        string
	CountryCode string
	FantasyRole int16
	TeamID      *int64
	TeamName    string
	TeamTag     string
	IsPro       bool
	IsLocked    bool
	LockedUntil *int64
}

const playersBatchSize = 500

func (r *Repository) UpsertPlayers(ctx context.Context, players []PlayerRef) error {
	if len(players) == 0 {
		return nil
	}
	for i := 0; i < len(players); i += playersBatchSize {
		end := i + playersBatchSize
		if end > len(players) {
			end = len(players)
		}
		if err := r.upsertPlayersChunk(ctx, players[i:end]); err != nil {
			return fmt.Errorf("players chunk [%d:%d]: %w", i, end, err)
		}
	}
	return nil
}

func (r *Repository) upsertPlayersChunk(ctx context.Context, players []PlayerRef) error {
	const cols = 16
	placeholders := make([]string, len(players))
	args := make([]any, 0, len(players)*cols)
	for i, p := range players {
		base := i * cols
		ph := make([]string, cols)
		for j := 0; j < cols; j++ {
			ph[j] = fmt.Sprintf("$%d", base+j+1)
		}
		placeholders[i] = "(" + strings.Join(ph, ",") + ")"
		args = append(args,
			p.AccountID, p.SteamID, p.Personaname, p.Avatar,
			p.AvatarMedium, p.AvatarFull, p.ProfileURL, p.LocCountryCode,
			p.Plus, p.Cheese, p.FhUnavailable,
			p.LastLogin, p.LastMatchTime, p.FullHistoryTime,
			p.ProfileTime, p.RankTierTime,
		)
	}

	q := `
		INSERT INTO players (
			account_id, steamid, personaname, avatar,
			avatarmedium, avatarfull, profileurl, loccountrycode,
			plus, cheese, fh_unavailable,
			last_login, last_match_time, full_history_time,
			profile_time, rank_tier_time
		) VALUES ` + strings.Join(placeholders, ", ") + `
		ON CONFLICT (account_id) DO UPDATE SET
			steamid           = COALESCE(NULLIF(EXCLUDED.steamid, ''),        players.steamid),
			personaname       = COALESCE(NULLIF(EXCLUDED.personaname, ''),    players.personaname),
			avatar            = COALESCE(NULLIF(EXCLUDED.avatar, ''),         players.avatar),
			avatarmedium      = COALESCE(NULLIF(EXCLUDED.avatarmedium, ''),   players.avatarmedium),
			avatarfull        = COALESCE(NULLIF(EXCLUDED.avatarfull, ''),     players.avatarfull),
			profileurl        = COALESCE(NULLIF(EXCLUDED.profileurl, ''),     players.profileurl),
			loccountrycode    = COALESCE(NULLIF(EXCLUDED.loccountrycode, ''), players.loccountrycode),
			plus              = EXCLUDED.plus,
			cheese            = GREATEST(players.cheese, EXCLUDED.cheese),
			fh_unavailable    = EXCLUDED.fh_unavailable,
			last_login        = COALESCE(EXCLUDED.last_login,        players.last_login),
			last_match_time   = COALESCE(EXCLUDED.last_match_time,   players.last_match_time),
			full_history_time = COALESCE(EXCLUDED.full_history_time, players.full_history_time),
			profile_time      = COALESCE(EXCLUDED.profile_time,      players.profile_time),
			rank_tier_time    = COALESCE(EXCLUDED.rank_tier_time,    players.rank_tier_time),
			updated_at        = NOW()`
	if _, err := r.pool.Exec(ctx, q, args...); err != nil {
		return fmt.Errorf("bulk upsert players: %w", err)
	}
	return nil
}

func (r *Repository) UpsertNotablePlayers(ctx context.Context, np []NotablePlayerRef) error {
	if len(np) == 0 {
		return nil
	}
	for i := 0; i < len(np); i += playersBatchSize {
		end := i + playersBatchSize
		if end > len(np) {
			end = len(np)
		}
		if err := r.upsertNotablePlayersChunk(ctx, np[i:end]); err != nil {
			return fmt.Errorf("notable_players chunk [%d:%d]: %w", i, end, err)
		}
	}
	return nil
}

func (r *Repository) upsertNotablePlayersChunk(ctx context.Context, np []NotablePlayerRef) error {
	const cols = 10
	placeholders := make([]string, len(np))
	args := make([]any, 0, len(np)*cols)
	for i, p := range np {
		base := i * cols
		ph := make([]string, cols)
		for j := 0; j < cols; j++ {
			ph[j] = fmt.Sprintf("$%d", base+j+1)
		}
		placeholders[i] = "(" + strings.Join(ph, ",") + ")"
		args = append(args,
			p.AccountID, p.Name, p.CountryCode, p.FantasyRole,
			p.TeamID, p.TeamName, p.TeamTag,
			p.IsPro, p.IsLocked, p.LockedUntil,
		)
	}

	q := `
		INSERT INTO notable_players (
			account_id, name, country_code, fantasy_role,
			team_id, team_name, team_tag,
			is_pro, is_locked, locked_until
		) VALUES ` + strings.Join(placeholders, ", ") + `
		ON CONFLICT (account_id) DO UPDATE SET
			name         = COALESCE(NULLIF(EXCLUDED.name, ''),         notable_players.name),
			country_code = COALESCE(NULLIF(EXCLUDED.country_code, ''), notable_players.country_code),
			fantasy_role = EXCLUDED.fantasy_role,
			team_id      = EXCLUDED.team_id,
			team_name    = COALESCE(NULLIF(EXCLUDED.team_name, ''),    notable_players.team_name),
			team_tag     = COALESCE(NULLIF(EXCLUDED.team_tag, ''),     notable_players.team_tag),
			is_pro       = EXCLUDED.is_pro,
			is_locked    = EXCLUDED.is_locked,
			locked_until = EXCLUDED.locked_until,
			updated_at   = NOW()`
	if _, err := r.pool.Exec(ctx, q, args...); err != nil {
		return fmt.Errorf("bulk upsert notable_players: %w", err)
	}
	return nil
}