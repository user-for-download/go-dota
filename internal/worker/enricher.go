package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/user-for-download/go-dota/internal/httpx"
	"github.com/user-for-download/go-dota/internal/storage/postgres"
	"github.com/user-for-download/go-dota/internal/storage/redis"
)

type Enricher struct {
	redis *redis.Client
	repo  *postgres.Repository
	log   *slog.Logger
	http  *httpx.ProxiedClient

	heroesURL     string
	leaguesURL    string
	teamsURL      string
	itemsURL      string
	gameModesURL  string
	lobbyTypesURL string
	patchesURL    string
}

type EnricherConfig struct {
	HeroesURL     string
	LeaguesURL    string
	TeamsURL      string
	ItemsURL      string
	GameModesURL  string
	LobbyTypesURL string
	PatchesURL    string
}

func DefaultEnricherConfig() EnricherConfig {
	return EnricherConfig{
		HeroesURL:     "https://api.opendota.com/api/heroes",
		LeaguesURL:    "https://api.opendota.com/api/leagues",
		TeamsURL:      "https://api.opendota.com/api/teams",
		ItemsURL:      "https://api.opendota.com/api/constants/items",
		GameModesURL:  "https://api.opendota.com/api/constants/game_mode",
		LobbyTypesURL: "https://api.opendota.com/api/constants/lobby_type",
		PatchesURL:    "https://api.opendota.com/api/constants/patch",
	}
}

func NewEnricher(
	rdb *redis.Client,
	repo *postgres.Repository,
	log *slog.Logger,
	cfg EnricherConfig,
) *Enricher {
	pool := httpx.NewTransportPool(httpx.DefaultOptions())
	return &Enricher{
		redis:         rdb,
		repo:          repo,
		log:           log,
		http:          httpx.NewProxiedClient(pool, 30*time.Second),
		heroesURL:     cfg.HeroesURL,
		leaguesURL:    cfg.LeaguesURL,
		teamsURL:      cfg.TeamsURL,
		itemsURL:      cfg.ItemsURL,
		gameModesURL:  cfg.GameModesURL,
		lobbyTypesURL: cfg.LobbyTypesURL,
		patchesURL:   cfg.PatchesURL,
	}
}

func (e *Enricher) Run(ctx context.Context) error {
	var errs []error
	steps := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"patches", e.enrichPatches},
		{"heroes", e.enrichHeroes},
		{"items", e.enrichItems},
		{"game_modes", e.enrichGameModes},
		{"lobby_types", e.enrichLobbyTypes},
		{"leagues", e.enrichLeagues},
		{"teams", e.enrichTeams},
	}
	for _, s := range steps {
		if err := s.fn(ctx); err != nil {
			e.log.Error("enrich step failed", "step", s.name, "error", err)
			errs = append(errs, fmt.Errorf("%s: %w", s.name, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (e *Enricher) fetchJSON(ctx context.Context, url string, v any) error {
	const maxAttempts = 3
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var proxy string
		if e.redis != nil {
			p, err := e.redis.GetWeightedRandomProxy(ctx)
			if err != nil {
				e.log.Warn("no proxy, going direct", "url", url, "error", err)
			} else {
				proxy = p
			}
		}

		resp, err := e.http.Get(ctx, url, proxy)
		if err != nil {
			lastErr = fmt.Errorf("attempt %d: %w", attempt, err)
			if !backoff(ctx, attempt) {
				return ctx.Err()
			}
			continue
		}

		switch {
		case resp.StatusCode == http.StatusOK:
			if err := json.Unmarshal(resp.Body, v); err != nil {
				return fmt.Errorf("unmarshal %s: %w", url, err)
			}
			return nil
		case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
			lastErr = fmt.Errorf("attempt %d: %s status %d", attempt, url, resp.StatusCode)
			if !backoff(ctx, attempt) {
				return ctx.Err()
			}
			continue
		default:
			return fmt.Errorf("%s: status %d", url, resp.StatusCode)
		}
	}
	return fmt.Errorf("exhausted retries: %w", lastErr)
}

func backoff(ctx context.Context, attempt int) bool {
	d := time.Duration(attempt*attempt) * 2 * time.Second
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type odHero struct {
	ID            int16    `json:"id"`
	Name          string   `json:"name"`
	LocalizedName string   `json:"localized_name"`
	PrimaryAttr   string   `json:"primary_attr"`
	AttackType    string   `json:"attack_type"`
	Roles         []string `json:"roles"`
	Legs          *int16   `json:"legs,omitempty"`
}

func (e *Enricher) enrichHeroes(ctx context.Context) error {
	var heroes []odHero
	if err := e.fetchJSON(ctx, e.heroesURL, &heroes); err != nil {
		return err
	}
	refs := make([]postgres.HeroRef, 0, len(heroes))
	for _, h := range heroes {
		roles := h.Roles
		if roles == nil {
			roles = []string{}
		}
		refs = append(refs, postgres.HeroRef{
			ID:            h.ID,
			Name:          h.Name,
			LocalizedName: h.LocalizedName,
			PrimaryAttr:   h.PrimaryAttr,
			AttackType:   h.AttackType,
			Roles:        roles,
			Legs:        h.Legs,
		})
	}
	if err := e.repo.UpsertHeroes(ctx, refs); err != nil {
		return err
	}
	e.log.Info("enriched heroes", "count", len(refs))
	return nil
}

type odLeague struct {
	LeagueID int32  `json:"leagueid"`
	Name     string `json:"name"`
	Ticket   string `json:"ticket"`
	Banner   string `json:"banner"`
	Tier     string `json:"tier"`
}

func (e *Enricher) enrichLeagues(ctx context.Context) error {
	var leagues []odLeague
	if err := e.fetchJSON(ctx, e.leaguesURL, &leagues); err != nil {
		return err
	}
	refs := make([]postgres.LeagueRef, 0, len(leagues))
	for _, l := range leagues {
		refs = append(refs, postgres.LeagueRef{
			ID:     l.LeagueID,
			Name:   l.Name,
			Ticket: l.Ticket,
			Banner: l.Banner,
			Tier:   l.Tier,
		})
	}
	if err := e.repo.UpsertLeagues(ctx, refs); err != nil {
		return err
	}
	e.log.Info("enriched leagues", "count", len(refs))
	return nil
}

type odTeam struct {
	TeamID        int64   `json:"team_id"`
	Name          string  `json:"name"`
	Tag           string  `json:"tag"`
	LogoURL       string  `json:"logo_url"`
	Rating        float32 `json:"rating"`
	Wins          int     `json:"wins"`
	LastMatchTime int64   `json:"last_match_time"`
}

func (e *Enricher) enrichTeams(ctx context.Context) error {
	var teams []odTeam
	if err := e.fetchJSON(ctx, e.teamsURL, &teams); err != nil {
		return err
	}
	refs := make([]postgres.TeamRef, 0, len(teams))
	for _, t := range teams {
		refs = append(refs, postgres.TeamRef{
			TeamID:        t.TeamID,
			Name:          t.Name,
			Tag:           t.Tag,
			LogoURL:       t.LogoURL,
			Rating:        t.Rating,
			Wins:          t.Wins,
			LastMatchTime: t.LastMatchTime,
		})
	}
	if err := e.repo.UpsertTeamsBulk(ctx, refs); err != nil {
		return err
	}
	withRating := 0
	for _, t := range teams {
		if t.Rating > 0 || t.Wins > 0 {
			withRating++
		}
	}
	e.log.Info("enriched teams", "count", len(teams), "with_rating", withRating)
	return nil
}

type odItem struct {
	ID         int    `json:"id"`
	DName      string `json:"dname"`
	Cost       int    `json:"cost"`
	Img        string `json:"img"`
	Recipe     int    `json:"recipe"`
	SecretShop int    `json:"secret_shop"`
	SideShop   int    `json:"side_shop"`
}

func (e *Enricher) enrichItems(ctx context.Context) error {
	var items map[string]odItem
	if err := e.fetchJSON(ctx, e.itemsURL, &items); err != nil {
		return fmt.Errorf("fetch items: %w", err)
	}
	refs := make([]postgres.ItemRef, 0, len(items))
	for name, it := range items {
		if it.ID == 0 {
			continue
		}
		refs = append(refs, postgres.ItemRef{
			ID:            it.ID,
			Name:          name,
			LocalizedName: it.DName,
			Cost:          it.Cost,
			Recipe:        it.Recipe == 1,
			SecretShop:    it.SecretShop == 1,
			SideShop:      it.SideShop == 1,
			Image:        it.Img,
		})
	}
	if err := e.repo.UpsertItems(ctx, refs); err != nil {
		return fmt.Errorf("upsert items: %w", err)
	}
	e.log.Info("enriched items", "count", len(refs))
	return nil
}

func (e *Enricher) enrichGameModes(ctx context.Context) error {
	var modes map[string]struct {
		ID   int16  `json:"id"`
		Name string `json:"name"`
	}
	if err := e.fetchJSON(ctx, e.gameModesURL, &modes); err != nil {
		return err
	}
	refs := make([]postgres.GameModeRef, 0, len(modes))
	for _, m := range modes {
		refs = append(refs, postgres.GameModeRef{ID: m.ID, Name: m.Name})
	}
	if err := e.repo.UpsertGameModes(ctx, refs); err != nil {
		return err
	}
	e.log.Info("enriched game modes", "count", len(refs))
	return nil
}

func (e *Enricher) enrichLobbyTypes(ctx context.Context) error {
	var types map[string]struct {
		ID   int16  `json:"id"`
		Name string `json:"name"`
	}
	if err := e.fetchJSON(ctx, e.lobbyTypesURL, &types); err != nil {
		return err
	}
	refs := make([]postgres.LobbyTypeRef, 0, len(types))
	for _, t := range types {
		refs = append(refs, postgres.LobbyTypeRef{ID: t.ID, Name: t.Name})
	}
	if err := e.repo.UpsertLobbyTypes(ctx, refs); err != nil {
		return err
	}
	e.log.Info("enriched lobby types", "count", len(refs))
	return nil
}

type odPatch struct {
	ID   int16  `json:"id"`
	Name string `json:"name"`
	Date string `json:"date"`
}

func (e *Enricher) enrichPatches(ctx context.Context) error {
	var patches []odPatch
	if err := e.fetchJSON(ctx, e.patchesURL, &patches); err != nil {
		return err
	}
	now := time.Now()
	refs := make([]postgres.PatchRef, 0, len(patches))
	for _, p := range patches {
		t, err := time.Parse(time.RFC3339, p.Date)
		if err != nil {
			e.log.Warn("invalid patch date", "patch", p.Name, "date", p.Date, "error", err)
			continue
		}
		if t.After(now) {
			continue
		}
		refs = append(refs, postgres.PatchRef{
			ID:           p.ID,
			Name:         p.Name,
			ReleaseDate:  t,
			ReleaseEpoch: t.Unix(),
		})
	}
	if err := e.repo.UpsertPatches(ctx, refs); err != nil {
		return err
	}
	e.log.Info("enriched patches", "count", len(refs))
	return nil
}