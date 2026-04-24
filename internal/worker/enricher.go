package worker

import (
	"context"
	"encoding/json"
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

	heroesURL  string
	leaguesURL string
	teamsURL   string
}

type EnricherConfig struct {
	HeroesURL  string
	LeaguesURL string
	TeamsURL   string
}

func DefaultEnricherConfig() EnricherConfig {
	return EnricherConfig{
		HeroesURL:  "https://api.opendota.com/api/heroes",
		LeaguesURL: "https://api.opendota.com/api/leagues",
		TeamsURL:   "https://api.opendota.com/api/teams",
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
		redis:      rdb,
		repo:       repo,
		log:       log,
		http:      httpx.NewProxiedClient(pool, 30*time.Second),
		heroesURL:  cfg.HeroesURL,
		leaguesURL: cfg.LeaguesURL,
		teamsURL:  cfg.TeamsURL,
	}
}

func (e *Enricher) Run(ctx context.Context) error {
	if err := e.enrichHeroes(ctx); err != nil {
		e.log.Error("enrich heroes", "error", err)
	}
	if err := e.enrichLeagues(ctx); err != nil {
		e.log.Error("enrich leagues", "error", err)
	}
	if err := e.enrichTeams(ctx); err != nil {
		e.log.Error("enrich teams", "error", err)
	}
	return nil
}

func (e *Enricher) fetchJSON(ctx context.Context, url string, v any) error {
	var proxy string
	var err error
	if e.redis != nil {
		proxy, err = e.redis.GetWeightedRandomProxy(ctx)
		if err != nil {
			return fmt.Errorf("get proxy: %w", err)
		}
	}
	resp, err := e.http.Get(ctx, url, proxy)
	if err != nil {
		return fmt.Errorf("get %s: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: status %d", url, resp.StatusCode)
	}
	return json.Unmarshal(resp.Body, v)
}

type odHero struct {
	ID            int16    `json:"id"`
	Name          string   `json:"name"`
	LocalizedName string   `json:"localized_name"`
	PrimaryAttr  string   `json:"primary_attr"`
	AttackType  string   `json:"attack_type"`
	Roles      []string `json:"roles"`
}

func (e *Enricher) enrichHeroes(ctx context.Context) error {
	var heroes []odHero
	if err := e.fetchJSON(ctx, e.heroesURL, &heroes); err != nil {
		return err
	}
	refs := make([]postgres.HeroRef, 0, len(heroes))
	for _, h := range heroes {
		refs = append(refs, postgres.HeroRef{
			ID:            h.ID,
			Name:          h.Name,
			LocalizedName: h.LocalizedName,
			PrimaryAttr:  h.PrimaryAttr,
			AttackType:   h.AttackType,
			Roles:        h.Roles,
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
	Region   *int16 `json:"region,omitempty"`
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
			Region: l.Region,
		})
	}
	if err := e.repo.UpsertLeagues(ctx, refs); err != nil {
		return err
	}
	e.log.Info("enriched leagues", "count", len(refs))
	return nil
}

type odTeam struct {
	TeamID      int64   `json:"team_id"`
	Name       string  `json:"name"`
	Tag        string  `json:"tag"`
	LogoURL    string  `json:"logo_url"`
	Rating    float32 `json:"rating"`
	Wins       int    `json:"wins"`
	Losses    int    `json:"losses"`
	LastMatchTime int64 `json:"last_match_time"`
}

func (e *Enricher) enrichTeams(ctx context.Context) error {
	var teams []odTeam
	if err := e.fetchJSON(ctx, e.teamsURL, &teams); err != nil {
		return err
	}
	for _, t := range teams {
		if err := e.repo.UpsertTeam(ctx, t.TeamID, t.Name, t.Tag, t.LogoURL); err != nil {
			e.log.Warn("upsert team failed", "team_id", t.TeamID, "error", err)
			continue
		}
		if t.Rating > 0 || t.Wins > 0 || t.Losses > 0 {
			if err := e.repo.UpsertTeamRating(
				ctx, t.TeamID, t.Rating, t.Wins, t.Losses, t.LastMatchTime, 0,
			); err != nil {
				e.log.Warn("upsert team rating failed", "team_id", t.TeamID, "error", err)
			}
		}
	}
	e.log.Info("enriched teams", "count", len(teams))
	return nil
}