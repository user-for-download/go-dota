package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
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
		patchesURL:    cfg.PatchesURL,
	}
}

func (e *Enricher) Run(ctx context.Context) error {
	criticalSteps := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"patches", e.enrichPatches},
		{"heroes", e.enrichHeroes},
		{"items", e.enrichItems},
		{"game_modes", e.enrichGameModes},
		{"lobby_types", e.enrichLobbyTypes},
		{"leagues", e.enrichLeagues},
	}
	softSteps := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"teams", e.enrichTeams},
	}

	var criticalErrs, softErrs []error

	for _, s := range criticalSteps {
		if err := s.fn(ctx); err != nil {
			e.log.Error("critical enrich step failed", "step", s.name, "error", err)
			criticalErrs = append(criticalErrs, fmt.Errorf("%s: %w", s.name, err))
		}
	}

	for _, s := range softSteps {
		if err := s.fn(ctx); err != nil {
			e.log.Warn("soft enrich step failed", "step", s.name, "error", err)
			softErrs = append(softErrs, fmt.Errorf("%s: %w", s.name, err))
		}
	}

	if len(criticalErrs) > 0 {
		e.log.Error("critical enricher steps failed; NOT marking bootstrap",
			"critical_errors", len(criticalErrs),
			"soft_errors", len(softErrs))
		return errors.Join(append(criticalErrs, softErrs...)...)
	}

	if e.redis != nil {
		if err := e.redis.Instance().Set(ctx, "enricher:bootstrapped", "1", 7*24*time.Hour).Err(); err != nil {
			e.log.Warn("failed to set enricher bootstrap marker", "error", err)
		}
	}

	if len(softErrs) > 0 {
		e.log.Warn("enricher complete with soft errors", "soft_errors", len(softErrs))
		return errors.Join(softErrs...)
	}

	e.log.Info("enricher pass complete")
	return nil
}

func (e *Enricher) fetchJSON(ctx context.Context, url string, v any) error {
	const maxAttempts = 4
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var proxy string
		isDirect := attempt == maxAttempts
		if isDirect {
			proxy = ""
		} else if e.redis != nil {
			p, err := e.redis.GetWeightedRandomProxy(ctx)
			if err != nil {
				e.log.Warn("no proxy available, will try direct",
					"url", url, "attempt", attempt, "error", err)
				isDirect = true
			} else if !isUsableProxyURL(p) {
				e.log.Warn("proxy pool returned malformed URL, removing",
					"proxy", p, "url", url)
				_ = e.redis.RemoveProxy(ctx, p)
				continue
			} else {
				proxy = p
			}
		}

		resp, err := e.http.Get(ctx, url, proxy)
		if err != nil {
			lastErr = fmt.Errorf("attempt %d: %w", attempt, err)
			if !isDirect && proxy != "" {
				_ = e.redis.RecordProxyFailure(ctx, proxy, redis.DefaultMaxProxyFails)
				e.http.RemoveProxy(proxy)
				e.log.Debug("proxy fetch failed, rotating",
					"proxy", proxy, "url", url, "error", err)
			}
			if !backoff(ctx, attempt) {
				return ctx.Err()
			}
			continue
		}

		switch {
		case resp.StatusCode == http.StatusOK:
			if proxy != "" {
				_ = e.redis.RecordProxySuccess(ctx, proxy)
			}
			if err := json.Unmarshal(resp.Body, v); err != nil {
				return fmt.Errorf("unmarshal %s: %w", url, err)
			}
			return nil
		case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
			lastErr = fmt.Errorf("attempt %d: %s status %d", attempt, url, resp.StatusCode)
			if !isDirect && proxy != "" {
				_ = e.redis.RecordProxyFailure(ctx, proxy, redis.DefaultMaxProxyFails)
				e.http.RemoveProxy(proxy)
			}
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

func isUsableProxyURL(s string) bool {
	if s == "" {
		return false
	}
	u, err := url.Parse(s)
	if err != nil || u.Host == "" || u.Port() == "" {
		return false
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https", "socks4", "socks5":
		return true
	}
	return false
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
			AttackType:    h.AttackType,
			Roles:         roles,
			Legs:          h.Legs,
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
	TeamID        int64    `json:"team_id"`
	Name          string   `json:"name"`
	Tag           string   `json:"tag"`
	LogoURL       string   `json:"logo_url"`
	Rating        *float32 `json:"rating"`
	Wins          *int     `json:"wins"`
	Losses        *int     `json:"losses"`
	LastMatchTime *int64   `json:"last_match_time"`
}

type explorerTeamResponse struct {
	Rows []odTeam `json:"rows"`
}

func (e *Enricher) enrichTeams(ctx context.Context) error {
	var rows []odTeam

	if strings.Contains(strings.ToLower(e.teamsURL), "explorer") {
		var wrapper explorerTeamResponse
		if err := e.fetchJSON(ctx, e.teamsURL, &wrapper); err != nil {
			return fmt.Errorf("fetch explorer teams: %w", err)
		}
		rows = wrapper.Rows
	} else {
		if err := e.fetchJSON(ctx, e.teamsURL, &rows); err != nil {
			return fmt.Errorf("fetch teams array: %w", err)
		}
	}

	refs := make([]postgres.TeamRef, 0, len(rows))
	withRating := 0

	for _, t := range rows {
		if t.TeamID == 0 {
			continue
		}

		ref := postgres.TeamRef{
			TeamID:  t.TeamID,
			Name:    t.Name,
			Tag:     t.Tag,
			LogoURL: t.LogoURL,
		}

		if t.Rating != nil {
			ref.Rating = *t.Rating
		}
		if t.Wins != nil {
			ref.Wins = *t.Wins
		}
		if t.Losses != nil {
			ref.Losses = *t.Losses
		}
		if t.LastMatchTime != nil {
			ref.LastMatchTime = *t.LastMatchTime
		}

		if ref.Rating > 0 || ref.Wins > 0 || ref.Losses > 0 {
			withRating++
		}

		refs = append(refs, ref)
	}

	if err := e.repo.UpsertTeamsBulk(ctx, refs); err != nil {
		return err
	}

	e.log.Info("enriched teams",
		"count", len(refs),
		"with_rating", withRating,
		"source", e.teamsURL,
	)

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
			Image:         it.Img,
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
