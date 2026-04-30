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

	heroesURL      string
	leaguesURL     string
	teamsURL       string
	itemsURL       string
	gameModesURL   string
	lobbyTypesURL  string
	patchesURL     string
	proPlayersURL  string
	abilitiesURL   string
	abilityIDsURL  string
	heroStatsURL   string
}

type EnricherConfig struct {
	HeroesURL      string
	LeaguesURL     string
	TeamsURL       string
	ItemsURL       string
	GameModesURL   string
	LobbyTypesURL  string
	PatchesURL     string
	ProPlayersURL  string
	AbilitiesURL   string
	AbilityIDsURL  string
	HeroStatsURL   string
	SkipTLSVerify  bool
}

func NewEnricher(
	rdb *redis.Client,
	repo *postgres.Repository,
	log *slog.Logger,
	cfg EnricherConfig,
) *Enricher {
	opts := httpx.DefaultOptions()
	opts.SkipTLSVerify = cfg.SkipTLSVerify
	pool := httpx.NewTransportPool(opts)
	return &Enricher{
		redis:         rdb,
		repo:          repo,
		log:           log,
		http:          httpx.NewProxiedClient(pool, 30*time.Second),
		heroesURL:      cfg.HeroesURL,
		leaguesURL:     cfg.LeaguesURL,
		teamsURL:       cfg.TeamsURL,
		itemsURL:       cfg.ItemsURL,
		gameModesURL:   cfg.GameModesURL,
		lobbyTypesURL:  cfg.LobbyTypesURL,
		patchesURL:     cfg.PatchesURL,
		proPlayersURL:  cfg.ProPlayersURL,
		abilitiesURL:   cfg.AbilitiesURL,
		abilityIDsURL:  cfg.AbilityIDsURL,
		heroStatsURL:   cfg.HeroStatsURL,
	}
}

func (e *Enricher) Run(ctx context.Context) error {
	criticalSteps := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"patches", e.enrichPatches},
		{"heroes", e.enrichHeroes},
		{"hero_stats", e.enrichHeroStats},
		{"abilities", e.enrichAbilities},
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
		{"pro_players", e.enrichProPlayers},
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
		var leaseToken string
		isDirect := attempt == maxAttempts
		releaseLease := func() {
			if proxy != "" && leaseToken != "" {
				if err := e.redis.ReleaseProxyLease(context.Background(), proxy, leaseToken); err != nil {
					e.log.Warn("release proxy lease failed", "proxy", proxy, "error", err)
				}
			}
		}

		if isDirect {
			proxy = ""
		} else if e.redis != nil {
			p, token, err := e.redis.AcquireLeasedProxy(ctx, 2*time.Minute, 10)
			if err != nil {
				e.log.Warn("no free proxy available, will try direct",
					"url", url, "attempt", attempt, "error", err)
				isDirect = true
			} else if !isUsableProxyURL(p) {
				_ = e.redis.ReleaseProxyLease(context.Background(), p, token)
				e.log.Warn("proxy pool returned malformed URL, removing",
					"proxy", p, "url", url)
				_ = e.redis.RemoveProxy(ctx, p)
				continue
			} else {
				proxy = p
				leaseToken = token
			}
		}

		resp, err := e.http.Get(ctx, url, proxy)
		if err != nil {
			releaseLease()
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
			releaseLease()
			if err := json.Unmarshal(resp.Body, v); err != nil {
				return fmt.Errorf("unmarshal %s: %w", url, err)
			}
			return nil
		case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
			releaseLease()
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
	case "http", "https":
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

type odProPlayer struct {
	AccountID       int64      `json:"account_id"`
	SteamID         *string    `json:"steamid"`
	Avatar          *string    `json:"avatar"`
	AvatarMedium    *string    `json:"avatarmedium"`
	AvatarFull      *string    `json:"avatarfull"`
	ProfileURL      *string    `json:"profileurl"`
	Personaname     *string    `json:"personaname"`
	LastLogin       *time.Time `json:"last_login"`
	FullHistoryTime *time.Time `json:"full_history_time"`
	Cheese          *int       `json:"cheese"`
	FhUnavailable   *bool      `json:"fh_unavailable"`
	LocCountryCode  *string    `json:"loccountrycode"`
	LastMatchTime   *time.Time `json:"last_match_time"`
	Plus            *bool      `json:"plus"`
	ProfileTime     *time.Time `json:"profile_time"`
	RankTierTime    *time.Time `json:"rank_tier_time"`

	Name        *string `json:"name"`
	CountryCode *string `json:"country_code"`
	FantasyRole *int16  `json:"fantasy_role"`
	TeamID      *int64  `json:"team_id"`
	TeamName    *string `json:"team_name"`
	TeamTag     *string `json:"team_tag"`
	IsLocked    *bool   `json:"is_locked"`
	IsPro       *bool   `json:"is_pro"`
	LockedUntil *int64  `json:"locked_until"`
}

func (e *Enricher) enrichProPlayers(ctx context.Context) error {
	if e.proPlayersURL == "" {
		e.log.Warn("pro_players URL not configured, skipping")
		return nil
	}

	var pros []odProPlayer
	if err := e.fetchJSON(ctx, e.proPlayersURL, &pros); err != nil {
		return fmt.Errorf("fetch pro players: %w", err)
	}

	teamSeen := make(map[int64]struct{}, len(pros))
	teamStubs := make([]postgres.TeamRef, 0, len(pros))
	for _, p := range pros {
		if p.TeamID == nil || *p.TeamID <= 0 {
			continue
		}
		if _, ok := teamSeen[*p.TeamID]; ok {
			continue
		}
		teamSeen[*p.TeamID] = struct{}{}
		teamStubs = append(teamStubs, postgres.TeamRef{
			TeamID:  *p.TeamID,
			Name:    strDeref(p.TeamName),
			Tag:     strDeref(p.TeamTag),
			LogoURL: "",
		})
	}
	if len(teamStubs) > 0 {
		if err := e.repo.UpsertTeamsBulk(ctx, teamStubs); err != nil {
			return fmt.Errorf("upsert team stubs: %w", err)
		}
	}

	players := make([]postgres.PlayerRef, 0, len(pros))
	notable := make([]postgres.NotablePlayerRef, 0, len(pros))

	for _, p := range pros {
		if p.AccountID == 0 {
			continue
		}

		players = append(players, postgres.PlayerRef{
			AccountID:       p.AccountID,
			SteamID:         strDeref(p.SteamID),
			Personaname:     strDeref(p.Personaname),
			Avatar:          strDeref(p.Avatar),
			AvatarMedium:    strDeref(p.AvatarMedium),
			AvatarFull:      strDeref(p.AvatarFull),
			ProfileURL:      strDeref(p.ProfileURL),
			LocCountryCode:  strDeref(p.LocCountryCode),
			Plus:            boolDeref(p.Plus),
			Cheese:          intDeref(p.Cheese),
			FhUnavailable:   boolDeref(p.FhUnavailable),
			LastLogin:       p.LastLogin,
			LastMatchTime:   p.LastMatchTime,
			FullHistoryTime: p.FullHistoryTime,
			ProfileTime:     p.ProfileTime,
			RankTierTime:    p.RankTierTime,
		})

		var teamID *int64
		if p.TeamID != nil && *p.TeamID > 0 {
			teamID = p.TeamID
		}

		notable = append(notable, postgres.NotablePlayerRef{
			AccountID:   p.AccountID,
			Name:        strDeref(p.Name),
			CountryCode: strDeref(p.CountryCode),
			FantasyRole: int16Deref(p.FantasyRole),
			TeamID:      teamID,
			TeamName:    strDeref(p.TeamName),
			TeamTag:     strDeref(p.TeamTag),
			IsPro:       boolDeref(p.IsPro),
			IsLocked:    boolDeref(p.IsLocked),
			LockedUntil: p.LockedUntil,
		})
	}

	if err := e.repo.UpsertPlayers(ctx, players); err != nil {
		return fmt.Errorf("upsert players: %w", err)
	}
	if err := e.repo.UpsertNotablePlayers(ctx, notable); err != nil {
		return fmt.Errorf("upsert notable_players: %w", err)
	}

	e.log.Info("enriched pro players",
		"players", len(players),
		"notable", len(notable),
		"team_stubs", len(teamStubs),
	)
	return nil
}

func strDeref(s *string) string  { if s == nil { return "" }; return *s }
func boolDeref(b *bool) bool     { if b == nil { return false }; return *b }
func intDeref(i *int) int        { if i == nil { return 0 }; return *i }
func int16Deref(i *int16) int16  { if i == nil { return 0 }; return *i }

type odAbility struct {
	DName       string          `json:"dname"`
	Behavior    json.RawMessage `json:"behavior"`
	TargetTeam  json.RawMessage `json:"target_team"`
	Description string          `json:"desc"`
	Img         string          `json:"img"`
	ManaCost    json.RawMessage `json:"mc"`
	Cooldown    json.RawMessage `json:"cd"`
	Attrib      json.RawMessage `json:"attrib"`
}

func (e *Enricher) enrichAbilities(ctx context.Context) error {
	var raw map[string]odAbility
	if err := e.fetchJSON(ctx, e.abilitiesURL, &raw); err != nil {
		return fmt.Errorf("fetch abilities: %w", err)
	}

	keyToID := make(map[string]int)
	if e.abilityIDsURL != "" {
		var idsByID map[string]string
		if err := e.fetchJSON(ctx, e.abilityIDsURL, &idsByID); err != nil {
			e.log.Warn("fetch ability_ids failed; continuing without ids", "error", err)
		} else {
			for idStr, key := range idsByID {
				if strings.Contains(idStr, ",") {
					continue
				}
				var id int
				if _, err := fmt.Sscanf(idStr, "%d", &id); err == nil && id > 0 {
					keyToID[key] = id
				}
			}
		}
	}

	refs := make([]postgres.AbilityRef, 0, len(raw))
	for key, a := range raw {
		if key == "special_bonus_attributes" ||
			strings.HasPrefix(key, "dota_base") ||
			strings.HasPrefix(key, "dota_empty") {
			continue
		}

		isTalent := strings.HasPrefix(key, "special_bonus_")
		if isTalent && a.DName == "" {
			continue
		}

		behaviorJSON := normalizeStringOrArray(a.Behavior)
		targetTeamStr := normalizeTargetTeam(a.TargetTeam)

		ref := postgres.AbilityRef{
			Key:         key,
			DName:       a.DName,
			Behavior:    behaviorJSON,
			TargetTeam:  targetTeamStr,
			Description: a.Description,
			Img:         a.Img,
			ManaCost:    flattenScalar(a.ManaCost),
			Cooldown:    flattenScalar(a.Cooldown),
			Attrib:      a.Attrib,
			IsTalent:    isTalent,
		}
		if id, ok := keyToID[key]; ok {
			ref.ID = &id
		}
		refs = append(refs, ref)
	}

	if err := e.repo.UpsertAbilities(ctx, refs); err != nil {
		return err
	}

	talentCount, abilityCount := 0, 0
	for _, r := range refs {
		if r.IsTalent {
			talentCount++
		} else {
			abilityCount++
		}
	}

	e.log.Info("enriched abilities",
		"total", len(refs),
		"abilities", abilityCount,
		"talents", talentCount,
		"with_ids", countAbilityIDs(refs),
	)
	return nil
}

func normalizeStringOrArray(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var arr []string
	if err := json.Unmarshal(raw, &arr); err == nil {
		out, _ := json.Marshal(arr)
		return out
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		out, _ := json.Marshal([]string{s})
		return out
	}
	return raw
}

func flattenScalar(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return strings.Trim(string(raw), `"`)
}

func normalizeTargetTeam(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var arr []string
	if err := json.Unmarshal(raw, &arr); err == nil && len(arr) > 0 {
		return strings.Join(arr, ", ")
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return strings.Trim(string(raw), `"`)
}

func countAbilityIDs(refs []postgres.AbilityRef) int {
	n := 0
	for _, r := range refs {
		if r.ID != nil {
			n++
		}
	}
	return n
}

type odHeroStats struct {
	ID              int16   `json:"id"`
	BaseHealth      int     `json:"base_health"`
	BaseMana        int     `json:"base_mana"`
	BaseArmor       float32 `json:"base_armor"`
	BaseMR          float32 `json:"base_mr"`
	BaseAttackMin   int16   `json:"base_attack_min"`
	BaseAttackMax   int16   `json:"base_attack_max"`
	BaseStr         int16   `json:"base_str"`
	BaseAgi         int16   `json:"base_agi"`
	BaseInt         int16   `json:"base_int"`
	StrGain         float32 `json:"str_gain"`
	AgiGain         float32 `json:"agi_gain"`
	IntGain         float32 `json:"int_gain"`
	AttackRange     int16   `json:"attack_range"`
	ProjectileSpeed int16   `json:"projectile_speed"`
	AttackRate      float32 `json:"attack_rate"`
	MoveSpeed       int16   `json:"move_speed"`
	TurnRate        *float32 `json:"turn_rate"`
	CMEnabled       bool    `json:"cm_enabled"`

	TurboPicks int `json:"turbo_picks"`
	TurboWins  int `json:"turbo_wins"`
	ProPick    int `json:"pro_pick"`
	ProWin     int `json:"pro_win"`
	ProBan     int `json:"pro_ban"`
	PubPick    int `json:"pub_pick"`
	PubWin     int `json:"pub_win"`
}

func (e *Enricher) enrichHeroStats(ctx context.Context) error {
	if e.heroStatsURL == "" {
		e.log.Warn("hero_stats URL not configured, skipping")
		return nil
	}

	var stats []odHeroStats
	if err := e.fetchJSON(ctx, e.heroStatsURL, &stats); err != nil {
		return fmt.Errorf("fetch hero stats: %w", err)
	}

	refs := make([]postgres.HeroStatsRef, 0, len(stats))
	for _, s := range stats {
		if s.ID == 0 {
			continue
		}

		var pubWR, proWR float32
		if s.PubPick > 0 {
			pubWR = float32(s.PubWin) / float32(s.PubPick)
		}
		if s.ProPick > 0 {
			proWR = float32(s.ProWin) / float32(s.ProPick)
		}

		refs = append(refs, postgres.HeroStatsRef{
			ID:              s.ID,
			BaseHealth:      s.BaseHealth,
			BaseMana:        s.BaseMana,
			BaseArmor:       s.BaseArmor,
			BaseMR:          s.BaseMR,
			BaseAttackMin:   s.BaseAttackMin,
			BaseAttackMax:   s.BaseAttackMax,
			BaseStr:         s.BaseStr,
			BaseAgi:         s.BaseAgi,
			BaseInt:         s.BaseInt,
			StrGain:         s.StrGain,
			AgiGain:         s.AgiGain,
			IntGain:         s.IntGain,
			AttackRange:     s.AttackRange,
			ProjectileSpeed: s.ProjectileSpeed,
			AttackRate:      s.AttackRate,
			MoveSpeed:       s.MoveSpeed,
			TurnRate:        s.TurnRate,
			CMEnabled:       s.CMEnabled,
			TurboPicks:      s.TurboPicks,
			TurboWins:       s.TurboWins,
			ProPicks:        s.ProPick,
			ProWins:         s.ProWin,
			ProBans:         s.ProBan,
			PubPicks:        s.PubPick,
			PubWins:         s.PubWin,
			PubWinRate:      pubWR,
			ProWinRate:      proWR,
		})
	}

	if err := e.repo.UpsertHeroStats(ctx, refs); err != nil {
		return fmt.Errorf("upsert hero_stats: %w", err)
	}

	e.log.Info("enriched hero stats", "count", len(refs))
	return nil
}
