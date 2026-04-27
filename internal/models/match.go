package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// =====================================================================
// PlayerSlot — typed alias with convenience helpers.
// Slots 0..127 are Radiant, 128..255 are Dire.
// =====================================================================

type PlayerSlot int16

// IsRadiant reports whether this slot belongs to the Radiant side.
func (s PlayerSlot) IsRadiant() bool { return s < 128 }

// TeamIndex returns 0 for Radiant, 1 for Dire.
func (s PlayerSlot) TeamIndex() int16 {
	if s < 128 {
		return 0
	}
	return 1
}

// =====================================================================
// Match — top-level /api/matches/{id} response.
//
// Design:
//   - Pointers for all nullable/optional fields (distinguish unset vs zero).
//   - json.RawMessage for cold nested objects that go straight into JSONB
//     columns (zero-copy into pgx).
//   - Integer widths (int16/int32) match the SQL schema exactly.
// =====================================================================

type Match struct {
	MatchID               int64   `json:"match_id"`
	MatchSeqNum           *int64  `json:"match_seq_num,omitempty"`
	StartTime             int64   `json:"start_time"`
	Duration              int32   `json:"duration"`
	RadiantWin            bool    `json:"radiant_win"`
	TowerStatusRadiant    *int16  `json:"tower_status_radiant,omitempty"`
	TowerStatusDire       *int16  `json:"tower_status_dire,omitempty"`
	BarracksStatusRadiant *int16  `json:"barracks_status_radiant,omitempty"`
	BarracksStatusDire    *int16  `json:"barracks_status_dire,omitempty"`
	RadiantScore          *int16  `json:"radiant_score,omitempty"`
	DireScore             *int16  `json:"dire_score,omitempty"`
	FirstBloodTime        *int32  `json:"first_blood_time,omitempty"`
	LobbyType             *int16  `json:"lobby_type,omitempty"`
	GameMode              *int16  `json:"game_mode,omitempty"`
	Cluster               *int16  `json:"cluster,omitempty"`
	Engine                *int16  `json:"engine,omitempty"`
	HumanPlayers          *int16  `json:"human_players,omitempty"`
	Version               *int16  `json:"version,omitempty"` // non-nil ⇔ replay parsed
	PatchID               *int16  `json:"patch,omitempty"`   // API field is "patch"
	PositiveVotes         *int32  `json:"positive_votes,omitempty"`
	NegativeVotes         *int32  `json:"negative_votes,omitempty"`
	LeagueID              *int32  `json:"leagueid,omitempty"`
	SeriesID              *int32  `json:"series_id,omitempty"`
	SeriesType            *int16  `json:"series_type,omitempty"`
	RadiantTeamID         *int64  `json:"radiant_team_id,omitempty"`
	DireTeamID            *int64  `json:"dire_team_id,omitempty"`
	RadiantCaptain        *int64  `json:"radiant_captain,omitempty"`
	DireCaptain           *int64  `json:"dire_captain,omitempty"`
	ReplaySalt            *int64  `json:"replay_salt,omitempty"`
	ReplayURL             *string `json:"replay_url,omitempty"`

	// Nested collections
	Players        []MatchPlayer   `json:"players"`
	PicksBans      []PicksBan      `json:"picks_bans,omitempty"`
	DraftTimings   []DraftTiming   `json:"draft_timings,omitempty"`
	Objectives     []Objective     `json:"objectives,omitempty"`
	Chat           []ChatEvent     `json:"chat,omitempty"`
	Teamfights     []Teamfight     `json:"teamfights,omitempty"`
	RadiantGoldAdv []int32         `json:"radiant_gold_adv,omitempty"`
	RadiantXPAdv   []int32         `json:"radiant_xp_adv,omitempty"`
	Cosmetics      json.RawMessage `json:"cosmetics,omitempty"`
}

// IsParsed reports whether the match has extended stats from replay parsing.
// OpenDota populates `version` only for parsed matches.
func (m *Match) IsParsed() bool { return m.Version != nil }

// Validate performs cheap structural checks before hitting the DB. Catches
// malformed API responses early and surfaces clearer errors than a COPY failure.
func (m *Match) Validate() error {
	if m.MatchID <= 0 {
		return fmt.Errorf("match_id must be positive, got %d", m.MatchID)
	}
	if m.StartTime <= 0 {
		return fmt.Errorf("start_time must be positive, got %d", m.StartTime)
	}
	if m.Duration < 0 {
		return fmt.Errorf("duration must be non-negative, got %d", m.Duration)
	}
	// OpenDota matches typically have 10 players, but bot/practice matches
	// and abandoned matches may have fewer. Allow 0-10 players.
	if n := len(m.Players); n > 10 {
		return fmt.Errorf("expected at most 10 players, got %d", n)
	}
	// Detect duplicate player_slot within a match (would violate PK).
	seen := make(map[int16]struct{}, len(m.Players))
	for i := range m.Players {
		slot := m.Players[i].PlayerSlot
		if _, dup := seen[slot]; dup {
			return fmt.Errorf("duplicate player_slot %d", slot)
		}
		seen[slot] = struct{}{}
	}
	return nil
}

// =====================================================================
// MatchPlayer — maps to player_matches (hot) + player_match_details (cold).
// =====================================================================

type MatchPlayer struct {
	// Identity
	AccountID   *int64 `json:"account_id,omitempty"`
	PlayerSlot  int16  `json:"player_slot"`
	HeroID      int16  `json:"hero_id"`
	HeroVariant *int16 `json:"hero_variant,omitempty"`

	// Core stats
	Kills       int16  `json:"kills"`
	Deaths      int16  `json:"deaths"`
	Assists     int16  `json:"assists"`
	Level       *int16 `json:"level,omitempty"`
	NetWorth    *int32 `json:"net_worth,omitempty"`
	Gold        *int32 `json:"gold,omitempty"`
	GoldSpent   *int32 `json:"gold_spent,omitempty"`
	GoldPerMin  *int16 `json:"gold_per_min,omitempty"`
	XPPerMin    *int16 `json:"xp_per_min,omitempty"`
	LastHits    *int16 `json:"last_hits,omitempty"`
	Denies      *int16 `json:"denies,omitempty"`
	HeroDamage  *int32 `json:"hero_damage,omitempty"`
	TowerDamage *int32 `json:"tower_damage,omitempty"`
	HeroHealing *int32 `json:"hero_healing,omitempty"`

	// Items
	Item0       *int32  `json:"item_0,omitempty"`
	Item1       *int32  `json:"item_1,omitempty"`
	Item2       *int32  `json:"item_2,omitempty"`
	Item3       *int32  `json:"item_3,omitempty"`
	Item4       *int32  `json:"item_4,omitempty"`
	Item5       *int32  `json:"item_5,omitempty"`
	ItemNeutral *int32  `json:"item_neutral,omitempty"`
	Backpack0   *int32  `json:"backpack_0,omitempty"`
	Backpack1   *int32  `json:"backpack_1,omitempty"`
	Backpack2   *int32  `json:"backpack_2,omitempty"`
	Backpack3   *int32  `json:"backpack_3,omitempty"`
	FinalItems  []int32 `json:"final_items,omitempty"`

	// Lane / role
	Lane      *int16 `json:"lane,omitempty"`
	LaneRole  *int16 `json:"lane_role,omitempty"`
	IsRoaming *bool  `json:"is_roaming,omitempty"`
	PartyID   *int32 `json:"party_id,omitempty"`
	PartySize *int16 `json:"party_size,omitempty"`

	// Advanced (parsed-only)
	Stuns                  *float32 `json:"stuns,omitempty"`
	ObsPlaced              *int16   `json:"obs_placed,omitempty"`
	SenPlaced              *int16   `json:"sen_placed,omitempty"`
	CreepsStacked          *int16   `json:"creeps_stacked,omitempty"`
	CampsStacked           *int16   `json:"camps_stacked,omitempty"`
	RunePickups            *int16   `json:"rune_pickups,omitempty"`
	FirstbloodClaimed      *IntBool `json:"firstblood_claimed,omitempty"`
	TeamfightParticipation *float32 `json:"teamfight_participation,omitempty"`
	TowersKilled           *int16   `json:"towers_killed,omitempty"`
	RoshansKilled          *int16   `json:"roshans_killed,omitempty"`
	ObserversPlaced        *int16   `json:"observers_placed,omitempty"`
	LeaverStatus           *int16   `json:"leaver_status,omitempty"`

	// Per-minute timeseries (denormalized arrays from the API).
	// Expanded into player_timeseries rows via BuildPlayerTimeseries.
	Times []int32 `json:"times,omitempty"`
	GoldT []int32 `json:"gold_t,omitempty"`
	XPT   []int32 `json:"xp_t,omitempty"`
	LHT   []int32 `json:"lh_t,omitempty"`
	DNT   []int32 `json:"dn_t,omitempty"`

	// Ability upgrade order
	AbilityUpgradesArr []int32 `json:"ability_upgrades_arr,omitempty"`

	// Cold JSONB blobs — written verbatim to player_match_details.
	Damage                  json.RawMessage `json:"damage,omitempty"`
	DamageTaken             json.RawMessage `json:"damage_taken,omitempty"`
	DamageInflictor         json.RawMessage `json:"damage_inflictor,omitempty"`
	DamageInflictorReceived json.RawMessage `json:"damage_inflictor_received,omitempty"`
	DamageTargets           json.RawMessage `json:"damage_targets,omitempty"`
	HeroHits                json.RawMessage `json:"hero_hits,omitempty"`
	MaxHeroHit              json.RawMessage `json:"max_hero_hit,omitempty"`
	AbilityUses             json.RawMessage `json:"ability_uses,omitempty"`
	AbilityTargets          json.RawMessage `json:"ability_targets,omitempty"`
	ItemUses                json.RawMessage `json:"item_uses,omitempty"`
	GoldReasons             json.RawMessage `json:"gold_reasons,omitempty"`
	XPReasons               json.RawMessage `json:"xp_reasons,omitempty"`
	Killed                  json.RawMessage `json:"killed,omitempty"`
	KilledBy                json.RawMessage `json:"killed_by,omitempty"`
	KillStreaks             json.RawMessage `json:"kill_streaks,omitempty"`
	MultiKills              json.RawMessage `json:"multi_kills,omitempty"`
	LifeState               json.RawMessage `json:"life_state,omitempty"`
	LanePos                 json.RawMessage `json:"lane_pos,omitempty"`
	Obs                     json.RawMessage `json:"obs,omitempty"`
	Sen                     json.RawMessage `json:"sen,omitempty"`
	Actions                 json.RawMessage `json:"actions,omitempty"`
	Pings                   json.RawMessage `json:"pings,omitempty"`
	Runes                   json.RawMessage `json:"runes,omitempty"`
	Purchase                json.RawMessage `json:"purchase,omitempty"`
	ObsLog                  json.RawMessage `json:"obs_log,omitempty"`
	SenLog                  json.RawMessage `json:"sen_log,omitempty"`
	ObsLeftLog              json.RawMessage `json:"obs_left_log,omitempty"`
	SenLeftLog              json.RawMessage `json:"sen_left_log,omitempty"`
	PurchaseLog             json.RawMessage `json:"purchase_log,omitempty"`
	KillsLog                json.RawMessage `json:"kills_log,omitempty"`
	BuybackLog              json.RawMessage `json:"buyback_log,omitempty"`
	RunesLog                json.RawMessage `json:"runes_log,omitempty"`
	ConnectionLog           json.RawMessage `json:"connection_log,omitempty"`
	PermanentBuffs          json.RawMessage `json:"permanent_buffs,omitempty"`
	NeutralTokensLog        json.RawMessage `json:"neutral_tokens_log,omitempty"`
	NeutralItemHistory      json.RawMessage `json:"neutral_item_history,omitempty"`
	AdditionalUnits         json.RawMessage `json:"additional_units,omitempty"`
}

// Slot returns the player's slot as a typed PlayerSlot.
func (p *MatchPlayer) Slot() PlayerSlot { return PlayerSlot(p.PlayerSlot) }

// IsRadiantSide reports whether the player is on Radiant.
func (p *MatchPlayer) IsRadiantSide() bool { return p.Slot().IsRadiant() }

// Won returns true if the player was on the winning side.
// Caller must pass Match.RadiantWin.
func (p *MatchPlayer) Won(radiantWin bool) bool { return p.IsRadiantSide() == radiantWin }

// =====================================================================
// Draft / objectives / chat / teamfights
// =====================================================================

// PicksBan is a single draft action.
// NOTE: the Order field maps to the SQL column "ord" (since "order" is reserved).
type PicksBan struct {
	IsPick bool  `json:"is_pick"`
	HeroID int16 `json:"hero_id"`
	Team   int16 `json:"team"` // 0 = Radiant, 1 = Dire
	Order  int16 `json:"order"`
}

// DraftTiming tracks time spent per draft step (parsed matches only).
type DraftTiming struct {
	Order          int16  `json:"order"`
	Pick           *bool  `json:"pick,omitempty"`
	ActiveTeam     *int16 `json:"active_team,omitempty"`
	HeroID         *int16 `json:"hero_id,omitempty"`
	PlayerSlot     *int16 `json:"player_slot,omitempty"`
	ExtraTime      *int32 `json:"extra_time,omitempty"`
	TotalTimeTaken *int32 `json:"total_time_taken,omitempty"`
}

// Objective represents a match event (kill, tower fall, aegis pickup, etc.).
// The API occasionally sends `key` as an integer for certain event types;
// we normalize it to *string here.
type Objective struct {
	Time       int32   `json:"time"`
	Type       string  `json:"type"`
	Slot       *int16  `json:"slot,omitempty"`
	PlayerSlot *int16  `json:"player_slot,omitempty"`
	Team       *int16  `json:"team,omitempty"`
	Key        *string `json:"-"` // populated by custom UnmarshalJSON
	Value      *int32  `json:"value,omitempty"`
	Unit       *string `json:"unit,omitempty"`
}

// UnmarshalJSON handles the polymorphic `key` field (string OR integer).
// Fails loudly on unsupported types so API drift is caught immediately.
func (o *Objective) UnmarshalJSON(data []byte) error {
	type alias Objective
	aux := struct {
		Key json.RawMessage `json:"key,omitempty"`
		*alias
	}{alias: (*alias)(o)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if len(aux.Key) == 0 || string(aux.Key) == "null" {
		return nil
	}

	// Try string first (most common case).
	var s string
	if err := json.Unmarshal(aux.Key, &s); err == nil {
		o.Key = &s
		return nil
	}
	// Fall back to integer.
	var n int64
	if err := json.Unmarshal(aux.Key, &n); err == nil {
		str := strconv.FormatInt(n, 10)
		o.Key = &str
		return nil
	}
	return fmt.Errorf("objective.key: unsupported JSON type %s", string(aux.Key))
}

// MarshalJSON re-emits `key` as a string (normalized form).
func (o Objective) MarshalJSON() ([]byte, error) {
	type alias Objective
	aux := struct {
		Key *string `json:"key,omitempty"`
		alias
	}{Key: o.Key, alias: alias(o)}
	return json.Marshal(aux)
}

// ChatEvent is an in-game chat / system message.
type ChatEvent struct {
	Time       int32   `json:"time"`
	Type       *string `json:"type,omitempty"`
	PlayerSlot *int16  `json:"player_slot,omitempty"`
	Unit       *string `json:"unit,omitempty"`
	Key        *string `json:"key,omitempty"`
}

// Teamfight is a detected team engagement summary. Players is kept raw
// because its per-participant delta schema is variable across API versions.
type Teamfight struct {
	Start     int32           `json:"start"`
	End       int32           `json:"end"`
	LastDeath int32           `json:"last_death"`
	Deaths    int16           `json:"deaths"`
	Players   json.RawMessage `json:"players,omitempty"`
}

// =====================================================================
// Derived: player_timeseries rows
// =====================================================================

// PlayerTimeseries is a single per-minute sample for the player_timeseries table.
type PlayerTimeseries struct {
	MatchID    int64
	PlayerSlot int16
	Minute     int16
	HeroID     int16
	AccountID  *int64
	PatchID    *int16
	Gold       *int32
	XP         *int32
	LH         *int16
	DN         *int16
}

// ErrNonMonotonicTimes is returned when a player's `times` array is not
// strictly increasing after the first sample. The caller decides whether to
// skip the series or fail the whole match.
var ErrNonMonotonicTimes = errors.New("player timeseries: non-monotonic times array")

// BuildPlayerTimeseries expands per-minute arrays into flat rows for the
// normalized player_timeseries table.
//
// Rules:
//   - Returns nil if the player has no timeseries (unparsed match).
//   - Skips negative sample times (OpenDota sometimes emits pre-horn t=-90).
//   - Enforces monotonically increasing minutes; duplicates would violate the
//     (match_id, player_slot, minute) primary key.
//   - Truncates to the shortest array length among times/gold_t/xp_t to avoid
//     index-out-of-range on malformed payloads.
func BuildPlayerTimeseries(matchID int64, patchID *int16, p *MatchPlayer) ([]PlayerTimeseries, error) {
	n := len(p.Times)
	if n == 0 {
		return nil, nil
	}

	// Determine the safe iteration length (shortest array wins).
	minLen := n
	if l := len(p.GoldT); l > 0 && l < minLen {
		minLen = l
	}
	if l := len(p.XPT); l > 0 && l < minLen {
		minLen = l
	}

	out := make([]PlayerTimeseries, 0, minLen)
	var lastMinute int16 = -1

	for i := 0; i < minLen; i++ {
		t := p.Times[i]
		if t < 0 {
			// Skip pre-horn samples; they'd collide with minute 0.
			continue
		}
		minute := int16(t / 60)
		if minute <= lastMinute {
			return nil, fmt.Errorf("%w: slot=%d at index %d (minute %d <= last %d)",
				ErrNonMonotonicTimes, p.PlayerSlot, i, minute, lastMinute)
		}
		lastMinute = minute

		row := PlayerTimeseries{
			MatchID:    matchID,
			PlayerSlot: p.PlayerSlot,
			Minute:     minute,
			HeroID:     p.HeroID,
			AccountID:  p.AccountID,
			PatchID:    patchID,
		}
		if i < len(p.GoldT) {
			v := p.GoldT[i]
			row.Gold = &v
		}
		if i < len(p.XPT) {
			v := p.XPT[i]
			row.XP = &v
		}
		if i < len(p.LHT) {
			v := int16(p.LHT[i])
			row.LH = &v
		}
		if i < len(p.DNT) {
			v := int16(p.DNT[i])
			row.DN = &v
		}
		out = append(out, row)
	}
	return out, nil
}

type IntBool bool

func (ib *IntBool) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	// Try boolean
	var b bool
	if err := json.Unmarshal(data, &b); err == nil {
		*ib = IntBool(b)
		return nil
	}
	// Try integer
	var i int
	if err := json.Unmarshal(data, &i); err == nil {
		*ib = IntBool(i != 0)
		return nil
	}
	return errors.New("cannot unmarshal to IntBool")
}
