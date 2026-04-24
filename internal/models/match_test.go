package models

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------
// Fixture loading
// ---------------------------------------------------------------------

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	// Resolve relative to this test file so `go test` works from any CWD.
	path := filepath.Join("..", "..", "testdata", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

// ---------------------------------------------------------------------
// Match — unmarshal & structural tests
// ---------------------------------------------------------------------

func TestMatch_Unmarshal_Core(t *testing.T) {
	data := loadFixture(t, "match_sample.json")

	var m Match
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if m.MatchID == 0 {
		t.Error("MatchID = 0")
	}
	if m.StartTime == 0 {
		t.Error("StartTime = 0")
	}
	if m.Duration == 0 {
		t.Error("Duration = 0")
	}
	if len(m.Players) == 0 {
		t.Error("Players is empty")
	}
}

func TestMatch_IsParsed(t *testing.T) {
	v := int16(20)
	tests := []struct {
		name    string
		version *int16
		want    bool
	}{
		{"parsed match has version", &v, true},
		{"unparsed match has nil version", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := Match{Version: tc.version}
			if got := m.IsParsed(); got != tc.want {
				t.Errorf("IsParsed() = %v, want %v", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------
// Match.Validate
// ---------------------------------------------------------------------

func TestMatch_Validate(t *testing.T) {
	mkPlayers := func(slots ...int16) []MatchPlayer {
		out := make([]MatchPlayer, len(slots))
		for i, s := range slots {
			out[i] = MatchPlayer{PlayerSlot: s, HeroID: 1}
		}
		return out
	}

	tenSlots := []int16{0, 1, 2, 3, 4, 128, 129, 130, 131, 132}

	tests := []struct {
		name    string
		m       Match
		wantErr bool
	}{
		{
			name: "valid full match",
			m: Match{
				MatchID:   7800000000,
				StartTime: 1710000000,
				Duration:  2400,
				Players:   mkPlayers(tenSlots...),
			},
			wantErr: false,
		},
		{
			name: "valid empty-players match",
			m: Match{
				MatchID:   7800000000,
				StartTime: 1710000000,
				Duration:  2400,
			},
			wantErr: false,
		},
		{
			name:    "zero match_id",
			m:       Match{MatchID: 0, StartTime: 1710000000, Duration: 2400},
			wantErr: true,
		},
		{
			name:    "negative match_id",
			m:       Match{MatchID: -1, StartTime: 1710000000, Duration: 2400},
			wantErr: true,
		},
		{
			name:    "zero start_time",
			m:       Match{MatchID: 1, StartTime: 0, Duration: 2400},
			wantErr: true,
		},
		{
			name:    "negative duration",
			m:       Match{MatchID: 1, StartTime: 1710000000, Duration: -1},
			wantErr: true,
		},
		{
			name: "wrong player count",
			m: Match{
				MatchID:   1,
				StartTime: 1710000000,
				Duration:  2400,
				Players:   mkPlayers(0, 1, 2),
			},
			wantErr: true,
		},
		{
			name: "duplicate player_slot",
			m: Match{
				MatchID:   1,
				StartTime: 1710000000,
				Duration:  2400,
				Players:   mkPlayers(0, 0, 2, 3, 4, 128, 129, 130, 131, 132),
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.m.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() err=%v, wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------
// PlayerSlot & MatchPlayer helpers
// ---------------------------------------------------------------------

func TestPlayerSlot_Helpers(t *testing.T) {
	tests := []struct {
		slot        PlayerSlot
		wantRadiant bool
		wantTeam    int16
	}{
		{0, true, 0},
		{4, true, 0},
		{127, true, 0},
		{128, false, 1},
		{132, false, 1},
		{255, false, 1},
	}
	for _, tc := range tests {
		if got := tc.slot.IsRadiant(); got != tc.wantRadiant {
			t.Errorf("slot=%d IsRadiant=%v, want %v", tc.slot, got, tc.wantRadiant)
		}
		if got := tc.slot.TeamIndex(); got != tc.wantTeam {
			t.Errorf("slot=%d TeamIndex=%d, want %d", tc.slot, got, tc.wantTeam)
		}
	}
}

func TestMatchPlayer_SideAndWon(t *testing.T) {
	tests := []struct {
		slot        int16
		radiantWin  bool
		wantRadiant bool
		wantWon     bool
	}{
		{0, true, true, true},
		{4, true, true, true},
		{127, true, true, true},
		{128, true, false, false},
		{132, false, false, true},
		{128, false, false, true},
		{0, false, true, false},
	}
	for _, tc := range tests {
		p := MatchPlayer{PlayerSlot: tc.slot}
		if got := p.IsRadiantSide(); got != tc.wantRadiant {
			t.Errorf("slot=%d IsRadiantSide=%v, want %v", tc.slot, got, tc.wantRadiant)
		}
		if got := p.Won(tc.radiantWin); got != tc.wantWon {
			t.Errorf("slot=%d radiantWin=%v Won=%v, want %v",
				tc.slot, tc.radiantWin, got, tc.wantWon)
		}
	}
}

// ---------------------------------------------------------------------
// JSON round-trip: raw JSONB fields preserved
// ---------------------------------------------------------------------

func TestMatch_RoundTrip_PreservesRawJSONB(t *testing.T) {
	data := loadFixture(t, "match_sample.json")

	var m Match
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(m.Players) == 0 {
		t.Fatal("no players in fixture")
	}

	// Every non-empty RawMessage must be syntactically valid JSON.
	for i := range m.Players {
		p := &m.Players[i]
		raws := map[string]json.RawMessage{
			"damage":       p.Damage,
			"purchase_log": p.PurchaseLog,
			"kills_log":    p.KillsLog,
			"ability_uses": p.AbilityUses,
		}
		for field, raw := range raws {
			if len(raw) == 0 {
				continue
			}
			var v any
			if err := json.Unmarshal(raw, &v); err != nil {
				t.Errorf("player[%d].%s not valid JSON: %v", i, field, err)
			}
		}
	}
}

// ---------------------------------------------------------------------
// Objective — polymorphic `key` handling
// ---------------------------------------------------------------------

func TestObjective_UnmarshalKey(t *testing.T) {
	tests := []struct {
		name     string
		payload  string
		wantKey  *string
		wantErr  bool
	}{
		{
			name:    "string key",
			payload: `{"time":100,"type":"CHAT_MESSAGE_TOWER_KILL","key":"npc_dota_badguys_tower1_top"}`,
			wantKey: strPtr("npc_dota_badguys_tower1_top"),
		},
		{
			name:    "int key",
			payload: `{"time":200,"type":"CHAT_MESSAGE_FIRSTBLOOD","key":12345}`,
			wantKey: strPtr("12345"),
		},
		{
			name:    "negative int key",
			payload: `{"time":300,"type":"X","key":-7}`,
			wantKey: strPtr("-7"),
		},
		{
			name:    "null key",
			payload: `{"time":400,"type":"X","key":null}`,
			wantKey: nil,
		},
		{
			name:    "missing key",
			payload: `{"time":500,"type":"X"}`,
			wantKey: nil,
		},
		{
			name:    "unsupported type (object)",
			payload: `{"time":600,"type":"X","key":{"nested":true}}`,
			wantErr: true,
		},
		{
			name:    "unsupported type (array)",
			payload: `{"time":700,"type":"X","key":[1,2]}`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var obj Objective
			err := json.Unmarshal([]byte(tc.payload), &obj)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			switch {
			case tc.wantKey == nil && obj.Key != nil:
				t.Errorf("expected nil key, got %q", *obj.Key)
			case tc.wantKey != nil && obj.Key == nil:
				t.Errorf("expected %q, got nil", *tc.wantKey)
			case tc.wantKey != nil && *tc.wantKey != *obj.Key:
				t.Errorf("key = %q, want %q", *obj.Key, *tc.wantKey)
			}
		})
	}
}

func TestObjective_MarshalRoundTrip(t *testing.T) {
	// After unmarshaling an int key, re-marshaling should emit it as a string.
	in := `{"time":200,"type":"X","key":12345}`
	var obj Objective
	if err := json.Unmarshal([]byte(in), &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	out, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back Objective
	if err := json.Unmarshal(out, &back); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if back.Key == nil || *back.Key != "12345" {
		t.Errorf("round-trip key lost; got %v", back.Key)
	}
}

// ---------------------------------------------------------------------
// BuildPlayerTimeseries
// ---------------------------------------------------------------------

func TestBuildPlayerTimeseries_Happy(t *testing.T) {
	patch := int16(55)
	acct := int64(42)
	p := &MatchPlayer{
		AccountID:  &acct,
		PlayerSlot: 3,
		HeroID:     11,
		Times:      []int32{0, 60, 120, 180},
		GoldT:      []int32{100, 250, 500, 900},
		XPT:        []int32{0, 150, 420, 800},
		LHT:        []int32{0, 5, 12, 20},
		DNT:        []int32{0, 1, 2, 4},
	}

	rows, err := BuildPlayerTimeseries(7800000000, &patch, p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	for i, r := range rows {
		wantMinute := int16(i)
		if r.Minute != wantMinute {
			t.Errorf("row %d: minute=%d, want %d", i, r.Minute, wantMinute)
		}
		if r.HeroID != 11 {
			t.Errorf("row %d: hero_id=%d, want 11", i, r.HeroID)
		}
		if r.PatchID == nil || *r.PatchID != 55 {
			t.Errorf("row %d: patch_id=%v, want 55", i, r.PatchID)
		}
		if r.AccountID == nil || *r.AccountID != 42 {
			t.Errorf("row %d: account_id=%v, want 42", i, r.AccountID)
		}
	}
}

func TestBuildPlayerTimeseries_Empty(t *testing.T) {
	p := &MatchPlayer{PlayerSlot: 0, HeroID: 1}
	rows, err := BuildPlayerTimeseries(1, nil, p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rows != nil {
		t.Errorf("expected nil, got %d rows", len(rows))
	}
}

func TestBuildPlayerTimeseries_SkipsNegativeTimes(t *testing.T) {
	// OpenDota sometimes emits a pre-horn sample at t=-90.
	p := &MatchPlayer{
		PlayerSlot: 0,
		HeroID:     1,
		Times:      []int32{-90, 0, 60, 120},
		GoldT:      []int32{0, 100, 200, 300},
		XPT:        []int32{0, 50, 150, 250},
	}
	rows, err := BuildPlayerTimeseries(1, nil, p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (negative skipped), got %d", len(rows))
	}
	if rows[0].Minute != 0 || rows[1].Minute != 1 || rows[2].Minute != 2 {
		t.Errorf("minutes = [%d,%d,%d], want [0,1,2]",
			rows[0].Minute, rows[1].Minute, rows[2].Minute)
	}
}

func TestBuildPlayerTimeseries_NonMonotonic(t *testing.T) {
	p := &MatchPlayer{
		PlayerSlot: 0,
		HeroID:     1,
		Times:      []int32{0, 60, 60}, // duplicate minute 1
		GoldT:      []int32{0, 100, 200},
		XPT:        []int32{0, 50, 100},
	}
	_, err := BuildPlayerTimeseries(1, nil, p)
	if err == nil {
		t.Fatal("expected ErrNonMonotonicTimes, got nil")
	}
	if !errors.Is(err, ErrNonMonotonicTimes) {
		t.Errorf("expected ErrNonMonotonicTimes, got %v", err)
	}
}

func TestBuildPlayerTimeseries_MismatchedArrayLengths(t *testing.T) {
	// GoldT shorter than Times — should truncate to shortest, no panic.
	p := &MatchPlayer{
		PlayerSlot: 0,
		HeroID:     1,
		Times:      []int32{0, 60, 120, 180},
		GoldT:      []int32{100, 200},
		XPT:        []int32{0, 50, 100, 150},
	}
	rows, err := BuildPlayerTimeseries(1, nil, p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (min of arrays), got %d", len(rows))
	}
	if rows[0].Gold == nil || *rows[0].Gold != 100 {
		t.Errorf("row 0 gold=%v, want 100", rows[0].Gold)
	}
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

func strPtr(s string) *string { return &s }