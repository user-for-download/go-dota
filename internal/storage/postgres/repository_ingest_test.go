package postgres

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/user-for-download/go-dota/internal/models"
)

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

func i16(v int16) *int16 { return &v }
func i32(v int32) *int32 { return &v }
func i64(v int64) *int64 { return &v }

// baseMatch returns a minimal but complete 10-player match for tests.
func baseMatch(id int64, startTime int64) *models.Match {
	m := &models.Match{
		MatchID:    id,
		StartTime:  startTime,
		Duration:   2400,
		RadiantWin: true,
		PatchID:    i16(55),
		LobbyType:  i16(7),
		GameMode:   i16(22),
	}
	slots := []int16{0, 1, 2, 3, 4, 128, 129, 130, 131, 132}
	for i, s := range slots {
		acc := int64(1000 + i)
		m.Players = append(m.Players, models.MatchPlayer{
			AccountID:  &acc,
			PlayerSlot: s,
			HeroID:     int16(i + 1),
			Kills:      int16(i),
			Deaths:     1,
			Assists:    2,
		})
	}
	return m
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

func TestIngestMatch_Minimal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)
	require.NoError(t, repo.Migrate(ctx))

	m := baseMatch(7800000001, 1710000001)
	require.NoError(t, repo.IngestMatch(ctx, m))

	var matches int
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM matches WHERE match_id = $1", m.MatchID,
	).Scan(&matches))
	require.Equal(t, 1, matches)

	var players int
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM player_matches WHERE match_id = $1", m.MatchID,
	).Scan(&players))
	require.Equal(t, 10, players)
}

func TestIngestMatch_Idempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)
	require.NoError(t, repo.Migrate(ctx))

	m := baseMatch(7800000002, 1710000002)
	require.NoError(t, repo.IngestMatch(ctx, m))
	require.NoError(t, repo.IngestMatch(ctx, m))

	var matches, players int
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM matches WHERE match_id = $1", m.MatchID,
	).Scan(&matches))
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM player_matches WHERE match_id = $1", m.MatchID,
	).Scan(&players))

	require.Equal(t, 1, matches, "match row duplicated on re-ingest")
	require.Equal(t, 10, players, "player rows duplicated on re-ingest")
}

func TestIngestMatch_ParsedReplacesUnparsed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)
	require.NoError(t, repo.Migrate(ctx))

	// First: unparsed match.
	m := baseMatch(7800000003, 1710000003)
	require.NoError(t, repo.IngestMatch(ctx, m))

	var isParsed bool
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT is_parsed FROM matches WHERE match_id = $1", m.MatchID,
	).Scan(&isParsed))
	require.False(t, isParsed)

	// Re-ingest as parsed (Version set + timeseries data).
	m.Version = i16(21)
	for i := range m.Players {
		p := &m.Players[i]
		p.Times = []int32{0, 60, 120}
		p.GoldT = []int32{100, 250, 500}
		p.XPT = []int32{0, 150, 420}
	}
	require.NoError(t, repo.IngestMatch(ctx, m))

	require.NoError(t, pool.QueryRow(ctx,
		"SELECT is_parsed FROM matches WHERE match_id = $1", m.MatchID,
	).Scan(&isParsed))
	require.True(t, isParsed, "is_parsed flag not flipped on re-ingest")

	var tsRows int
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM player_timeseries WHERE match_id = $1", m.MatchID,
	).Scan(&tsRows))
	require.Equal(t, 30, tsRows, "expected 10 players * 3 minutes of timeseries")
}

func TestIngestMatch_InvalidTimeseriesDoesNotAbort(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)
	require.NoError(t, repo.Migrate(ctx))

	m := baseMatch(7800000004, 1710000004)
	m.Version = i16(21)

	// Player 0 has non-monotonic times — should be skipped, not fail the tx.
	m.Players[0].Times = []int32{0, 60, 60}
	m.Players[0].GoldT = []int32{0, 100, 200}
	m.Players[0].XPT = []int32{0, 50, 100}

	// Other players have valid timeseries.
	for i := 1; i < len(m.Players); i++ {
		p := &m.Players[i]
		p.Times = []int32{0, 60, 120}
		p.GoldT = []int32{100, 250, 500}
		p.XPT = []int32{0, 150, 420}
	}

	require.NoError(t, repo.IngestMatch(ctx, m),
		"ingest should succeed despite one malformed series")

	var tsRows int
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM player_timeseries WHERE match_id = $1", m.MatchID,
	).Scan(&tsRows))
	require.Equal(t, 27, tsRows, "expected 9 valid players * 3 minutes = 27 rows")
}

func TestIngestMatch_TeamLinksIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)
	require.NoError(t, repo.Migrate(ctx))

	// Pre-seed teams to satisfy any FK constraints.
	require.NoError(t, repo.UpsertTeam(ctx, 111, "Team Radiant", "RAD", ""))
	require.NoError(t, repo.UpsertTeam(ctx, 222, "Team Dire", "DIR", ""))

	m := baseMatch(7800000005, 1710000005)
	m.RadiantTeamID = i64(111)
	m.DireTeamID = i64(222)
	m.LeagueID = i32(16000)

	require.NoError(t, repo.IngestMatch(ctx, m))
	require.NoError(t, repo.IngestMatch(ctx, m)) // re-ingest

	var n int
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM team_matches WHERE match_id = $1", m.MatchID,
	).Scan(&n))
	require.Equal(t, 2, n, "team_matches duplicated on re-ingest")

	// Verify win flags are correct.
	var radiantWin, direWin bool
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT win FROM team_matches WHERE match_id = $1 AND team_id = 111", m.MatchID,
	).Scan(&radiantWin))
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT win FROM team_matches WHERE match_id = $1 AND team_id = 222", m.MatchID,
	).Scan(&direWin))
	require.True(t, radiantWin)
	require.False(t, direWin)
}

func TestIngestMatch_ValidationRejects(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)
	require.NoError(t, repo.Migrate(ctx))

	tests := []struct {
		name string
		m    *models.Match
	}{
		{"zero match_id", &models.Match{MatchID: 0, StartTime: 1710000000, Duration: 2400}},
		{"zero start_time", &models.Match{MatchID: 1, StartTime: 0, Duration: 2400}},
		{"negative duration", &models.Match{MatchID: 1, StartTime: 1710000000, Duration: -1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := repo.IngestMatch(ctx, tc.m)
			require.Error(t, err)
			require.Contains(t, err.Error(), "validate match")
		})
	}
}

func TestIngestMatch_WithRawJSONBFields(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)
	require.NoError(t, repo.Migrate(ctx))

	m := baseMatch(7800000006, 1710000006)
	m.Version = i16(21)
	m.Cosmetics = json.RawMessage(`{"123":{"item_id":123}}`)
	m.Players[0].Damage = json.RawMessage(`{"npc_dota_hero_axe":1500}`)
	m.Players[0].PurchaseLog = json.RawMessage(`[{"time":10,"key":"tango"}]`)

	require.NoError(t, repo.IngestMatch(ctx, m))

	// Verify JSONB round-trip.
	var cosmetics []byte
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT cosmetics FROM match_cosmetics WHERE match_id = $1", m.MatchID,
	).Scan(&cosmetics))
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(cosmetics, &parsed))
	require.Contains(t, parsed, "123")

	var damage []byte
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT damage FROM player_match_details WHERE match_id = $1 AND player_slot = 0",
		m.MatchID,
	).Scan(&damage))
	require.JSONEq(t, `{"npc_dota_hero_axe":1500}`, string(damage))
}

func TestFilterUnknownMatchIDs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)
	require.NoError(t, repo.Migrate(ctx))

	// Seed a few known matches.
	for _, id := range []int64{100, 200, 300} {
		m := baseMatch(id, 1710000000+id)
		require.NoError(t, repo.IngestMatch(ctx, m))
	}

	tests := []struct {
		name    string
		input   []int64
		wantLen int
		wantSet map[int64]bool
	}{
		{
			name:    "all known",
			input:   []int64{100, 200, 300},
			wantLen: 0,
			wantSet: map[int64]bool{},
		},
		{
			name:    "all unknown",
			input:   []int64{101, 201, 301},
			wantLen: 3,
			wantSet: map[int64]bool{101: true, 201: true, 301: true},
		},
		{
			name:    "mixed",
			input:   []int64{100, 101, 200, 999},
			wantLen: 2,
			wantSet: map[int64]bool{101: true, 999: true},
		},
		{
			name:    "empty input",
			input:   nil,
			wantLen: 0,
			wantSet: map[int64]bool{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := repo.FilterUnknownMatchIDs(ctx, tc.input)
			require.NoError(t, err)
			require.Len(t, got, tc.wantLen)
			for _, id := range got {
				require.True(t, tc.wantSet[id], "unexpected id %d", id)
			}
		})
	}
}

func TestIngestMatch_AutoCreatesTeamAndLeagueStubs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)
	require.NoError(t, repo.Migrate(ctx))

	m := baseMatch(7800000007, 1710000007)
	m.RadiantTeamID = i64(777001)
	m.DireTeamID = i64(777002)
	m.LeagueID = i32(999001)

	require.NoError(t, repo.IngestMatch(ctx, m))

	var teamCount int
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM teams WHERE team_id IN (777001, 777002)",
	).Scan(&teamCount))
	require.Equal(t, 2, teamCount)

	var leagueCount int
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM leagues WHERE leagueid = 999001",
	).Scan(&leagueCount))
	require.Equal(t, 1, leagueCount)

	var tmCount int
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM team_matches WHERE match_id = $1", m.MatchID,
	).Scan(&tmCount))
	require.Equal(t, 2, tmCount)
}

func TestIngestMatch_SeededReferenceData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)
	require.NoError(t, repo.Migrate(ctx))

	var heroCount int
	require.NoError(t, pool.QueryRow(ctx, "SELECT COUNT(*) FROM heroes").Scan(&heroCount))
	require.GreaterOrEqual(t, heroCount, 50, "expected >=50 heroes seeded")

	var patchCount int
	require.NoError(t, pool.QueryRow(ctx, "SELECT COUNT(*) FROM patches").Scan(&patchCount))
	require.GreaterOrEqual(t, patchCount, 50, "expected >=50 patches seeded")

	var gameModeCount int
	require.NoError(t, pool.QueryRow(ctx, "SELECT COUNT(*) FROM game_modes").Scan(&gameModeCount))
	require.GreaterOrEqual(t, gameModeCount, 10, "expected >=10 game_modes seeded")

	var lobbyTypeCount int
	require.NoError(t, pool.QueryRow(ctx, "SELECT COUNT(*) FROM lobby_types").Scan(&lobbyTypeCount))
	require.GreaterOrEqual(t, lobbyTypeCount, 4, "expected >=4 lobby_types seeded")
}
