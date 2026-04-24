package postgres

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/user-for-download/go-dota/internal/models"
)

func TestIngestMatch_FromLocalFixture(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)
	require.NoError(t, repo.Migrate(ctx), "migrations must apply cleanly")

	path := filepath.Join("..", "..", "..", "testdata", "match_sample.json")
	data, err := os.ReadFile(path)
	require.NoError(t, err, "read fixture %s", path)
	t.Logf("loaded fixture: %d bytes", len(data))

	var m models.Match
	require.NoError(t, json.Unmarshal(data, &m), "unmarshal fixture")
	require.NoError(t, m.Validate(), "fixture must pass validation")

	t.Logf("match_id=%d parsed=%v players=%d picks_bans=%d",
		m.MatchID, m.IsParsed(), len(m.Players), len(m.PicksBans))

	require.NoError(t, repo.IngestMatch(ctx, &m), "IngestMatch must succeed")

	var dbMatchID int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT match_id FROM matches WHERE match_id = $1`, m.MatchID,
	).Scan(&dbMatchID))
	require.Equal(t, m.MatchID, dbMatchID)

	var playerCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM player_matches WHERE match_id = $1`, m.MatchID,
	).Scan(&playerCount))
	require.Equal(t, len(m.Players), playerCount, "player_matches row count")

	if len(m.PicksBans) > 0 {
		var pbCount int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM picks_bans WHERE match_id = $1`, m.MatchID,
		).Scan(&pbCount))
		require.Equal(t, len(m.PicksBans), pbCount, "picks_bans row count")
	}

	require.NoError(t, repo.IngestMatch(ctx, &m), "re-ingest must succeed")

	var afterPlayers int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM player_matches WHERE match_id = $1`, m.MatchID,
	).Scan(&afterPlayers))
	require.Equal(t, playerCount, afterPlayers, "re-ingest must not duplicate rows")

	t.Logf("✓ match %d ingested + verified", m.MatchID)
}

func TestIngestMatch_FromLocalFixture_DumpStats(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	pool, cleanup := setupPostgresContainer(ctx, t)
	defer cleanup()

	repo := NewRepositoryFromPool(pool)
	require.NoError(t, repo.Migrate(ctx))

	path := filepath.Join("..", "..", "..", "testdata", "match_sample.json")
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var m models.Match
	require.NoError(t, json.Unmarshal(data, &m))
	require.NoError(t, repo.IngestMatch(ctx, &m))

	tables := []string{
		"matches", "player_matches", "player_match_details",
		"player_timeseries", "picks_bans", "draft_timings",
		"match_objectives", "match_chat", "match_teamfights",
		"match_advantages", "match_cosmetics",
		"team_matches", "teams", "leagues", "players",
	}

	t.Log("------ row counts after ingest ------")
	for _, tbl := range tables {
		var n int
		err := pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM `+tbl+` WHERE match_id = $1`, m.MatchID,
		).Scan(&n)
		if err != nil {
			err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM `+tbl).Scan(&n)
			require.NoError(t, err, "count %s", tbl)
			t.Logf("%-24s  total=%d", tbl, n)
			continue
		}
		t.Logf("%-24s  for_match=%d", tbl, n)
	}
}

func TestInspect_FixtureContents(t *testing.T) {
	path := filepath.Join("..", "..", "..", "testdata", "match_sample.json")
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var m models.Match
	require.NoError(t, json.Unmarshal(data, &m))

	// Note: version appears but od_data.has_parsed=false means NOT replay-parsed.
	// Basic API doesn't include timeseries, objectives, chat, teamfights.
	t.Logf("match_id            = %d", m.MatchID)
	t.Logf("version             = %v (API version, not replay parse)", m.Version)
	t.Logf("is_parsed           = %v (replay-parsed flag)", m.IsParsed())
	t.Logf("radiant_team_id     = %v", m.RadiantTeamID)
	t.Logf("dire_team_id        = %v", m.DireTeamID)
	t.Logf("league_id           = %v", m.LeagueID)
	t.Logf("players             = %d", len(m.Players))
	t.Logf("picks_bans          = %d", len(m.PicksBans))
	t.Logf("draft_timings       = %d", len(m.DraftTimings))
	t.Logf("objectives         = %d", len(m.Objectives))
	t.Logf("chat                = %d", len(m.Chat))
	t.Logf("teamfights         = %d", len(m.Teamfights))
	t.Logf("radiant_gold_adv   = %d", len(m.RadiantGoldAdv))
	t.Logf("radiant_xp_adv     = %d", len(m.RadiantXPAdv))
	t.Logf("cosmetics bytes    = %d", len(m.Cosmetics))

	for i, p := range m.Players {
		t.Logf("player[%d] slot=%d hero=%d times=%d gold_t=%d xp_t=%d",
			i, p.PlayerSlot, p.HeroID, len(p.Times), len(p.GoldT), len(p.XPT))
	}
}