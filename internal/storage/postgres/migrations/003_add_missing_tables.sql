-- game_modes
CREATE TABLE IF NOT EXISTS game_modes (
                                          id   SMALLINT PRIMARY KEY,
                                          name TEXT NOT NULL DEFAULT ''
);

-- lobby_types
CREATE TABLE IF NOT EXISTS lobby_types (
                                           id   SMALLINT PRIMARY KEY,
                                           name TEXT NOT NULL DEFAULT ''
);

-- team_rating (needs the teams table to exist, which 001_init.sql likely already creates)
CREATE TABLE IF NOT EXISTS team_rating (
                                           team_id        BIGINT PRIMARY KEY REFERENCES teams(team_id) ON DELETE CASCADE,
    rating         REAL    NOT NULL DEFAULT 0,
    wins           INTEGER NOT NULL DEFAULT 0,
    losses         INTEGER NOT NULL DEFAULT 0,
    last_match_time BIGINT NOT NULL DEFAULT 0,
    last_match_id  BIGINT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

-- Also ensure that the heroes, items, leagues, patches, players, teams tables exist.
