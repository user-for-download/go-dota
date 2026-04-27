-- =====================================================
-- 001_init.sql — OpenDota pipeline initial schema (consolidated)
--
-- Combines: 001_init + 002_fixes + 003_schema_refinements
-- Plus corrections for /matches/{match_id} API alignment:
--   - radiant_win / win nullable (abandoned matches)
--   - region, skill, pauses on matches
--   - per-player cosmetics & benchmarks on player_match_details
--   - rank_tier snapshot on player_matches
--   - picks_bans partitioned by HASH(match_id)
--   - event tables: natural PKs (re-parse idempotent), no FK to matches
--   - cosmetics.used_by_heroes as TEXT[]
--
-- Transaction-safe: no CONCURRENTLY, no VACUUM, no REFRESH MV CONCURRENTLY.
-- Idempotent: all CREATE statements use IF NOT EXISTS where possible.
-- =====================================================

-- ----- Extensions --------------------------------------------------
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- =====================================================
-- Heroes
-- =====================================================
CREATE TABLE IF NOT EXISTS heroes (
    id              SMALLINT PRIMARY KEY,
    name            TEXT NOT NULL,
    localized_name  TEXT NOT NULL,
    primary_attr    TEXT,
    attack_type     TEXT,
    roles           TEXT[],
    legs            SMALLINT,
    img             TEXT,
    icon            TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Stub for empty pick/ban slots (OpenDota emits hero_id = 0).
INSERT INTO heroes (id, name, localized_name)
VALUES (0, 'no_hero', 'No Hero')
ON CONFLICT (id) DO NOTHING;

-- =====================================================
-- Items
-- =====================================================
CREATE TABLE IF NOT EXISTS items (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    localized_name  TEXT,
    cost            INTEGER,
    secret_shop     BOOLEAN DEFAULT FALSE,
    side_shop       BOOLEAN DEFAULT FALSE,
    recipe          BOOLEAN DEFAULT FALSE,
    img             TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- =====================================================
-- Abilities
-- =====================================================
CREATE TABLE IF NOT EXISTS abilities (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    localized_name  TEXT,
    description     TEXT,
    img             TEXT
);

-- =====================================================
-- Patches
-- =====================================================
CREATE TABLE IF NOT EXISTS patches (
    id              SMALLINT PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    release_date    TIMESTAMPTZ NOT NULL,
    release_epoch   BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_patches_release_epoch ON patches(release_epoch DESC);

-- =====================================================
-- Leagues / tournaments
-- =====================================================
CREATE TABLE IF NOT EXISTS leagues (
    leagueid        INTEGER PRIMARY KEY,
    name            TEXT,
    tier            TEXT,
    ticket          TEXT,
    banner          TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_leagues_tier ON leagues(tier) WHERE tier IS NOT NULL;

-- =====================================================
-- Teams
-- =====================================================
CREATE TABLE IF NOT EXISTS teams (
    team_id         BIGINT PRIMARY KEY,
    name            TEXT,
    tag             TEXT,
    logo_url        TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_teams_name_trgm
    ON teams USING GIN (name gin_trgm_ops);

-- =====================================================
-- Players (Steam profiles)
-- =====================================================
CREATE TABLE IF NOT EXISTS players (
    account_id          BIGINT PRIMARY KEY,
    steamid             TEXT,
    personaname         TEXT,
    avatar              TEXT,
    avatarmedium        TEXT,
    avatarfull          TEXT,
    profileurl          TEXT,
    loccountrycode      TEXT,
    plus                BOOLEAN DEFAULT FALSE,
    cheese              INTEGER DEFAULT 0,
    fh_unavailable      BOOLEAN DEFAULT FALSE,
    last_login          TIMESTAMPTZ,
    last_match_time     TIMESTAMPTZ,
    full_history_time   TIMESTAMPTZ,
    profile_time        TIMESTAMPTZ,
    rank_tier_time      TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_players_personaname_trgm
    ON players USING GIN (personaname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_players_last_match_time
    ON players(last_match_time DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_players_full_history_time
    ON players(full_history_time ASC NULLS FIRST);
CREATE INDEX IF NOT EXISTS idx_players_profile_time
    ON players(profile_time ASC NULLS FIRST);
CREATE INDEX IF NOT EXISTS idx_players_rank_tier_time
    ON players(rank_tier_time ASC NULLS FIRST);

-- =====================================================
-- Notable (pro) players
-- =====================================================
CREATE TABLE IF NOT EXISTS notable_players (
    account_id      BIGINT PRIMARY KEY REFERENCES players(account_id) ON DELETE CASCADE,
    name            TEXT,
    country_code    TEXT,
    fantasy_role    SMALLINT,
    team_id         BIGINT REFERENCES teams(team_id) ON DELETE SET NULL,
    team_name       TEXT,
    team_tag        TEXT,
    is_pro          BOOLEAN DEFAULT TRUE,
    is_locked       BOOLEAN DEFAULT FALSE,
    locked_until    BIGINT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_notable_players_team_id
    ON notable_players(team_id) WHERE team_id IS NOT NULL;

-- =====================================================
-- Player rank history
-- =====================================================
CREATE TABLE IF NOT EXISTS player_ranks (
    account_id              BIGINT NOT NULL,
    recorded_at             TIMESTAMPTZ NOT NULL,
    rank_tier               SMALLINT,
    leaderboard_rank        INTEGER,
    solo_competitive_rank   INTEGER,
    competitive_rank        INTEGER,
    match_id                BIGINT,
    PRIMARY KEY (account_id, recorded_at)
);
CREATE INDEX IF NOT EXISTS idx_player_ranks_account
    ON player_ranks(account_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_player_ranks_leaderboard
    ON player_ranks(leaderboard_rank) WHERE leaderboard_rank IS NOT NULL;

-- =====================================================
-- Matches (RANGE partitioned on start_time, quarterly)
-- =====================================================
CREATE TABLE IF NOT EXISTS matches (
    match_id                BIGINT NOT NULL,
    match_seq_num           BIGINT,
    start_time              BIGINT NOT NULL,
    duration                INTEGER NOT NULL CHECK (duration >= 0),
    radiant_win             BOOLEAN,                              -- nullable: abandoned matches
    tower_status_radiant    SMALLINT,
    tower_status_dire       SMALLINT,
    barracks_status_radiant SMALLINT,
    barracks_status_dire    SMALLINT,
    radiant_score           SMALLINT CHECK (radiant_score IS NULL OR radiant_score >= 0),
    dire_score              SMALLINT CHECK (dire_score    IS NULL OR dire_score    >= 0),
    first_blood_time        INTEGER,
    lobby_type              SMALLINT,
    game_mode               SMALLINT,
    cluster                 SMALLINT,
    region                  SMALLINT,                             -- API: region
    skill                   SMALLINT,                             -- API: skill (nullable)
    engine                  SMALLINT,
    human_players           SMALLINT,
    version                 SMALLINT,
    patch_id                SMALLINT REFERENCES patches(id),
    positive_votes          INTEGER DEFAULT 0,
    negative_votes          INTEGER DEFAULT 0,
    leagueid                INTEGER REFERENCES leagues(leagueid) ON DELETE SET NULL,
    series_id               INTEGER,
    series_type             SMALLINT,
    radiant_team_id         BIGINT REFERENCES teams(team_id) ON DELETE SET NULL,
    dire_team_id            BIGINT REFERENCES teams(team_id) ON DELETE SET NULL,
    radiant_captain         BIGINT,
    dire_captain            BIGINT,
    replay_salt             BIGINT,
    replay_url              TEXT,
    pauses                  JSONB,                                -- API: pauses[]
    is_parsed               BOOLEAN DEFAULT FALSE,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (match_id, start_time)
) PARTITION BY RANGE (start_time);

COMMENT ON COLUMN matches.start_time  IS 'Unix epoch seconds. Used for PK and range partition pruning.';
COMMENT ON COLUMN matches.radiant_win IS 'Nullable per OpenDota API (abandoned/incomplete matches).';
COMMENT ON COLUMN matches.pauses      IS 'Array of {time,duration} pause events from API.';

-- Quarterly partitions 2024-2027
CREATE TABLE IF NOT EXISTS matches_2024_q1 PARTITION OF matches FOR VALUES FROM (1704067200) TO (1711929600);
CREATE TABLE IF NOT EXISTS matches_2024_q2 PARTITION OF matches FOR VALUES FROM (1711929600) TO (1719792000);
CREATE TABLE IF NOT EXISTS matches_2024_q3 PARTITION OF matches FOR VALUES FROM (1719792000) TO (1727740800);
CREATE TABLE IF NOT EXISTS matches_2024_q4 PARTITION OF matches FOR VALUES FROM (1727740800) TO (1735689600);
CREATE TABLE IF NOT EXISTS matches_2025_q1 PARTITION OF matches FOR VALUES FROM (1735689600) TO (1743465600);
CREATE TABLE IF NOT EXISTS matches_2025_q2 PARTITION OF matches FOR VALUES FROM (1743465600) TO (1751328000);
CREATE TABLE IF NOT EXISTS matches_2025_q3 PARTITION OF matches FOR VALUES FROM (1751328000) TO (1759276800);
CREATE TABLE IF NOT EXISTS matches_2025_q4 PARTITION OF matches FOR VALUES FROM (1759276800) TO (1767225600);
CREATE TABLE IF NOT EXISTS matches_2026_q1 PARTITION OF matches FOR VALUES FROM (1767225600) TO (1775001600);
CREATE TABLE IF NOT EXISTS matches_2026_q2 PARTITION OF matches FOR VALUES FROM (1775001600) TO (1782864000);
CREATE TABLE IF NOT EXISTS matches_2026_q3 PARTITION OF matches FOR VALUES FROM (1782864000) TO (1790812800);
CREATE TABLE IF NOT EXISTS matches_2026_q4 PARTITION OF matches FOR VALUES FROM (1790812800) TO (1798761600);
CREATE TABLE IF NOT EXISTS matches_2027_q1 PARTITION OF matches FOR VALUES FROM (1798761600) TO (1806624000);
CREATE TABLE IF NOT EXISTS matches_2027_q2 PARTITION OF matches FOR VALUES FROM (1806624000) TO (1814486400);
CREATE TABLE IF NOT EXISTS matches_2027_q3 PARTITION OF matches FOR VALUES FROM (1814486400) TO (1822435200);
CREATE TABLE IF NOT EXISTS matches_2027_q4 PARTITION OF matches FOR VALUES FROM (1822435200) TO (1830384000);
CREATE TABLE IF NOT EXISTS matches_default  PARTITION OF matches DEFAULT;

CREATE INDEX IF NOT EXISTS idx_matches_start_time   ON matches(start_time DESC);
CREATE INDEX IF NOT EXISTS idx_matches_leagueid     ON matches(leagueid, start_time DESC) WHERE leagueid > 0;
CREATE INDEX IF NOT EXISTS idx_matches_radiant_team ON matches(radiant_team_id, start_time DESC) WHERE radiant_team_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_matches_dire_team    ON matches(dire_team_id, start_time DESC)    WHERE dire_team_id    IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_matches_series       ON matches(series_id, start_time DESC)       WHERE series_id IS NOT NULL AND series_id > 0;
CREATE INDEX IF NOT EXISTS idx_matches_patch        ON matches(patch_id, start_time DESC)        WHERE patch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_matches_unparsed     ON matches(match_id) WHERE is_parsed = FALSE;
CREATE INDEX IF NOT EXISTS idx_matches_recent_covering
    ON matches(start_time DESC) INCLUDE (match_id, radiant_win, duration, leagueid);

-- =====================================================
-- player_matches — HOT (RANGE partitioned on start_time)
-- =====================================================
CREATE TABLE IF NOT EXISTS player_matches (
    match_id                BIGINT NOT NULL,
    player_slot             SMALLINT NOT NULL,
    start_time              BIGINT NOT NULL,
    account_id              BIGINT,
    hero_id                 SMALLINT NOT NULL REFERENCES heroes(id),
    hero_variant            SMALLINT,
    is_radiant              BOOLEAN NOT NULL,
    win                     BOOLEAN,                              -- nullable (abandoned)
    duration                INTEGER NOT NULL,
    patch_id                SMALLINT,
    lobby_type              SMALLINT,
    game_mode               SMALLINT,
    rank_tier               SMALLINT,                             -- snapshot at match time
    kills                   SMALLINT NOT NULL DEFAULT 0,
    deaths                  SMALLINT NOT NULL DEFAULT 0,
    assists                 SMALLINT NOT NULL DEFAULT 0,
    level                   SMALLINT,
    net_worth               INTEGER,
    gold                    INTEGER,
    gold_spent              INTEGER,
    gold_per_min            SMALLINT,
    xp_per_min              SMALLINT,
    last_hits               SMALLINT,
    denies                  SMALLINT,
    hero_damage             INTEGER,
    tower_damage            INTEGER,
    hero_healing            INTEGER,
    item_0                  INTEGER,
    item_1                  INTEGER,
    item_2                  INTEGER,
    item_3                  INTEGER,
    item_4                  INTEGER,
    item_5                  INTEGER,
    item_neutral            INTEGER,
    backpack_0              INTEGER,
    backpack_1              INTEGER,
    backpack_2              INTEGER,
    backpack_3              INTEGER,
    lane                    SMALLINT,
    lane_role               SMALLINT,
    is_roaming              BOOLEAN,
    party_id                INTEGER,
    party_size              SMALLINT,
    stuns                   REAL,
    obs_placed              SMALLINT,
    sen_placed              SMALLINT,
    creeps_stacked          SMALLINT,
    camps_stacked           SMALLINT,
    rune_pickups            SMALLINT,
    firstblood_claimed      BOOLEAN,
    teamfight_participation REAL,
    towers_killed           SMALLINT,
    roshans_killed          SMALLINT,
    observers_placed        SMALLINT,
    leaver_status           SMALLINT,
    PRIMARY KEY (match_id, player_slot, start_time)
) PARTITION BY RANGE (start_time);

CREATE TABLE IF NOT EXISTS player_matches_2024_q1 PARTITION OF player_matches FOR VALUES FROM (1704067200) TO (1711929600);
CREATE TABLE IF NOT EXISTS player_matches_2024_q2 PARTITION OF player_matches FOR VALUES FROM (1711929600) TO (1719792000);
CREATE TABLE IF NOT EXISTS player_matches_2024_q3 PARTITION OF player_matches FOR VALUES FROM (1719792000) TO (1727740800);
CREATE TABLE IF NOT EXISTS player_matches_2024_q4 PARTITION OF player_matches FOR VALUES FROM (1727740800) TO (1735689600);
CREATE TABLE IF NOT EXISTS player_matches_2025_q1 PARTITION OF player_matches FOR VALUES FROM (1735689600) TO (1743465600);
CREATE TABLE IF NOT EXISTS player_matches_2025_q2 PARTITION OF player_matches FOR VALUES FROM (1743465600) TO (1751328000);
CREATE TABLE IF NOT EXISTS player_matches_2025_q3 PARTITION OF player_matches FOR VALUES FROM (1751328000) TO (1759276800);
CREATE TABLE IF NOT EXISTS player_matches_2025_q4 PARTITION OF player_matches FOR VALUES FROM (1759276800) TO (1767225600);
CREATE TABLE IF NOT EXISTS player_matches_2026_q1 PARTITION OF player_matches FOR VALUES FROM (1767225600) TO (1775001600);
CREATE TABLE IF NOT EXISTS player_matches_2026_q2 PARTITION OF player_matches FOR VALUES FROM (1775001600) TO (1782864000);
CREATE TABLE IF NOT EXISTS player_matches_2026_q3 PARTITION OF player_matches FOR VALUES FROM (1782864000) TO (1790812800);
CREATE TABLE IF NOT EXISTS player_matches_2026_q4 PARTITION OF player_matches FOR VALUES FROM (1790812800) TO (1798761600);
CREATE TABLE IF NOT EXISTS player_matches_2027_q1 PARTITION OF player_matches FOR VALUES FROM (1798761600) TO (1806624000);
CREATE TABLE IF NOT EXISTS player_matches_2027_q2 PARTITION OF player_matches FOR VALUES FROM (1806624000) TO (1814486400);
CREATE TABLE IF NOT EXISTS player_matches_2027_q3 PARTITION OF player_matches FOR VALUES FROM (1814486400) TO (1822435200);
CREATE TABLE IF NOT EXISTS player_matches_2027_q4 PARTITION OF player_matches FOR VALUES FROM (1822435200) TO (1830384000);
CREATE TABLE IF NOT EXISTS player_matches_default  PARTITION OF player_matches DEFAULT;

CREATE INDEX IF NOT EXISTS idx_pm_account
    ON player_matches(account_id, start_time DESC) WHERE account_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pm_hero
    ON player_matches(hero_id, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_pm_hero_patch
    ON player_matches(hero_id, patch_id) WHERE patch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pm_account_hero_time
    ON player_matches(account_id, hero_id, start_time DESC) WHERE account_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pm_account_win
    ON player_matches(account_id, win) WHERE account_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pm_match
    ON player_matches(match_id);

-- =====================================================
-- player_match_details — COLD (HASH partitioned on match_id)
-- =====================================================
CREATE TABLE IF NOT EXISTS player_match_details (
    match_id                    BIGINT NOT NULL,
    player_slot                 SMALLINT NOT NULL,
    -- damage breakdowns
    damage                      JSONB,
    damage_taken                JSONB,
    damage_inflictor            JSONB,
    damage_inflictor_received   JSONB,
    damage_targets              JSONB,
    hero_hits                   JSONB,
    max_hero_hit                JSONB,
    -- abilities & items
    ability_uses                JSONB,
    ability_targets             JSONB,
    ability_upgrades_arr        JSONB,
    item_uses                   JSONB,
    -- economy
    gold_reasons                JSONB,
    xp_reasons                  JSONB,
    killed                      JSONB,
    killed_by                   JSONB,
    kill_streaks                JSONB,
    multi_kills                 JSONB,
    life_state                  JSONB,
    lane_pos                    JSONB,
    obs                         JSONB,
    sen                         JSONB,
    actions                     JSONB,
    pings                       JSONB,
    runes                       JSONB,
    purchase                    JSONB,
    obs_log                     JSONB,
    sen_log                     JSONB,
    obs_left_log                JSONB,
    sen_left_log                JSONB,
    purchase_log                JSONB,
    kills_log                   JSONB,
    buyback_log                 JSONB,
    runes_log                   JSONB,
    connection_log              JSONB,
    permanent_buffs             JSONB,
    neutral_tokens_log          JSONB,
    neutral_item_history        JSONB,
    additional_units            JSONB,
    -- API alignment additions
    cosmetics                   JSONB,
    benchmarks                  JSONB,
    PRIMARY KEY (match_id, player_slot)
) PARTITION BY HASH (match_id);

CREATE TABLE IF NOT EXISTS player_match_details_p0 PARTITION OF player_match_details FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS player_match_details_p1 PARTITION OF player_match_details FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS player_match_details_p2 PARTITION OF player_match_details FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS player_match_details_p3 PARTITION OF player_match_details FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS player_match_details_p4 PARTITION OF player_match_details FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS player_match_details_p5 PARTITION OF player_match_details FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS player_match_details_p6 PARTITION OF player_match_details FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS player_match_details_p7 PARTITION OF player_match_details FOR VALUES WITH (MODULUS 8, REMAINDER 7);

-- =====================================================
-- Picks / Bans
-- =====================================================
CREATE TABLE IF NOT EXISTS picks_bans (
                                          match_id        BIGINT NOT NULL,
                                          ord             SMALLINT NOT NULL,
                                          is_pick         BOOLEAN NOT NULL,
                                          hero_id         SMALLINT NOT NULL REFERENCES heroes(id),
    team            SMALLINT NOT NULL CHECK (team IN (0, 1)),
    PRIMARY KEY (match_id, ord)
    ) PARTITION BY HASH (match_id);

CREATE TABLE IF NOT EXISTS picks_bans_p0 PARTITION OF picks_bans FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS picks_bans_p1 PARTITION OF picks_bans FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS picks_bans_p2 PARTITION OF picks_bans FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS picks_bans_p3 PARTITION OF picks_bans FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS picks_bans_p4 PARTITION OF picks_bans FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS picks_bans_p5 PARTITION OF picks_bans FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS picks_bans_p6 PARTITION OF picks_bans FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS picks_bans_p7 PARTITION OF picks_bans FOR VALUES WITH (MODULUS 8, REMAINDER 7);

-- =====================================================
-- Match Events (Objectives / Chat / Teamfights)
-- =====================================================
CREATE TABLE IF NOT EXISTS match_objectives (
                                                id              BIGINT GENERATED ALWAYS AS IDENTITY,
                                                match_id        BIGINT NOT NULL,
                                                start_time      BIGINT NOT NULL,
                                                time            INTEGER NOT NULL,
                                                type            TEXT NOT NULL,
                                                slot            SMALLINT,
                                                player_slot     SMALLINT,
                                                team            SMALLINT,
                                                key             TEXT,
                                                value           INTEGER,
                                                unit            TEXT,
                                                PRIMARY KEY (match_id, id)
    ) PARTITION BY HASH (match_id);

CREATE TABLE IF NOT EXISTS match_objectives_p0 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS match_objectives_p1 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS match_objectives_p2 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS match_objectives_p3 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS match_objectives_p4 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS match_objectives_p5 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS match_objectives_p6 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS match_objectives_p7 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 7);

CREATE TABLE IF NOT EXISTS match_chat (
                                          id              BIGINT GENERATED ALWAYS AS IDENTITY,
                                          match_id        BIGINT NOT NULL,
                                          start_time      BIGINT NOT NULL,
                                          time            INTEGER NOT NULL,
                                          type            TEXT,
                                          player_slot     SMALLINT,
                                          unit            TEXT,
                                          key             TEXT,
                                          PRIMARY KEY (match_id, id)
    ) PARTITION BY HASH (match_id);

CREATE TABLE IF NOT EXISTS match_chat_p0 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS match_chat_p1 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS match_chat_p2 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS match_chat_p3 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS match_chat_p4 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS match_chat_p5 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS match_chat_p6 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS match_chat_p7 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 7);

CREATE TABLE IF NOT EXISTS match_teamfights (
                                                id              BIGINT GENERATED ALWAYS AS IDENTITY,
                                                match_id        BIGINT NOT NULL,
                                                start_time      BIGINT NOT NULL,
                                                end_time        INTEGER NOT NULL,
                                                last_death      INTEGER,
                                                deaths          SMALLINT,
                                                players         JSONB,
                                                PRIMARY KEY (match_id, id)
    ) PARTITION BY HASH (match_id);

CREATE TABLE IF NOT EXISTS match_teamfights_p0 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS match_teamfights_p1 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS match_teamfights_p2 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS match_teamfights_p3 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS match_teamfights_p4 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS match_teamfights_p5 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS match_teamfights_p6 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS match_teamfights_p7 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 7);

-- =====================================================
-- Other tables (simplified)
-- =====================================================
CREATE TABLE IF NOT EXISTS match_advantages (
                                                match_id            BIGINT PRIMARY KEY,
                                                radiant_gold_adv    INTEGER[],
                                                radiant_xp_adv      INTEGER[]
);

CREATE TABLE IF NOT EXISTS cosmetics (
                                         item_id             INTEGER PRIMARY KEY,
                                         name                TEXT,
                                         prefab              TEXT,
                                         creation_date       TIMESTAMPTZ,
                                         image_inventory     TEXT,
                                         image_path          TEXT,
                                         item_description    TEXT,
                                         item_name           TEXT,
                                         item_rarity         TEXT,
                                         item_type_name      TEXT,
                                         used_by_heroes      TEXT[]
);

-- =====================================================
-- Public Matches
-- =====================================================
CREATE TABLE IF NOT EXISTS public_matches (
                                              match_id        BIGINT NOT NULL,
                                              start_time      BIGINT NOT NULL,
                                              duration        INTEGER,
                                              radiant_win     BOOLEAN,
                                              lobby_type      SMALLINT,
                                              game_mode       SMALLINT,
                                              avg_rank_tier   SMALLINT,
                                              radiant_team    SMALLINT[],
                                              dire_team       SMALLINT[],
                                              PRIMARY KEY (match_id, start_time)
    ) PARTITION BY RANGE (start_time);

-- ... (Create partitions same as matches table) ...
CREATE TABLE IF NOT EXISTS public_matches_default PARTITION OF public_matches DEFAULT;

-- =====================================================
-- Job Queue & Migration
-- =====================================================
CREATE TABLE IF NOT EXISTS job_queue (
                                         id              BIGSERIAL PRIMARY KEY,
                                         type            TEXT NOT NULL,
                                         payload         JSONB NOT NULL,
                                         status          TEXT DEFAULT 'pending',
                                         created_at      TIMESTAMPTZ DEFAULT NOW()
    );

CREATE TABLE IF NOT EXISTS migration_log (
                                             source_match_id     BIGINT PRIMARY KEY,
                                             status              TEXT NOT NULL,
                                             error               TEXT,
                                             migrated_at         TIMESTAMPTZ DEFAULT NOW()
    );
