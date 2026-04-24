
-- =====================================================
-- 001_init.sql — OpenDota pipeline initial schema
--
-- Transaction-safe: no CONCURRENTLY, no VACUUM, no REFRESH MV CONCURRENTLY.
-- Idempotent: all CREATE statements use IF NOT EXISTS.
-- =====================================================

-- ----- Extensions -----------------------------------------------
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- =====================================================
-- Герои
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

-- =====================================================
-- Предметы
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
-- Способности
-- =====================================================
CREATE TABLE IF NOT EXISTS abilities (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    localized_name  TEXT,
    description     TEXT,
    img             TEXT
);

-- =====================================================
-- Патчи
-- =====================================================
CREATE TABLE IF NOT EXISTS patches (
    id              SMALLINT PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    release_date    TIMESTAMPTZ NOT NULL,
    release_epoch   BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_patches_release_epoch ON patches(release_epoch DESC);

-- =====================================================
-- Лиги / турниры
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
-- Команды (без рейтинга — рейтинг в team_rating)
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
-- Рейтинг команд
-- =====================================================
CREATE TABLE IF NOT EXISTS team_rating (
    team_id         BIGINT PRIMARY KEY REFERENCES teams(team_id) ON DELETE CASCADE,
    rating          REAL,
    wins            INTEGER DEFAULT 0,
    losses          INTEGER DEFAULT 0,
    last_match_time BIGINT,
    last_match_id   BIGINT,
    delta           REAL,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_team_rating_rating
    ON team_rating(rating DESC NULLS LAST);

-- =====================================================
-- Игроки (Steam профили)
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
-- personaname: GIN вместо GIST (btree_gin уже подключён, gist_trgm_ops требует btree_gist)
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
-- Про-игроки (notable players)
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
    locked_until    INTEGER,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_notable_players_team_id
    ON notable_players(team_id) WHERE team_id IS NOT NULL;

-- =====================================================
-- Ранги игроков (история)
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
-- Матчи (партиционированы по start_time на КВАРТАЛЫ)
-- =====================================================
CREATE TABLE IF NOT EXISTS matches (
    match_id                BIGINT NOT NULL,
    match_seq_num           BIGINT,
    start_time              BIGINT NOT NULL,
    duration                INTEGER NOT NULL,
    radiant_win             BOOLEAN NOT NULL,
    tower_status_radiant    SMALLINT,
    tower_status_dire       SMALLINT,
    barracks_status_radiant SMALLINT,
    barracks_status_dire    SMALLINT,
    radiant_score           SMALLINT,
    dire_score              SMALLINT,
    first_blood_time        INTEGER,
    lobby_type              SMALLINT,
    game_mode               SMALLINT,
    cluster                 SMALLINT,
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
    is_parsed               BOOLEAN DEFAULT FALSE,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (match_id, start_time)
) PARTITION BY RANGE (start_time);

-- Квартальные партиции 2024-2025
CREATE TABLE IF NOT EXISTS matches_2024_q1 PARTITION OF matches FOR VALUES FROM (1704067200) TO (1711929600);
CREATE TABLE IF NOT EXISTS matches_2024_q2 PARTITION OF matches FOR VALUES FROM (1711929600) TO (1719792000);
CREATE TABLE IF NOT EXISTS matches_2024_q3 PARTITION OF matches FOR VALUES FROM (1719792000) TO (1727740800);
CREATE TABLE IF NOT EXISTS matches_2024_q4 PARTITION OF matches FOR VALUES FROM (1727740800) TO (1735689600);
CREATE TABLE IF NOT EXISTS matches_2025_q1 PARTITION OF matches FOR VALUES FROM (1735689600) TO (1743465600);
CREATE TABLE IF NOT EXISTS matches_2025_q2 PARTITION OF matches FOR VALUES FROM (1743465600) TO (1751328000);
CREATE TABLE IF NOT EXISTS matches_2025_q3 PARTITION OF matches FOR VALUES FROM (1751328000) TO (1759276800);
CREATE TABLE IF NOT EXISTS matches_2025_q4 PARTITION OF matches FOR VALUES FROM (1759276800) TO (1767225600);
CREATE TABLE IF NOT EXISTS matches_default PARTITION OF matches DEFAULT;

CREATE INDEX IF NOT EXISTS idx_matches_start_time ON matches(start_time DESC);
CREATE INDEX IF NOT EXISTS idx_matches_leagueid ON matches(leagueid, start_time DESC) WHERE leagueid > 0;
CREATE INDEX IF NOT EXISTS idx_matches_radiant_team ON matches(radiant_team_id, start_time DESC) WHERE radiant_team_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_matches_dire_team ON matches(dire_team_id, start_time DESC) WHERE dire_team_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_matches_series ON matches(series_id) WHERE series_id > 0;
CREATE INDEX IF NOT EXISTS idx_matches_patch ON matches(patch_id, start_time DESC) WHERE patch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_matches_unparsed ON matches(match_id) WHERE is_parsed = FALSE;

-- =====================================================
-- player_matches — HOT таблица
-- =====================================================
CREATE TABLE IF NOT EXISTS player_matches (
    match_id                BIGINT NOT NULL,
    player_slot             SMALLINT NOT NULL,
    start_time              BIGINT NOT NULL,
    account_id              BIGINT,
    hero_id                 SMALLINT NOT NULL REFERENCES heroes(id),
    hero_variant            SMALLINT,
    is_radiant              BOOLEAN NOT NULL,
    win                     BOOLEAN NOT NULL,
    duration                INTEGER NOT NULL,
    patch_id                SMALLINT,
    lobby_type              SMALLINT,
    game_mode               SMALLINT,
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
    final_items             INTEGER[],
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
    times                   INTEGER[],
    gold_t                  INTEGER[],
    xp_t                    INTEGER[],
    lh_t                    INTEGER[],
    dn_t                    INTEGER[],
    ability_upgrades_arr    INTEGER[],
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
CREATE TABLE IF NOT EXISTS player_matches_default PARTITION OF player_matches DEFAULT;

CREATE INDEX IF NOT EXISTS idx_pm_account       ON player_matches(account_id, start_time DESC) WHERE account_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pm_hero          ON player_matches(hero_id, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_pm_hero_patch    ON player_matches(hero_id, patch_id) WHERE patch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pm_account_hero  ON player_matches(account_id, hero_id) WHERE account_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pm_final_items_gin ON player_matches USING GIN(final_items);
CREATE INDEX IF NOT EXISTS idx_pm_match         ON player_matches(match_id);

-- =====================================================
-- player_match_details — COLD таблица
-- =====================================================
CREATE TABLE IF NOT EXISTS player_match_details (
    match_id                    BIGINT NOT NULL,
    player_slot                 SMALLINT NOT NULL,
    damage                      JSONB,
    damage_taken                JSONB,
    damage_inflictor            JSONB,
    damage_inflictor_received   JSONB,
    damage_targets              JSONB,
    hero_hits                   JSONB,
    max_hero_hit                JSONB,
    ability_uses                JSONB,
    ability_targets             JSONB,
    item_uses                   JSONB,
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
-- Пики и баны
-- =====================================================
CREATE TABLE IF NOT EXISTS picks_bans (
    match_id        BIGINT NOT NULL,
    ord             SMALLINT NOT NULL,
    is_pick         BOOLEAN NOT NULL,
    hero_id         SMALLINT NOT NULL REFERENCES heroes(id),
    team            SMALLINT NOT NULL,
    PRIMARY KEY (match_id, ord)
);
CREATE INDEX IF NOT EXISTS idx_picks_bans_hero ON picks_bans(hero_id, is_pick);

CREATE TABLE IF NOT EXISTS draft_timings (
    match_id            BIGINT NOT NULL,
    ord                 SMALLINT NOT NULL,
    pick                BOOLEAN,
    active_team         SMALLINT,
    hero_id             SMALLINT REFERENCES heroes(id),
    player_slot         SMALLINT,
    extra_time          INTEGER,
    total_time_taken    INTEGER,
    PRIMARY KEY (match_id, ord)
);

-- =====================================================
-- События матча (objectives) (ПАРТИЦИОНИРОВАНО)
-- =====================================================
CREATE TABLE IF NOT EXISTS match_objectives (
    match_id        BIGINT NOT NULL,
    time            INTEGER NOT NULL,
    type            TEXT NOT NULL,
    slot            SMALLINT,
    player_slot     SMALLINT,
    team            SMALLINT,
    key             TEXT,
    value           INTEGER,
    unit            TEXT
) PARTITION BY HASH (match_id);

CREATE TABLE IF NOT EXISTS match_objectives_p0 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS match_objectives_p1 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS match_objectives_p2 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS match_objectives_p3 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS match_objectives_p4 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS match_objectives_p5 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS match_objectives_p6 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS match_objectives_p7 PARTITION OF match_objectives FOR VALUES WITH (MODULUS 8, REMAINDER 7);

CREATE INDEX IF NOT EXISTS idx_objectives_match ON match_objectives(match_id, time);
CREATE INDEX IF NOT EXISTS idx_objectives_type ON match_objectives(type, match_id);

-- =====================================================
-- Чат матча (ПАРТИЦИОНИРОВАНО)
-- =====================================================
CREATE TABLE IF NOT EXISTS match_chat (
    match_id        BIGINT NOT NULL,
    time            INTEGER NOT NULL,
    type            TEXT,
    player_slot     SMALLINT,
    unit            TEXT,
    key             TEXT
) PARTITION BY HASH (match_id);

CREATE TABLE IF NOT EXISTS match_chat_p0 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS match_chat_p1 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS match_chat_p2 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS match_chat_p3 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS match_chat_p4 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS match_chat_p5 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS match_chat_p6 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS match_chat_p7 PARTITION OF match_chat FOR VALUES WITH (MODULUS 8, REMAINDER 7);

CREATE INDEX IF NOT EXISTS idx_chat_match ON match_chat(match_id, time);

-- =====================================================
-- Тимфайты (сводка) (ПАРТИЦИОНИРОВАНО)
-- =====================================================
CREATE TABLE IF NOT EXISTS match_teamfights (
    match_id        BIGINT NOT NULL,
    start_time      INTEGER NOT NULL,
    end_time        INTEGER NOT NULL,
    last_death      INTEGER,
    deaths          SMALLINT,
    players         JSONB
) PARTITION BY HASH (match_id);

CREATE TABLE IF NOT EXISTS match_teamfights_p0 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS match_teamfights_p1 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS match_teamfights_p2 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS match_teamfights_p3 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS match_teamfights_p4 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS match_teamfights_p5 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS match_teamfights_p6 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS match_teamfights_p7 PARTITION OF match_teamfights FOR VALUES WITH (MODULUS 8, REMAINDER 7);

CREATE INDEX IF NOT EXISTS idx_teamfights_match ON match_teamfights(match_id, start_time);

-- =====================================================
-- Преимущества по золоту/опыту (радиант)
-- =====================================================
CREATE TABLE IF NOT EXISTS match_advantages (
    match_id            BIGINT PRIMARY KEY,
    radiant_gold_adv    INTEGER[],
    radiant_xp_adv      INTEGER[]
);

-- =====================================================
-- Связь команда ↔ матч 
-- =====================================================
CREATE TABLE IF NOT EXISTS team_matches (
    team_id         BIGINT NOT NULL REFERENCES teams(team_id) ON DELETE CASCADE,
    match_id        BIGINT NOT NULL,
    is_radiant      BOOLEAN NOT NULL,
    win             BOOLEAN NOT NULL,
    start_time      BIGINT NOT NULL,
    leagueid        INTEGER,
    PRIMARY KEY (team_id, match_id)
);
CREATE INDEX IF NOT EXISTS idx_team_matches_start_time ON team_matches(team_id, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_team_matches_league ON team_matches(leagueid) WHERE leagueid IS NOT NULL;

-- =====================================================
-- Косметика матча 
-- =====================================================
CREATE TABLE IF NOT EXISTS match_cosmetics (
    match_id        BIGINT PRIMARY KEY,
    cosmetics       JSONB
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
    used_by_heroes      TEXT
);
	
-- =====================================================
-- Поминутная таймсерия игрока
-- =====================================================
CREATE TABLE IF NOT EXISTS player_timeseries (
    match_id        BIGINT NOT NULL,
    player_slot     SMALLINT NOT NULL,
    minute          SMALLINT NOT NULL,        
    hero_id         SMALLINT NOT NULL,
    account_id      BIGINT,
    patch_id        SMALLINT,
    gold            INTEGER,
    xp              INTEGER,
    lh              SMALLINT,
    dn              SMALLINT,
    PRIMARY KEY (match_id, player_slot, minute)
) PARTITION BY RANGE (match_id);

CREATE TABLE IF NOT EXISTS player_timeseries_p0 PARTITION OF player_timeseries FOR VALUES FROM (MINVALUE) TO (7000000000);
CREATE TABLE IF NOT EXISTS player_timeseries_p1 PARTITION OF player_timeseries FOR VALUES FROM (7000000000) TO (7500000000);
CREATE TABLE IF NOT EXISTS player_timeseries_p2 PARTITION OF player_timeseries FOR VALUES FROM (7500000000) TO (8000000000);
CREATE TABLE IF NOT EXISTS player_timeseries_p3 PARTITION OF player_timeseries FOR VALUES FROM (8000000000) TO (8500000000);
CREATE TABLE IF NOT EXISTS player_timeseries_p4 PARTITION OF player_timeseries FOR VALUES FROM (8500000000) TO (MAXVALUE);

CREATE INDEX IF NOT EXISTS idx_ts_hero_minute_patch ON player_timeseries(hero_id, minute, patch_id);
CREATE INDEX IF NOT EXISTS idx_ts_account ON player_timeseries(account_id, match_id) WHERE account_id IS NOT NULL;
	
-- =====================================================
-- public_matches — упрощённая запись без детальной статистики
-- =====================================================
CREATE TABLE IF NOT EXISTS public_matches (
    match_id        BIGINT NOT NULL,
    match_seq_num   BIGINT,
    start_time      BIGINT NOT NULL,
    duration        INTEGER,
    radiant_win     BOOLEAN,
    lobby_type      SMALLINT,
    game_mode       SMALLINT,
    cluster         SMALLINT,
    avg_rank_tier   SMALLINT,                 
    num_rank_tier   SMALLINT,                 
    avg_mmr         INTEGER,
    num_mmr         SMALLINT,
    radiant_team    SMALLINT[],               
    dire_team       SMALLINT[],               
    PRIMARY KEY (match_id, start_time)
) PARTITION BY RANGE (start_time);

-- Квартальные партиции -- ИСПРАВЛЕНО
CREATE TABLE IF NOT EXISTS public_matches_2024_q1 PARTITION OF public_matches FOR VALUES FROM (1704067200) TO (1711929600);
CREATE TABLE IF NOT EXISTS public_matches_2024_q2 PARTITION OF public_matches FOR VALUES FROM (1711929600) TO (1719792000);
CREATE TABLE IF NOT EXISTS public_matches_2024_q3 PARTITION OF public_matches FOR VALUES FROM (1719792000) TO (1727740800);
CREATE TABLE IF NOT EXISTS public_matches_2024_q4 PARTITION OF public_matches FOR VALUES FROM (1727740800) TO (1735689600);
CREATE TABLE IF NOT EXISTS public_matches_2025_q1 PARTITION OF public_matches FOR VALUES FROM (1735689600) TO (1743465600);
CREATE TABLE IF NOT EXISTS public_matches_2025_q2 PARTITION OF public_matches FOR VALUES FROM (1743465600) TO (1751328000);
CREATE TABLE IF NOT EXISTS public_matches_2025_q3 PARTITION OF public_matches FOR VALUES FROM (1751328000) TO (1759276800);
CREATE TABLE IF NOT EXISTS public_matches_2025_q4 PARTITION OF public_matches FOR VALUES FROM (1759276800) TO (1767225600);

CREATE INDEX IF NOT EXISTS idx_public_matches_start_time ON public_matches(start_time DESC);
CREATE INDEX IF NOT EXISTS idx_public_matches_avg_rank ON public_matches(avg_rank_tier) WHERE avg_rank_tier IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_public_matches_radiant_gin ON public_matches USING GIN(radiant_team);
CREATE INDEX IF NOT EXISTS idx_public_matches_dire_gin ON public_matches USING GIN(dire_team);
	
-- =====================================================
-- Рейтинг команд
-- =====================================================
CREATE TABLE IF NOT EXISTS team_rating (
    team_id         BIGINT PRIMARY KEY REFERENCES teams(team_id) ON DELETE CASCADE,
    rating          REAL,
    wins            INTEGER DEFAULT 0,
    losses          INTEGER DEFAULT 0,
    last_match_time BIGINT,
    last_match_id   BIGINT,
    delta           REAL,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_team_rating_rating ON team_rating(rating DESC NULLS LAST);

-- =====================================================
-- Ранкинг героев по игрокам
-- =====================================================
CREATE TABLE IF NOT EXISTS hero_rankings (
    account_id      BIGINT NOT NULL,
    hero_id         SMALLINT NOT NULL REFERENCES heroes(id),
    score           DOUBLE PRECISION NOT NULL,
    percent_rank    REAL,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (account_id, hero_id)
);
CREATE INDEX IF NOT EXISTS idx_hero_rankings_hero_score ON hero_rankings(hero_id, score DESC);
	
-- =====================================================
-- Очередь задач
-- =====================================================
CREATE TABLE IF NOT EXISTS job_queue (
    id              BIGSERIAL PRIMARY KEY,
    type            TEXT NOT NULL,            
    payload         JSONB NOT NULL,
    priority        SMALLINT DEFAULT 0,
    status          TEXT DEFAULT 'pending',   
    attempts        SMALLINT DEFAULT 0,
    max_attempts    SMALLINT DEFAULT 5,
    last_error      TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_job_queue_pending ON job_queue(priority DESC, id ASC) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_job_queue_type_status ON job_queue(type, status);

-- =====================================================
-- Лог миграции
-- =====================================================
CREATE TABLE IF NOT EXISTS migration_log (
    id                  BIGSERIAL PRIMARY KEY,
    source_match_id     BIGINT NOT NULL,
    status              TEXT NOT NULL,        
    error               TEXT,
    attempts            SMALLINT DEFAULT 1,
    migrated_at         TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_migration_log_status ON migration_log(status, migrated_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_migration_log_match ON migration_log(source_match_id);
	
-- =====================================================
-- Winrate героев по патчам (MVs)
-- =====================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hero_winrate_patch AS
SELECT 
    hero_id,
    patch_id,
    COUNT(*) AS games,
    SUM(CASE WHEN win THEN 1 ELSE 0 END) AS wins,
    ROUND(100.0 * SUM(CASE WHEN win THEN 1 ELSE 0 END) / COUNT(*), 2) AS winrate,
    AVG(kills)::REAL AS avg_kills,
    AVG(deaths)::REAL AS avg_deaths,
    AVG(assists)::REAL AS avg_assists,
    AVG(gold_per_min)::REAL AS avg_gpm,
    AVG(xp_per_min)::REAL AS avg_xpm
FROM player_matches
WHERE patch_id IS NOT NULL
GROUP BY hero_id, patch_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_hero_winrate_patch ON mv_hero_winrate_patch(hero_id, patch_id);

-- =====================================================
-- Winrate героев в публичных матчах по рангам
-- =====================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hero_winrate_public AS
WITH exploded AS (
    SELECT 
        unnest(radiant_team) AS hero_id,
        radiant_win AS win,
        avg_rank_tier,
        start_time
    FROM public_matches
    WHERE avg_rank_tier IS NOT NULL
    UNION ALL
    SELECT 
        unnest(dire_team) AS hero_id,
        NOT radiant_win AS win,
        avg_rank_tier,
        start_time
    FROM public_matches
    WHERE avg_rank_tier IS NOT NULL
)
SELECT 
    hero_id,
    (avg_rank_tier / 10)::SMALLINT AS rank_bracket,
    COUNT(*) AS games,
    SUM(CASE WHEN win THEN 1 ELSE 0 END) AS wins,
    ROUND(100.0 * SUM(CASE WHEN win THEN 1 ELSE 0 END) / COUNT(*), 2) AS winrate
FROM exploded
WHERE start_time >= EXTRACT(EPOCH FROM NOW() - INTERVAL '30 days')::BIGINT
GROUP BY hero_id, rank_bracket;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_hero_winrate_public ON mv_hero_winrate_public(hero_id, rank_bracket);

-- =====================================================
-- Pick rate героев
-- =====================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hero_pickrate_patch AS
WITH total_matches AS (
    SELECT patch_id, COUNT(DISTINCT match_id) AS total
    FROM player_matches
    WHERE patch_id IS NOT NULL
    GROUP BY patch_id
)
SELECT 
    pm.hero_id,
    pm.patch_id,
    COUNT(*) AS picks,
    tm.total AS total_games,
    ROUND(100.0 * COUNT(*) / tm.total, 2) AS pick_rate
FROM player_matches pm
JOIN total_matches tm USING (patch_id)
GROUP BY pm.hero_id, pm.patch_id, tm.total;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_hero_pickrate_patch ON mv_hero_pickrate_patch(hero_id, patch_id);

-- =====================================================
-- Статистика игрока (сводка)
-- =====================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_stats AS
SELECT 
    account_id,
    COUNT(*) AS total_matches,
    SUM(CASE WHEN win THEN 1 ELSE 0 END) AS wins,
    ROUND(100.0 * SUM(CASE WHEN win THEN 1 ELSE 0 END) / COUNT(*), 2) AS winrate,
    AVG(kills)::REAL AS avg_kills,
    AVG(deaths)::REAL AS avg_deaths,
    AVG(assists)::REAL AS avg_assists,
    AVG(gold_per_min)::REAL AS avg_gpm,
    AVG(xp_per_min)::REAL AS avg_xpm,
    MODE() WITHIN GROUP (ORDER BY hero_id) AS most_played_hero
FROM player_matches
WHERE account_id IS NOT NULL
GROUP BY account_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_player_stats ON mv_player_stats(account_id);

-- =====================================================
-- Рейтинг команд (последние 90 дней)
-- =====================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_team_recent_performance AS
SELECT 
    team_id,
    COUNT(*) AS total_games,
    SUM(CASE WHEN win THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN NOT win THEN 1 ELSE 0 END) AS losses,
    ROUND(100.0 * SUM(CASE WHEN win THEN 1 ELSE 0 END) / COUNT(*), 2) AS winrate
FROM team_matches
WHERE start_time >= EXTRACT(EPOCH FROM NOW() - INTERVAL '90 days')::BIGINT
GROUP BY team_id;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mv_team_recent_performance ON mv_team_recent_performance(team_id);
