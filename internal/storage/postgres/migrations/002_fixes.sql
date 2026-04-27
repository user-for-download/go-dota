-- =====================================================
-- 002_fixes.sql — corrections to 001_init.sql
--
-- Addresses:
--   1. picks_bans.hero_id = 0 FK violations (no-hero stub + CHECK)
--   2. picks_bans.team value domain (CHECK 0/1)
--   3. Partition coverage through 2027
--   4. public_matches missing DEFAULT partition
--   5. Redundant timeseries array columns in player_matches
--   6. Missing PKs on event tables (match_objectives/chat/teamfights)
--   7. notable_players.locked_until widening
--   8. Useful supplementary indexes
--
-- Transaction-safe: no CONCURRENTLY, no VACUUM.
-- Idempotent: every change is guarded.
-- =====================================================

-- -----------------------------------------------------
-- 1. Seed "no hero" stub so picks_bans.hero_id = 0 ingests cleanly.
--     OpenDota emits hero_id = 0 for empty ban slots in some modes.
-- -----------------------------------------------------
INSERT INTO heroes (id, name, localized_name)
VALUES (0, 'no_hero', 'No Hero')
ON CONFLICT (id) DO NOTHING;

-- -----------------------------------------------------
-- 2. Domain CHECKs on picks_bans.
-- -----------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'picks_bans_team_check'
    ) THEN
        ALTER TABLE picks_bans
            ADD CONSTRAINT picks_bans_team_check CHECK (team IN (0, 1));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'picks_bans_hero_id_nonneg'
    ) THEN
        ALTER TABLE picks_bans
            ADD CONSTRAINT picks_bans_hero_id_nonneg CHECK (hero_id >= 0);
    END IF;
END$$;

-- -----------------------------------------------------
-- 3. Partition strategy: extend QUARTERLY through 2027.
-- -----------------------------------------------------

-- matches
CREATE TABLE IF NOT EXISTS matches_2026_q1 PARTITION OF matches FOR VALUES FROM (1767225600) TO (1775001600);
CREATE TABLE IF NOT EXISTS matches_2026_q2 PARTITION OF matches FOR VALUES FROM (1775001600) TO (1782864000);
CREATE TABLE IF NOT EXISTS matches_2026_q3 PARTITION OF matches FOR VALUES FROM (1782864000) TO (1790812800);
CREATE TABLE IF NOT EXISTS matches_2026_q4 PARTITION OF matches FOR VALUES FROM (1790812800) TO (1798761600);
CREATE TABLE IF NOT EXISTS matches_2027_q1 PARTITION OF matches FOR VALUES FROM (1798761600) TO (1806624000);
CREATE TABLE IF NOT EXISTS matches_2027_q2 PARTITION OF matches FOR VALUES FROM (1806624000) TO (1814486400);
CREATE TABLE IF NOT EXISTS matches_2027_q3 PARTITION OF matches FOR VALUES FROM (1814486400) TO (1822435200);
CREATE TABLE IF NOT EXISTS matches_2027_q4 PARTITION OF matches FOR VALUES FROM (1822435200) TO (1830384000);

-- player_matches
CREATE TABLE IF NOT EXISTS player_matches_2026_q1 PARTITION OF player_matches FOR VALUES FROM (1767225600) TO (1775001600);
CREATE TABLE IF NOT EXISTS player_matches_2026_q2 PARTITION OF player_matches FOR VALUES FROM (1775001600) TO (1782864000);
CREATE TABLE IF NOT EXISTS player_matches_2026_q3 PARTITION OF player_matches FOR VALUES FROM (1782864000) TO (1790812800);
CREATE TABLE IF NOT EXISTS player_matches_2026_q4 PARTITION OF player_matches FOR VALUES FROM (1790812800) TO (1798761600);
CREATE TABLE IF NOT EXISTS player_matches_2027_q1 PARTITION OF player_matches FOR VALUES FROM (1798761600) TO (1806624000);
CREATE TABLE IF NOT EXISTS player_matches_2027_q2 PARTITION OF player_matches FOR VALUES FROM (1806624000) TO (1814486400);
CREATE TABLE IF NOT EXISTS player_matches_2027_q3 PARTITION OF player_matches FOR VALUES FROM (1814486400) TO (1822435200);
CREATE TABLE IF NOT EXISTS player_matches_2027_q4 PARTITION OF player_matches FOR VALUES FROM (1822435200) TO (1830384000);

-- public_matches
CREATE TABLE IF NOT EXISTS public_matches_2026_q1 PARTITION OF public_matches FOR VALUES FROM (1767225600) TO (1775001600);
CREATE TABLE IF NOT EXISTS public_matches_2026_q2 PARTITION OF public_matches FOR VALUES FROM (1775001600) TO (1782864000);
CREATE TABLE IF NOT EXISTS public_matches_2026_q3 PARTITION OF public_matches FOR VALUES FROM (1782864000) TO (1790812800);
CREATE TABLE IF NOT EXISTS public_matches_2026_q4 PARTITION OF public_matches FOR VALUES FROM (1790812800) TO (1798761600);
CREATE TABLE IF NOT EXISTS public_matches_2027_q1 PARTITION OF public_matches FOR VALUES FROM (1798761600) TO (1806624000);
CREATE TABLE IF NOT EXISTS public_matches_2027_q2 PARTITION OF public_matches FOR VALUES FROM (1806624000) TO (1814486400);
CREATE TABLE IF NOT EXISTS public_matches_2027_q3 PARTITION OF public_matches FOR VALUES FROM (1814486400) TO (1822435200);
CREATE TABLE IF NOT EXISTS public_matches_2027_q4 PARTITION OF public_matches FOR VALUES FROM (1822435200) TO (1830384000);
CREATE TABLE IF NOT EXISTS public_matches_default PARTITION OF public_matches DEFAULT;

-- -----------------------------------------------------
-- 4. Drop redundant timeseries arrays from player_matches.
--     Authoritative data lives in player_timeseries.
-- -----------------------------------------------------
ALTER TABLE player_matches DROP COLUMN IF EXISTS times;
ALTER TABLE player_matches DROP COLUMN IF EXISTS gold_t;
ALTER TABLE player_matches DROP COLUMN IF EXISTS xp_t;
ALTER TABLE player_matches DROP COLUMN IF EXISTS lh_t;
ALTER TABLE player_matches DROP COLUMN IF EXISTS dn_t;
ALTER TABLE player_matches DROP COLUMN IF EXISTS ability_upgrades_arr;
DROP INDEX IF EXISTS idx_pm_final_items_gin;
ALTER TABLE player_matches DROP COLUMN IF EXISTS final_items;

-- -----------------------------------------------------
-- 5. Add primary keys to event tables.
-- -----------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'match_objectives_pkey'
    ) THEN
        ALTER TABLE match_objectives
            ADD COLUMN IF NOT EXISTS id BIGINT GENERATED ALWAYS AS IDENTITY;
        ALTER TABLE match_objectives
            ADD CONSTRAINT match_objectives_pkey PRIMARY KEY (match_id, id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'match_chat_pkey'
    ) THEN
        ALTER TABLE match_chat
            ADD COLUMN IF NOT EXISTS id BIGINT GENERATED ALWAYS AS IDENTITY;
        ALTER TABLE match_chat
            ADD CONSTRAINT match_chat_pkey PRIMARY KEY (match_id, id);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'match_teamfights_pkey'
    ) THEN
        ALTER TABLE match_teamfights
            ADD COLUMN IF NOT EXISTS id BIGINT GENERATED ALWAYS AS IDENTITY;
        ALTER TABLE match_teamfights
            ADD CONSTRAINT match_teamfights_pkey PRIMARY KEY (match_id, id);
    END IF;
END$$;

-- -----------------------------------------------------
-- 6. notable_players.locked_until: widen INT → BIGINT.
-- -----------------------------------------------------
ALTER TABLE notable_players
    ALTER COLUMN locked_until TYPE BIGINT USING locked_until::BIGINT;

-- -----------------------------------------------------
-- 7. Supplementary indexes for hot query paths.
-- -----------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_pm_account_win
    ON player_matches(account_id, win)
    WHERE account_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pm_account_hero_time
    ON player_matches(account_id, hero_id, start_time DESC)
    WHERE account_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_matches_recent_covering
    ON matches(start_time DESC)
    INCLUDE (match_id, radiant_win, duration, leagueid);

CREATE INDEX IF NOT EXISTS idx_matches_series_start
    ON matches(series_id, start_time DESC)
    WHERE series_id IS NOT NULL AND series_id > 0;

CREATE INDEX IF NOT EXISTS idx_team_matches_match
    ON team_matches(match_id);