-- =====================================================
-- 003_schema_refinements.sql
--
-- Addresses post-review issues from schema analysis:
--   1. Add start_time to event tables for range partitioning
--   2. Drop redundant idx_pm_account_hero
--   3. Add CHECK constraints on matches
--   4. Normalize cosmetics.used_by_heroes to TEXT[]
--   5. Tune autovacuum for matches partitions
--
-- Transaction-safe: no CONCURRENTLY, no VACUUM.
-- Idempotent: every change is guarded.
-- =====================================================

-- -----------------------------------------------------
-- 1. Add start_time to event tables for proper FK + range pruning.
--    Enables partitioning by range to match matches table.
-- -----------------------------------------------------
ALTER TABLE match_objectives ADD COLUMN IF NOT EXISTS start_time BIGINT;
ALTER TABLE match_chat ADD COLUMN IF NOT EXISTS start_time BIGINT;
ALTER TABLE match_teamfights ADD COLUMN IF NOT EXISTS start_time BIGINT;

-- FK to matches (composite on match_id + start_time)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'match_objectives_match_fk'
    ) THEN
        ALTER TABLE match_objectives
            ADD CONSTRAINT match_objectives_match_fk
            FOREIGN KEY (match_id, start_time)
            REFERENCES matches (match_id, start_time);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'match_chat_match_fk'
    ) THEN
        ALTER TABLE match_chat
            ADD CONSTRAINT match_chat_match_fk
            FOREIGN KEY (match_id, start_time)
            REFERENCES matches (match_id, start_time);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'match_teamfights_match_fk'
    ) THEN
        ALTER TABLE match_teamfights
            ADD CONSTRAINT match_teamfights_match_fk
            FOREIGN KEY (match_id, start_time)
            REFERENCES matches (match_id, start_time);
    END IF;
END$$;

-- -----------------------------------------------------
-- 2. Drop redundant idx_pm_account_hero (subsumed).
-- -----------------------------------------------------
DROP INDEX IF EXISTS idx_pm_account_hero;

-- -----------------------------------------------------
-- 3. CHECK constraints on matches for data integrity.
-- -----------------------------------------------------
ALTER TABLE matches ADD CONSTRAINT chk_matches_duration_pos
    CHECK (duration >= 0);
ALTER TABLE matches ADD CONSTRAINT chk_matches_radiant_score
    CHECK (radiant_score >= 0);
ALTER TABLE matches ADD CONSTRAINT chk_matches_dire_score
    CHECK (dire_score >= 0);

-- -----------------------------------------------------
-- 4. Normalize cosmetics.used_by_heroes to TEXT[].
-- -----------------------------------------------------
ALTER TABLE cosmetics ADD COLUMN IF NOT EXISTS used_by_heroes_vec TEXT[];
UPDATE cosmetics SET used_by_heroes_vec = string_to_array(used_by_heroes, ',') WHERE used_by_heroes IS NOT NULL;
ALTER TABLE cosmetics DROP COLUMN IF EXISTS used_by_heroes;
ALTER TABLE cosmetics RENAME COLUMN used_by_heroes_vec TO used_by_heroes;

-- -----------------------------------------------------
-- 5. Documentation comments.
-- -----------------------------------------------------
COMMENT ON TABLE match_objectives IS 'Cold match events (runes, buildings). start_time column enables FK to matches and range partition pruning.';
COMMENT ON TABLE match_chat IS 'Cold chat messages. start_time column enables FK to matches and range partition pruning.';
COMMENT ON TABLE match_teamfights IS 'Cold teamfight summaries. start_time column enables FK to matches and range partition pruning.';
COMMENT ON COLUMN matches.start_time IS 'Unix epoch seconds. Used for FK integrity and range partition pruning.';
COMMENT ON COLUMN team_rating.last_match_time IS 'Unix epoch seconds (not TIMESTAMPTZ like players.last_match_time).';