-- =====================================================
-- 002_fix.sql — Schema fixes for OpenDota API alignment
--
-- Adds:
--   - Time-series arrays to player_matches (gold_t, xp_t, lh_t, dn_t, times)
--   - draft_timings table (partitioned by hash on match_id)
--   - Quarterly partitions for public_matches
--   - Partition match_advantages by HASH(match_id)
--
-- Idempotent: all statements use IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.
-- =====================================================
DO $$
DECLARE
rec RECORD;
BEGIN
FOR rec IN
SELECT relname
FROM pg_class
WHERE relkind = 'r'
  AND relname LIKE 'matches_p_%'
  AND relname >= 'matches_p_' || to_char(current_date - interval '12 months', 'YYYY_Q')
    LOOP
        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON %I (match_id)',
            rec.relname || '_match_id_idx',
            rec.relname
        );
END LOOP;
END $$;
-- =====================================================
-- 1. Add time-series arrays to player_matches
-- =====================================================
ALTER TABLE player_matches
    ADD COLUMN IF NOT EXISTS gold_t INTEGER[],
    ADD COLUMN IF NOT EXISTS xp_t   INTEGER[],
    ADD COLUMN IF NOT EXISTS lh_t   INTEGER[],
    ADD COLUMN IF NOT EXISTS dn_t   INTEGER[],
    ADD COLUMN IF NOT EXISTS times  INTEGER[];

COMMENT ON COLUMN player_matches.gold_t IS 'Per-minute total gold (from API gold_t)';
COMMENT ON COLUMN player_matches.xp_t   IS 'Per-minute total XP (from API xp_t)';
COMMENT ON COLUMN player_matches.lh_t   IS 'Per-minute last hits (from API lh_t)';
COMMENT ON COLUMN player_matches.dn_t  IS 'Per-minute denies (from API dn_t)';
COMMENT ON COLUMN player_matches.times IS 'Game time (seconds) for each entry in gold_t/xp_t/lh_t/dn_t';

-- =====================================================
-- 2. Create draft_timings table (partitioned by HASH match_id)
-- =====================================================
CREATE TABLE IF NOT EXISTS draft_timings (
    match_id          BIGINT  NOT NULL,
    ord               SMALLINT NOT NULL,
    pick              BOOLEAN  NOT NULL,
    active_team       SMALLINT,
    hero_id           SMALLINT REFERENCES heroes(id),
    player_slot       SMALLINT,
    extra_time        INTEGER,
    total_time_taken  INTEGER,
    PRIMARY KEY (match_id, ord)
) PARTITION BY HASH (match_id);

CREATE TABLE IF NOT EXISTS draft_timings_p0 PARTITION OF draft_timings FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS draft_timings_p1 PARTITION OF draft_timings FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS draft_timings_p2 PARTITION OF draft_timings FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS draft_timings_p3 PARTITION OF draft_timings FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS draft_timings_p4 PARTITION OF draft_timings FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS draft_timings_p5 PARTITION OF draft_timings FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS draft_timings_p6 PARTITION OF draft_timings FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS draft_timings_p7 PARTITION OF draft_timings FOR VALUES WITH (MODULUS 8, REMAINDER 7);

-- =====================================================
-- 3. Add quarterly partitions for public_matches
--    (matching the matches table partitions)
-- =====================================================
CREATE TABLE IF NOT EXISTS public_matches_2024_q1 PARTITION OF public_matches FOR VALUES FROM (1704067200) TO (1711929600);
CREATE TABLE IF NOT EXISTS public_matches_2024_q2 PARTITION OF public_matches FOR VALUES FROM (1711929600) TO (1719792000);
CREATE TABLE IF NOT EXISTS public_matches_2024_q3 PARTITION OF public_matches FOR VALUES FROM (1719792000) TO (1727740800);
CREATE TABLE IF NOT EXISTS public_matches_2024_q4 PARTITION OF public_matches FOR VALUES FROM (1727740800) TO (1735689600);
CREATE TABLE IF NOT EXISTS public_matches_2025_q1 PARTITION OF public_matches FOR VALUES FROM (1735689600) TO (1743465600);
CREATE TABLE IF NOT EXISTS public_matches_2025_q2 PARTITION OF public_matches FOR VALUES FROM (1743465600) TO (1751328000);
CREATE TABLE IF NOT EXISTS public_matches_2025_q3 PARTITION OF public_matches FOR VALUES FROM (1751328000) TO (1759276800);
CREATE TABLE IF NOT EXISTS public_matches_2025_q4 PARTITION OF public_matches FOR VALUES FROM (1759276800) TO (1767225600);
CREATE TABLE IF NOT EXISTS public_matches_2026_q1 PARTITION OF public_matches FOR VALUES FROM (1767225600) TO (1775001600);
CREATE TABLE IF NOT EXISTS public_matches_2026_q2 PARTITION OF public_matches FOR VALUES FROM (1775001600) TO (1782864000);
CREATE TABLE IF NOT EXISTS public_matches_2026_q3 PARTITION OF public_matches FOR VALUES FROM (1782864000) TO (1790812800);
CREATE TABLE IF NOT EXISTS public_matches_2026_q4 PARTITION OF public_matches FOR VALUES FROM (1790812800) TO (1798761600);
CREATE TABLE IF NOT EXISTS public_matches_2027_q1 PARTITION OF public_matches FOR VALUES FROM (1798761600) TO (1806624000);
CREATE TABLE IF NOT EXISTS public_matches_2027_q2 PARTITION OF public_matches FOR VALUES FROM (1806624000) TO (1814486400);
CREATE TABLE IF NOT EXISTS public_matches_2027_q3 PARTITION OF public_matches FOR VALUES FROM (1814486400) TO (1822435200);
CREATE TABLE IF NOT EXISTS public_matches_2027_q4 PARTITION OF public_matches FOR VALUES FROM (1822435200) TO (1830384000);

-- =====================================================
-- 4. Partition match_advantages by HASH(match_id)
--    to match player_match_details scaling
--
-- NOTE: Destructive. Safe only if match_advantages is empty (not yet populated).
-- If data exists, use the rename-and-migrate pattern instead.
-- =====================================================
DROP TABLE IF EXISTS match_advantages;

CREATE TABLE IF NOT EXISTS match_advantages (
    match_id            BIGINT NOT NULL,
    radiant_gold_adv    INTEGER[],
    radiant_xp_adv      INTEGER[],
    PRIMARY KEY (match_id)
) PARTITION BY HASH (match_id);

CREATE TABLE IF NOT EXISTS match_advantages_p0 PARTITION OF match_advantages FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE IF NOT EXISTS match_advantages_p1 PARTITION OF match_advantages FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE IF NOT EXISTS match_advantages_p2 PARTITION OF match_advantages FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE IF NOT EXISTS match_advantages_p3 PARTITION OF match_advantages FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE IF NOT EXISTS match_advantages_p4 PARTITION OF match_advantages FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE IF NOT EXISTS match_advantages_p5 PARTITION OF match_advantages FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE IF NOT EXISTS match_advantages_p6 PARTITION OF match_advantages FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE IF NOT EXISTS match_advantages_p7 PARTITION OF match_advantages FOR VALUES WITH (MODULUS 8, REMAINDER 7);