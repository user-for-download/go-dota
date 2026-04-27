-- =====================================================
-- 002_dedup_index.sql — Index on match_id for dedup queries
--
-- The matches PK is (match_id, start_time). match_id is the
-- leading column so the PK already supports lookups by match_id.
-- Add this only if EXPLAIN shows the planner failing to use it.
--
-- Note: Cannot use CONCURRENTLY on partitioned tables.
-- =====================================================

CREATE INDEX IF NOT EXISTS idx_matches_match_id
    ON matches (match_id);