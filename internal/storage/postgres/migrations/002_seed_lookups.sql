-- =====================================================
-- 005_seed_lookups.sql
-- Lookup tables (game_modes, lobby_types) are populated at runtime
-- by the enricher service. Only the table DDL is kept here.
-- =====================================================

CREATE TABLE IF NOT EXISTS game_modes (
    id   SMALLINT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lobby_types (
    id   SMALLINT PRIMARY KEY,
    name TEXT NOT NULL
);