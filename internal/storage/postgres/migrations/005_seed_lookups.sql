-- =====================================================
-- 005_seed_lookups.sql — reference data: game modes, lobby types
-- Idempotent via ON CONFLICT DO NOTHING.
-- =====================================================

CREATE TABLE IF NOT EXISTS game_modes (
    id   SMALLINT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lobby_types (
    id   SMALLINT PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO game_modes (id, name) VALUES 
    (0,  'unknown'),
    (1,  'all_pick'),
    (2,  'captains_mode'),
    (3,  'random_draft'),
    (4,  'single_draft'),
    (5,  'all_random'),
    (11, 'mid_only'),
    (16, 'captains_draft'),
    (18, 'ability_draft'),
    (22, 'all_draft'),
    (23, 'turbo')
ON CONFLICT (id) DO NOTHING;

INSERT INTO lobby_types (id, name) VALUES 
    (0, 'normal'),
    (2, 'tournament'),
    (5, 'ranked_team'),
    (6, 'ranked_solo'),
    (7, 'ranked')
ON CONFLICT (id) DO NOTHING;