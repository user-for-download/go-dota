-- =====================================================
-- 002_legacy_parsed_data.sql
-- Сохраняем legacy-таблицу для обратной совместимости.
-- Позже данные из неё нужно будет перенести в основные таблицы (matches/player_matches).
-- =====================================================

CREATE TABLE IF NOT EXISTS parsed_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(255) UNIQUE NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_parsed_data_external_id ON parsed_data(external_id);