SELECT ARRAY_AGG(match_id ORDER BY start_time DESC) AS match_ids
FROM matches
WHERE start_time >= (EXTRACT(EPOCH FROM NOW() - INTERVAL '1 day'))::BIGINT;