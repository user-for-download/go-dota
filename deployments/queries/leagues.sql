SELECT COALESCE(ARRAY_AGG(DISTINCT m.match_id ORDER BY m.match_id DESC), '{}') AS match_ids
FROM matches m
         JOIN leagues l ON m.leagueid = l.leagueid
WHERE m.start_time >= EXTRACT(EPOCH FROM TIMESTAMPTZ '2026-03-24T00:50:59.580Z')::BIGINT;