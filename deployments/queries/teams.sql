SELECT COALESCE(ARRAY_AGG(DISTINCT tm.match_id ORDER BY tm.match_id DESC), '{}') AS match_ids
FROM teams t
         JOIN team_rating tr ON t.team_id = tr.team_id
         JOIN team_match tm ON t.team_id = tm.team_id
         JOIN matches m ON tm.match_id = m.match_id
WHERE m.start_time >= EXTRACT(EPOCH FROM TIMESTAMPTZ '2026-03-24T00:50:59.580Z')::BIGINT;