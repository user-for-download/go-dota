SELECT
  t.team_id,
  t.name,
  t.tag,
  t.logo_url,
  tr.rating,
  tr.wins,
  tr.losses,
  tr.last_match_time,
  COALESCE(trm.match_ids, '{}'::BIGINT[]) AS match_ids
FROM teams t
JOIN team_rating tr ON t.team_id = tr.team_id
LEFT JOIN LATERAL (
    SELECT ARRAY_AGG(m.match_id ORDER BY m.start_time DESC) AS match_ids
    FROM team_match tm
    JOIN matches m ON tm.match_id = m.match_id
    WHERE tm.team_id = t.team_id
      AND m.start_time >= EXTRACT(EPOCH FROM TIMESTAMPTZ '2025-01-01T00:00:00Z')
) trm ON TRUE
WHERE tr.rating >= 1200
ORDER BY tr.rating DESC
LIMIT 50;