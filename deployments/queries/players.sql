SELECT 
    np.account_id,
    np.name AS pro_name,
    np.team_name,
    np.team_id,
    np.team_tag,
    np.country_code,
    np.fantasy_role,
    np.is_locked,
    np.is_pro,
    np.locked_until,
    COALESCE(prm.match_ids, '{}'::BIGINT[]) AS match_ids
FROM notable_players np
LEFT JOIN LATERAL (
    SELECT ARRAY_AGG(m.match_id ORDER BY m.start_time DESC) AS match_ids
    FROM player_matches pm
    JOIN matches m ON pm.match_id = m.match_id
    WHERE pm.account_id = np.account_id
      AND m.start_time >= EXTRACT(EPOCH FROM TIMESTAMPTZ '2026-03-24T00:50:59.580Z')
) prm ON TRUE
WHERE np.is_pro = TRUE
  AND np.is_locked = TRUE
ORDER BY np.team_id DESC
LIMIT 50;