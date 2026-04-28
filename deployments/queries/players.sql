SELECT COALESCE(ARRAY_AGG(DISTINCT pm.match_id ORDER BY pm.match_id DESC), '{}') AS match_ids
FROM notable_players np
         JOIN player_matches pm ON np.account_id = pm.account_id
         JOIN matches m ON pm.match_id = m.match_id
WHERE np.is_pro = TRUE
  AND np.is_locked = TRUE
  AND m.start_time >= EXTRACT(EPOCH FROM TIMESTAMPTZ '2026-01-01T00:50:59.580Z')::BIGINT;