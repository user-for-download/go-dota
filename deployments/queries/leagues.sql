WITH league_recent_matches AS (
    SELECT 
        m.leagueid,
    ARRAY_AGG(m.match_id ORDER BY m.start_time DESC) AS match_ids
    FROM matches m
    WHERE m.start_time >= (EXTRACT(EPOCH FROM TIMESTAMPTZ '2026-03-24T00:50:59.580Z'))::BIGINT
      AND m.leagueid IS NOT NULL
    GROUP BY m.leagueid
)
SELECT 
    l.name,
    l.leagueid,
    l.ticket,
    l.banner,
    l.tier,
    lrm.match_ids
FROM leagues l
INNER JOIN league_recent_matches lrm 
    ON l.leagueid = lrm.leagueid
ORDER BY l.leagueid DESC;