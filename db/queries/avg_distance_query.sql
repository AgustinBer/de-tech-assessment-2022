SELECT AVG(ST_Distance(ST_MakePoint(lng, lat), ST_MakePoint(prev_lng, prev_lat)) / 1000) as avg_distance
FROM (
    SELECT
        event,
        vehicle,
        organization,
        start,
        finish,
        lat,
        lng,
        LAG(lat) OVER (PARTITION BY vehicle, organization, start, finish ORDER BY timestamp) as prev_lat,
        LAG(lng) OVER (PARTITION BY vehicle, organization, start, finish ORDER BY timestamp) as prev_lng
    FROM events
    WHERE event = 'update'
) as updates
WHERE prev_lat IS NOT NULL AND prev_lng IS NOT NULL;
