-- FleetConnect demo queries. Run them one by one:
--   docker compose exec postgis psql -U fleetconnect -d fleetconnect
-- then paste a query. Every query uses lon/lat order: ST_MakePoint(lon, lat).
-- The "Snowflake:" lines show the equivalent function names for the cloud warehouse.

-- 1. NEAREST N — 5 dealers closest to Part-Dieu station.
--    `<->` is the KNN operator: it walks the GiST index nearest-first (fast).
--    ::geography makes distances come out in METRES on the real Earth, not degrees.
--    Look closely: the order is not perfectly sorted by metres. `<->` on geometry measures in
--    DEGREES, and at 45°N one degree of longitude is only ~78 km vs ~111 km for latitude.
--    Fine for a shortlist at city scale; use geography (or a projected CRS) when exact order matters.
--    Snowflake: ORDER BY ST_DISTANCE(geo, TO_GEOGRAPHY('POINT(4.859 45.7605)')) LIMIT 5
SELECT dealer_id, name, city,
       round(ST_Distance(geom::geography, ST_SetSRID(ST_MakePoint(4.8590, 45.7605), 4326)::geography)) AS metres
FROM poi.dealer
ORDER BY geom <-> ST_SetSRID(ST_MakePoint(4.8590, 45.7605), 4326)
LIMIT 5;

-- 2. WITHIN RADIUS — dealers within 2 km of Bron that stock 205/55R16.
--    Snowflake: ST_DWITHIN(geo, point, 2000) AND ARRAY_CONTAINS('205/55R16'::VARIANT, tire_sizes)
SELECT dealer_id, name, tire_sizes
FROM poi.dealer
WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(4.9100, 45.7380), 4326)::geography, 2000)
  AND tire_sizes @> ARRAY['205/55R16'];   -- @> uses the GIN index; '...' = ANY(tire_sizes) would not

-- 3. POINT IN POLYGON — which sales zone is each of the first 10 dealers in?
--    Snowflake: ST_CONTAINS(zone.geo, dealer.geo)
SELECT d.dealer_id, d.city, z.name AS zone
FROM poi.dealer d
LEFT JOIN ref.sales_zone z ON ST_Contains(z.geom, d.geom)
ORDER BY d.dealer_id
LIMIT 10;

-- 4. SPATIAL JOIN + PRIVACY — customers per sales zone.
--    Only consented customers have a point, so a point-based join silently undercounts.
--    Compare the two columns: this is the privacy trade-off in numbers.
SELECT z.name AS zone,
       count(c.customer_id)                          AS consented_customers_with_point,
       (SELECT count(*) FROM customer.location)      AS all_customers
FROM ref.sales_zone z
LEFT JOIN customer.location c ON ST_Contains(z.geom, c.geom)
GROUP BY z.name
ORDER BY 2 DESC;

-- 5. H3 AGGREGATION — demand by H3 res7 area (works for ALL customers, consented or not).
--    Areas with fewer than 3 customers are suppressed (k-anonymity: no tiny, identifying groups).
--    Snowflake: H3_POINT_TO_CELL_STRING(geo, 7) computes the same cell id natively.
SELECT h3_res7, count(*) AS customers, count(*) FILTER (WHERE consent_precise_location) AS consented
FROM customer.location
GROUP BY h3_res7
HAVING count(*) >= 3
ORDER BY customers DESC
LIMIT 10;

-- 6. H3 LOOKUP — "dealers in the same res7 area (~5 km2) as dealer D-0001".
--    An equality lookup on a text column: no geometry maths at all, which is why H3 is
--    a cheap shared join key across Kafka, APIs and warehouses. (grid_disk neighbours
--    are computed in the API: GET /h3/cell?...&k=1)
SELECT dealer_id, name, h3_res9
FROM poi.dealer
WHERE h3_res7 = (SELECT h3_res7 FROM poi.dealer WHERE dealer_id = 'D-0001');

-- 7. ROUTE CORRIDOR — restaurants within 1 km of a line from Part-Dieu to the airport,
--    ordered by how far along the route they are (0 = start, 1 = end).
--    Snowflake: ST_DWITHIN(geo, route_geo, 1000)
WITH route AS (SELECT ST_SetSRID(ST_MakeLine(ST_MakePoint(4.8590, 45.7605), ST_MakePoint(5.0800, 45.7260)), 4326) AS g)
SELECT r.name, r.cuisine, r.recommendation_score,
       round(ST_Distance(r.geom::geography, route.g::geography)) AS metres_off_route,
       round(ST_LineLocatePoint(route.g, r.geom)::numeric, 2)    AS position_on_route
FROM poi.restaurant r, route
WHERE ST_DWithin(r.geom::geography, route.g::geography, 1000)
ORDER BY position_on_route;
