-- Tables written by pipeline/03_sink_to_postgis.py. Runs once on an empty data volume.
-- geometry(Point, 4326) = a point in WGS84 lon/lat. The type itself enforces the CRS.

CREATE TABLE poi.dealer (
    dealer_id           text PRIMARY KEY,
    name                text NOT NULL,
    address             text,
    city                text,
    tire_sizes          text[] NOT NULL,
    opening_hours       text,
    geocode_match_level text,
    geocode_confidence  double precision,
    h3_res9             text NOT NULL,
    h3_res7             text NOT NULL,
    geom                geometry(Point, 4326) NOT NULL,
    event_time          timestamptz NOT NULL,
    source              text,
    loaded_at           timestamptz NOT NULL DEFAULT now()
);
-- GiST = spatial index (an R-tree of bounding boxes). Makes "near" / "within" fast.
CREATE INDEX dealer_geom_gix   ON poi.dealer USING gist (geom);
CREATE INDEX dealer_h3_9_idx   ON poi.dealer (h3_res9);
CREATE INDEX dealer_sizes_gin  ON poi.dealer USING gin (tire_sizes);   -- "stocks size X?"

CREATE TABLE poi.restaurant (
    restaurant_id        text PRIMARY KEY,
    name                 text NOT NULL,
    cuisine              text,
    recommendation_score double precision,
    price_band           text,
    geocode_match_level  text,
    geocode_confidence   double precision,
    h3_res9              text NOT NULL,
    h3_res7              text NOT NULL,
    geom                 geometry(Point, 4326) NOT NULL,
    event_time           timestamptz NOT NULL,
    source               text,
    loaded_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX restaurant_geom_gix ON poi.restaurant USING gist (geom);
CREATE INDEX restaurant_h3_9_idx ON poi.restaurant (h3_res9);

-- Privacy by design: geom / h3_res9 may be NULL (no consent); h3_res7 is always there.
CREATE TABLE customer.location (
    customer_id              text PRIMARY KEY,
    consent_precise_location boolean NOT NULL,
    geom                     geometry(Point, 4326),
    h3_res9                  text,
    h3_res7                  text NOT NULL,
    tire_size                text,
    channel                  text,
    event_time               timestamptz NOT NULL,
    loaded_at                timestamptz NOT NULL DEFAULT now(),
    -- the database itself refuses a precise point without consent
    CONSTRAINT precise_only_with_consent CHECK (consent_precise_location OR (geom IS NULL AND h3_res9 IS NULL))
);
CREATE INDEX customer_geom_gix ON customer.location USING gist (geom);
CREATE INDEX customer_h3_7_idx ON customer.location (h3_res7);

-- Reference data: simple sales zones (rectangles) for point-in-polygon demos.
CREATE TABLE ref.sales_zone (
    zone_id text PRIMARY KEY,
    name    text NOT NULL,
    geom    geometry(Polygon, 4326) NOT NULL
);
CREATE INDEX sales_zone_gix ON ref.sales_zone USING gist (geom);
-- ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, srid)
INSERT INTO ref.sales_zone VALUES
  ('Z-CENTRE', 'Lyon Centre',                 ST_MakeEnvelope(4.80, 45.73, 4.88, 45.79, 4326)),
  ('Z-EAST',   'East (Villeurbanne - Bron)',  ST_MakeEnvelope(4.88, 45.70, 5.05, 45.80, 4326)),
  ('Z-WEST',   'West (Ecully - Tassin)',      ST_MakeEnvelope(4.70, 45.73, 4.80, 45.83, 4326)),
  ('Z-SOUTH',  'South (Venissieux - St-Priest)', ST_MakeEnvelope(4.78, 45.65, 4.98, 45.70, 4326)),
  ('Z-NORTH',  'North (Rillieux - Neuville)', ST_MakeEnvelope(4.80, 45.79, 4.95, 45.90, 4326));
