-- Runs once when the PostGIS container starts with an empty data volume.
-- PostGIS adds spatial types (geometry/geography), spatial indexes (GiST)
-- and functions such as ST_DWithin, ST_Distance, ST_Contains.
CREATE EXTENSION IF NOT EXISTS postgis;

-- Separate schemas mirror how a warehouse (e.g. Snowflake) would be organised:
--   poi      = product/location master data (dealers, restaurants)  -> Phase 3
--   customer = consumer data, privacy-controlled                     -> Phase 6
--   ref      = reference geometry (sales zones, regions)             -> Phase 3
CREATE SCHEMA IF NOT EXISTS poi;
CREATE SCHEMA IF NOT EXISTS customer;
CREATE SCHEMA IF NOT EXISTS ref;
