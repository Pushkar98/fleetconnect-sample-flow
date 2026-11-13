# CLAUDE.md — FleetConnect

## What this is

A **lean learning and demo project** for a Data & Integration Architect interview (CX ecosystem
with a GIS team). The owner is new to GIS. The aim is to understand and visualise every concept,
then explain it from the diagram and the code. It is **not** a production build.

## Rules

- **Keep it small.** Add nothing (services, scripts, docs, tooling) unless it teaches or shows a
  concept. Ask before adding.
- **100% local.** No licences, accounts or API keys. The AI agent stays mocked (`mock_model()`).
- **The diagram is the source of truth.** `docs/architecture.md` must name every container, topic,
  table, endpoint and the file that implements it. Update it in the same change.
- Pin exact versions (Docker images, Python packages). Docker budget is ~4 GB RAM / 2 CPUs.
- No real brands or proprietary datasets. All data synthetic; OSM tiles with attribution.
- Explain in plain words, defining every GIS term. Show a real record when discussing data.

## Platform standards (all live in `pipeline/common.py`)

- WGS84 / EPSG:4326, **lon, lat** order everywhere; comment wherever a library is lat-first (h3).
- Every location carries `h3_res9` (proximity) and `h3_res7` (analytics, privacy).
- Avro geometry = WKT + lon/lat doubles + H3. Schemas in `/schemas`, BACKWARD compatibility.
- Topics: `poi.dealer.v1`, `poi.restaurant.v1` (compacted), `customer.location.v1`, `poi.rejected.v1`.
- PostGIS `geometry(Point,4326)` + GiST. Customer point only with consent (n8n, API and DB all enforce it).
- Every channel (web map, n8n, agent) goes through the Location API, never the DB or Kafka directly.

## Layout

See README.md. Steps are numbered in `pipeline/` (01 generate → 02 publish → 03 sink → 04 sign-ups).
