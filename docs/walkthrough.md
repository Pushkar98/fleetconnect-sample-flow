# FleetConnect — Step-by-step walkthrough

A guided tour of diagram 1 in [architecture.md](architecture.md), left to right. For every step:
**what it does → how it works technically → where it is in the code → how to say it in an interview.**

We follow one real dealer, **D-0001**, through the whole system so you can see the record change
shape at each stage.

---

## How to read the diagram

There are six numbered zones and one side box (Kafka UI). Arrows show **which way data moves**.
Dashed arrows are **lookups or monitoring**: nothing is written along them.

There are two paths into the platform:
- **Product data** (dealers, restaurants) is event-driven: ① → ② → Kafka → ④.
- **Customer data** (sign-ups) is workflow-driven: ① → ③ n8n → ⑤ API → Kafka → ④.

Both end up in the same storage (④) and are read only through the API (⑤) by the channels (⑥).

**Why two paths?** They have different needs. Product data is high-volume reference data with many
consumers, which suits a log that many readers can replay. Customer data is personal, low-volume
and full of business rules (consent), which suits a visual workflow that business and privacy
people can read and audit.

---

## ① Sources — `pipeline/01_generate_data.py` → `data/*.geojson`

**What it is.** This stands in for upstream systems: a dealer master-data system and a restaurant
guide. They export files, and we generate those exports.

**GeoJSON.** A standard JSON format for geographic data. A `FeatureCollection` holds `Feature`s,
and each feature has a `geometry` (where) and `properties` (what). D-0001 as the source sends it:

```json
{ "type": "Feature",
  "geometry": { "type": "Point", "coordinates": [4.930506, 45.779054] },
  "properties": { "dealer_id": "D-0001", "name": "Express Tyres Vaulx-en-Velin",
                  "tire_sizes": ["195/65R15","215/60R16", "..."],
                  "geocode_match_level": "rooftop", "geocode_confidence": 0.97 } }
```

**Coordinates and CRS.** `[4.930506, 45.779054]` is **[longitude, latitude]**.
- Longitude is east–west (x); latitude is north–south (y).
- A **CRS (coordinate reference system)** says what the numbers mean. Ours is **WGS84 /
  EPSG:4326**: plain GPS degrees on the Earth's ellipsoid. EPSG:4326 is just its catalogue number.
- GeoJSON *requires* lon/lat order, while many people and libraries say "lat, lon". Mixing them up
  is the most common bug in GIS, which is why the platform standard fixes the order as lon/lat
  everywhere.

**Geocoding quality.** Real dealer data starts as addresses. A **geocoder** turns an address into
coordinates and reports two things:
- a **match level**: `rooftop` (the exact building), `street` (somewhere on the street) or `city`
  (just the town centroid)
- a **confidence** score from 0 to 1

A "city" match puts every dealer in a town on the same dot, which is useless for "nearest dealer".
So we keep both fields and enforce a minimum confidence.

**The deliberate bad records:**

| Id | What's wrong | Why it happens in real life |
|---|---|---|
| D-9001 | `[45.76, 4.86]`, lon and lat swapped | A source system exported lat/lon |
| D-9002 | A Paris point | Valid data, but outside our service region |
| D-9003 | Confidence 0.30 | The geocoder only matched the city |
| D-9004 | Latitude 95 | Impossible value (the maximum is 90) |
| R-9001 | `geometry: null` | Missing data |

**Note:** the source has **no H3 and no WKT**. Raw sources don't produce them; the platform adds
them in step ②. That way enrichment happens once, centrally and to the standard, rather than
differently in every source.

**Interview angle:** "Sources are untrusted. I assume every upstream system will violate the
contract, so the first thing the platform does is a quality gate."

---

## ② Product data — validate, enrich, publish (`pipeline/02_publish_pois.py`)

This is the most important box for the role (Kafka + Avro). Each feature goes through four stages.

### 2a. Validate (data-quality gate)

The rules are in `validate_point()` in `pipeline/common.py` and run **in this order**:

1. **Missing geometry** → `MISSING_GEOMETRY`
2. **Range check:** lon must be −180..180 and lat −90..90 → `INVALID_COORDINATE_RANGE`
3. **Region check:** inside lon 4.6–5.2 and lat 45.6–45.9?
   - If not, the code tries swapping the two numbers. If the swapped point *is* in the region →
     `LIKELY_SWAPPED_LON_LAT`. Otherwise → `OUT_OF_REGION`.
   - This turns a vague error into one that tells the source team exactly what to fix.
4. **Confidence below 0.6** → `LOW_GEOCODE_CONFIDENCE`

The order matters: the cheap structural checks run first, and the business rules last.

Invalid records **are not dropped**. They go to **`poi.rejected.v1`**, a **dead-letter topic
(DLQ)**: a separate topic for messages that failed processing, kept with the reason so someone can
fix and replay them.

```json
{"source_topic": "poi.dealer.v1", "reason": "LIKELY_SWAPPED_LON_LAT", "record": { "...original feature..." }}
```

The DLQ is JSON rather than Avro on purpose. A broken record might not fit the schema at all, and
the dead-letter topic must be able to accept *anything*.

### 2b. Enrich (platform standards)

The producer adds three fields to every valid record.

**`geometry_wkt`: `"POINT(4.930506 45.779054)"`.** **WKT (Well-Known Text)** is a standard text
format for geometry. Its order is "x y" = lon lat. We include it because any consumer (Snowflake
`TO_GEOGRAPHY`, PostGIS `ST_GeomFromText`, Python shapely) can load it without custom parsing.

We *also* keep `lon` and `lat` as plain doubles, so simple consumers don't need a geometry parser
at all. For points that duplication is cheap and practical. For lines and polygons, WKT is the only
sensible option.

**`h3_res9` and `h3_res7`: H3.** H3 is Uber's open-source global grid of hexagons:
- The whole Earth is tiled with hexagons at 16 **resolutions** (0 = huge, 15 = tiny).
- Every point falls in exactly one cell at each resolution.
- Each cell has a 15-character id such as `891f90242afffff`.

| Resolution | Average area | Edge length | We use it for |
|---|---|---|---|
| 9 | ~0.1 km² | ~175 m | proximity ("what's in my block") |
| 7 | ~5.2 km² | ~1.2 km | analytics and privacy (a neighbourhood) |

**Why hexagons?**
- Every neighbour is the same distance from the centre: a hexagon has 6 equal neighbours, while a
  square has 4 edge neighbours and 4 corner neighbours.
- That makes "the ring of cells around me" (`grid_disk`) a good stand-in for a circle.

**Why H3 at all, when we have real geometry?**
- A cell id is **just a string**, so equality joins and `GROUP BY` work in any system: Kafka keys,
  Redis, Snowflake, a CSV.
- No geometry engine is needed.
- It's the "shared location key" across teams.

**Parent/child.** H3 has an aperture of 7: each cell has about 7 children at the next resolution
down. That puts roughly 49 res9 cells inside a res7 cell. Because hexagons can't nest perfectly,
children are *approximately* contained in their parent. We compute res9 from the point and then
`cell_to_parent(res9, 7)`, so the two values always agree.

**The lat-first gotcha.** `h3.latlng_to_cell(lat, lon, 9)` takes **latitude first**. Our standard
is lon/lat, so `h3_fields()` swaps the order in exactly one place, with a comment. Snowflake's H3
functions differ too. Put the conversion at the edge, once.

D-0001 after enrichment:

```json
{ "dealer_id": "D-0001", "lon": 4.930506, "lat": 45.779054,
  "geometry_wkt": "POINT(4.930506 45.779054)",
  "h3_res9": "891f90242afffff", "h3_res7": "871f90242ffffff",
  "geocode_match_level": "rooftop", "geocode_confidence": 0.97,
  "event_time": 1794555585015, "source": "dealer-master-export" }
```

`event_time` records when the change happened; the sink uses it in step ④. `source` is
**lineage**: where the record came from.

### 2c. Serialize with Avro + Schema Registry

**Avro** is a compact binary format whose schema is defined in JSON (`schemas/poi_dealer.avsc`).
- The binary message holds **only the values**, with no field names, so it's much smaller than JSON.
- To read it you need the schema it was **written** with.

The **Schema Registry** is a separate service that stores schemas and gives each one a numeric
**id**.

When the producer serializes D-0001:
1. `AvroSerializer` checks with the registry under the **subject** `poi.dealer.v1-value`. The
   default naming rule is *topic name + `-value`*. If the schema is new, it's registered
   (auto-register is on) and gets an id. The id is cached, so this happens once per producer, not
   once per message.
2. The message bytes follow the **Confluent wire format**:

```
[0x00][4-byte schema id][Avro binary body]
 magic   e.g. id = 1      values only, in schema field order
```

Every message therefore carries a pointer to its exact schema. A consumer reading a message from
two years ago gets the id and fetches the right schema. That's what makes schema evolution safe.

**Avro over JSON or Protobuf:**
- Avro has the most mature registry and compatibility tooling in the Kafka ecosystem.
- Its schemas are JSON, so they're readable in pull requests.
- Its **defaults** are what make evolution work.
- Protobuf is a valid alternative (it has a registry too, and is popular with gRPC teams).
- Plain JSON has no enforced contract.

The `doc` fields in the `.avsc` files are part of the contract. They state the CRS, units, and when
a field is null. A schema documents *meaning* as well as types.

### 2d. Publish to Kafka

```python
Producer({"enable.idempotence": True, "acks": "all"})
producer.produce(topic, key=record["dealer_id"], value=avro_bytes)
```

- **The key is `dealer_id`.** Kafka hashes the key to choose a **partition**, so every event for
  D-0001 always goes to the same partition. **Kafka only guarantees order within a partition**, so
  per-dealer ordering is guaranteed.
  - Gotcha worth knowing: our Python client (librdkafka) hashes keys with CRC32 by default, while
    the Java client uses murmur2. The same key can land in *different* partitions depending on the
    client language. In a mixed estate, set `partitioner=murmur2_random` for consistency.
- **`acks=all`:** the broker confirms only once all in-sync replicas have the message. With one
  broker locally that's trivial; in production with replication factor 3 it's your durability
  guarantee.
- **`enable.idempotence=True`:** the producer gets a producer id and numbers each message per
  partition. If a network blip causes a retry, the broker spots the duplicate number and discards
  it. Retries then can't create duplicates or reorder messages.
- **`flush()`** at the end blocks until every message is confirmed. `produce()` itself is
  asynchronous and only buffers.

**Interview angle:** "The producer is where standards get enforced: validation, enrichment and the
contract. Anything that fails goes to a dead-letter topic with a machine-readable reason, never
silently dropped."

### 2e. Schema evolution (`pipeline/02b_schema_evolution_demo.py`)

Our registry is set to **BACKWARD** compatibility (in `docker-compose.yml`): *a consumer using the
new schema must be able to read data written with the old one.*

| Change | Result | Why |
|---|---|---|
| Add `dealer_tier` **with a default** | Accepted | An old message lacks the field, so the new reader fills in the default |
| Add `dealer_tier` **without a default** | **Rejected, HTTP 409** | An old message lacks the field and the new reader has nothing to fill in (`READER_FIELD_MISSING_DEFAULT_VALUE`) |
| Change `geocode_confidence` from double to string | Rejected | The new reader can't read old doubles as strings |

What BACKWARD means operationally:
- **Upgrade consumers first, then producers.** New consumers can read both old and new data.
- Deleting a field is allowed under BACKWARD, because the new reader just ignores it. It can break
  *old* consumers, though, which is what FORWARD protects against.
- **FULL** = both directions, the safest but the most restrictive.
- **`_TRANSITIVE`** variants check against *every* past version, not just the latest. That matters
  when consumers replay old data, as compacted topics do.

A genuinely breaking change means a **new topic** (`poi.dealer.v2`), running both topics side by
side during migration, and then retiring v1. That's why the topic names end in `.v1`.

**Interview angle:** "Schemas are contracts, and the registry is the enforcement point. In CI I'd
run the same compatibility check before merge, so a breaking change fails the pull request, not
production."

---

## Kafka itself (the box inside ②)

**KRaft.** Kafka used to need a separate ZooKeeper cluster for metadata. In **KRaft mode** Kafka
manages metadata itself, using an internal Raft-based quorum of *controller* nodes that write to a
`__cluster_metadata` log. Our single node is both `controller` and `broker`
(`KAFKA_PROCESS_ROLES: broker,controller`). In production you'd run 3 dedicated controllers plus
several brokers.

**Listeners.** This is the most confusing part of local Kafka.
- A client connects to a **bootstrap** address. The broker then tells it which **advertised**
  address to use from then on.
- Containers must reach it as `kafka:9092`; your laptop must use `localhost:9094`.
- So there are two listeners, `INTERNAL` and `EXTERNAL`, each advertising the right name. Getting
  this wrong causes the classic "connects, then hangs" symptom.

**Topics** (created as code in `ensure_topics()`, because broker auto-create is **off**):

| Topic | Partitions | Cleanup | Why |
|---|---|---|---|
| `poi.dealer.v1` | 3 | **compact** | Master data: we want the *latest state per key* |
| `poi.restaurant.v1` | 3 | compact | Same |
| `customer.location.v1` | 3 | delete, 7 days | Personal data: keep it short |
| `poi.rejected.v1` | 1 | delete | Low volume, for operations |

- **Partitions** are the unit of parallelism: at most 3 consumers in a group can read a
  3-partition topic in parallel.
- **Compaction:** Kafka's log cleaner eventually removes older events for the same key and keeps
  the latest. The topic becomes a replayable "current table": a new consumer reads it from the
  start and ends up with every dealer's current state.
  - Sending a message with the key and a `null` value (a **tombstone**) deletes that key.
  - Locally you won't *see* compaction happen. It only runs on closed log segments, and the
    defaults are large.
- **Delete with retention** (customer topic): data older than 7 days is removed. That's
  privacy-friendly, a form of data minimisation.
- **Replication factor 1** locally. In production use **RF=3 with `min.insync.replicas=2`**: you
  can lose one broker with no data loss and keep writing.

The Schema Registry stores its own data in a compacted Kafka topic, `_schemas`. That's why it
depends on Kafka in the compose file.

**Interview angle:** "Topic design is key choice (ordering), partitions (parallelism), cleanup
policy (state vs events), retention (cost and privacy) and naming with versions (evolution).
Auto-create is off, so topics are governed like code."

---

## ③ Consumer data — the n8n consent gate (`n8n/customer-consent.json`)

**n8n** is a self-hosted workflow automation tool. You build flows from nodes on a canvas; each run
is an **execution** you can inspect step by step in the UI.

The flow, node by node:
1. **Webhook "Customer sign-up"**
   - `POST /webhook/customer-signup` receives
     `{customer_id, lon, lat, consent_precise_location, tire_size, channel}`.
   - `responseMode: lastNode` means the HTTP caller gets back the output of the last node that ran,
     here the API's response. So the simulator can print exactly what was published.
2. **IF "Consent to precise location?"**
   - A strict boolean check on `$json.body.consent_precise_location`.
   - `$json` is the data coming in from the previous node.
3. **True branch → "Publish precise point"**
   - Sends `POST http://api:8000/ingest/customer-location` with lon/lat.
4. **False branch → "Coarsen to H3 res7 area" → "Publish area only"**
   - First node: `GET /h3/cell?lon&lat&res=7` returns `{cell: "871f90209ffffff"}`.
   - Second node: builds a payload with **only** `h3_res7`. It reads the other fields back from the
     first node with `$('Customer sign-up').item.json.body...`, n8n's syntax for referring to an
     earlier node's data.
   - The raw coordinates **never leave the workflow**.

The two outcomes, from a real run:

```
consent=true   on Kafka: lon 4.935234, lat 45.774775, h3_res9 891f…, h3_res7 871f…
consent=false  on Kafka: lon null, lat null, h3_res9 null, h3_res7 871f90209ffffff
```

**Why n8n calls the API instead of writing to Kafka directly.** This is a deliberate architecture
decision, and a good one to defend:
- The API **owns the contract**: Avro serialization, the schema, validation. n8n doesn't need Kafka
  credentials, Avro libraries or schema knowledge.
- If you swap n8n for another tool, the contract stays put.
- This is the **API-led ingestion pattern**: workflow tools orchestrate, and platform APIs own the
  data contracts.

**Privacy by design: "defence in depth".** No single layer is trusted:
1. **n8n** removes the coordinates in the no-consent branch.
2. **The API** returns `422` if coordinates arrive with `consent=false`, and requires a valid
   resolution-7 H3 cell.
3. **The database** has a `CHECK` constraint: `consent OR (geom IS NULL AND h3_res9 IS NULL)`. Even
   a buggy future consumer can't store a point without consent.

**What "coarsening" means.** Instead of an exact point, we keep only the ~5 km² hexagon it falls
in. That's still useful for analytics ("demand in this area") but not identifying. It's also
**data minimisation** in GDPR terms: collect only what the purpose needs.

**Production n8n:**
- **Queue mode:** a main instance plus workers, with Redis as the queue.
- Workflows exported to git and promoted through environments.
- Credentials in n8n's encrypted store or a vault.
- An **error workflow** to catch failures.
- **Retries** on HTTP nodes.

**Interview angle:** "n8n is good for business-rule orchestration that non-engineers need to read:
consent, enrichment calls, notifications. I wouldn't use it for high-throughput streaming. That's
Kafka's job."

---

## ④ Storage — sink + PostGIS

### The sink (`pipeline/03_sink_to_postgis.py`, runs as the `sink` container)

**Consumer groups and offsets:**
- The sink joins the **consumer group** `postgis-sink`. Kafka splits the partitions among the
  group's members; with one member, it reads all of them.
- Kafka tracks progress per group with **offsets**: "this group has processed partition 2 up to
  message 57".
- `auto.offset.reset=earliest` means a brand-new group starts at the beginning of each topic.

**The loop, per message:**
1. `poll()` fetches a message.
2. `AvroDeserializer` reads the 5-byte header, takes the schema id and fetches the **writer's
   schema** from the registry (cached), then decodes the bytes into a dictionary.
3. It runs the **UPSERT** for that topic.
4. It **commits the database transaction**.
5. Only then does it **commit the Kafka offset** (`enable.auto.commit=False`, then a manual commit).

**Delivery semantics.** Committing *after* the database write gives **at-least-once** delivery.
- If the sink crashes between steps 4 and 5, the message is processed again on restart.
- That's safe because the write is an **idempotent UPSERT**
  (`INSERT … ON CONFLICT (dealer_id) DO UPDATE`): processing the same message twice produces the
  same row.
- If you committed the offset *first*, a crash would lose data (at-most-once).
- "Exactly-once" end to end in practice usually means **at-least-once plus idempotent writes**,
  which is what we have.

**Out-of-order protection.** The upsert has `WHERE poi.dealer.event_time <= EXCLUDED.event_time`.
If an older event arrives after a newer one (after a replay, or from a slow source), the older one
doesn't overwrite. This is a **last-writer-wins on event time** guard.

**Trade-off to mention:** committing every message synchronously is simple but slow. In production
you'd batch (for example, commit every 500 messages or every second), or use **Kafka Connect** sinks
or **Snowpipe Streaming** for Snowflake.

### PostGIS (`db/init/02_tables.sql`)

**PostGIS** is a PostgreSQL extension that adds spatial types, spatial indexes and around 300
`ST_*` functions (ST = "spatial type"). Here it stands in for Snowflake, which has equivalent
`GEOGRAPHY` types, `ST_*` functions and native `H3_*` functions.

**Database schemas:**
- `poi` (product data)
- `customer` (personal data, which could have separate access roles)
- `ref` (reference geometry)

This separation mirrors how you'd split databases or schemas in Snowflake for access control.

**`geometry(Point, 4326)`:**
- The column type enforces both the shape (Point) and the CRS (SRID 4326). Insert a polygon or a
  different SRID and it's refused.
- The sink builds the point with `ST_SetSRID(ST_MakePoint(lon, lat), 4326)`. `MakePoint` is
  (x, y) = (lon, lat), our standard again.

**Indexes:**

| Index | Type | Serves |
|---|---|---|
| `dealer_geom_gix` | **GiST** on `geom` | "near", "within", "contains" queries |
| `dealer_h3_9_idx` | B-tree on `h3_res9` | H3 equality lookups |
| `dealer_sizes_gin` | **GIN** on `tire_sizes` | "stocks size X" array searches |

- **GiST** works like an **R-tree**: it groups nearby points into bounding boxes, then groups of
  boxes into bigger boxes, so a spatial search skips whole regions.
- **GIN** is an inverted index for array membership.

**Geometry vs geography.** This is a concept interviewers love.
- **`geometry`** does maths on a **flat plane** in the CRS's units. For 4326 those units are
  **degrees**, so a distance of `0.01` means nothing useful.
- **`geography`** does maths on the **Earth's curved surface**, with results in **metres**.
- Our queries cast `geom::geography` whenever they need metres.
- At 45°N, 1° of longitude ≈ 78 km but 1° of latitude ≈ 111 km. Degree "distances" are therefore
  distorted, as step ⑤ shows.

**Interview angle:** "The database is the last line of defence for both quality and privacy. The
types enforce the CRS, constraints enforce consent, and upserts make loading idempotent."

---

## ⑤ The API layer (`api/main.py`)

**API-led architecture** here means every channel (web, n8n, agent) goes through one documented
interface and never touches the database or Kafka. You get one place for validation, privacy
rules, caching, auth, rate limits and audit, and you can change storage (PostGIS → Snowflake)
without breaking any channel.

FastAPI generates **OpenAPI** documentation automatically at `/docs`. Every response is
**GeoJSON**, so any map library or GIS tool can use it directly.

The endpoints and what each does technically:

**`GET /dealers/nearest?lon&lat&tire_size`.** A two-step nearest-neighbour search.
1. The **KNN operator `<->`** uses the GiST index to return candidates nearest-first *in degrees*.
   It's very fast, but slightly wrong because of the longitude distortion. Shortlist `limit × 4`.
2. Re-sort that shortlist by `ST_Distance(geography)` in **true metres**.

Degree order alone gave 598, 735, 664 m; the corrected order gives 598, 664, 723, 735, 898 m, and
the true 5th-nearest dealer changed. (`db/queries.sql` query 1 keeps the degree-only version on
purpose, to show the effect.) The `tire_size` filter uses `= ANY(tire_sizes)`, served by the GIN
index.

**`POST /restaurants/along-route`.**
- `ST_GeomFromGeoJSON` turns the route into a line.
- `ST_DWithin(p.geom::geography, route::geography, 1000)` keeps restaurants within 1000 real metres
  of *any part* of the line, which is the "corridor".
- `ST_LineLocatePoint` returns 0–1, *how far along* the route each restaurant is. Results are sorted
  that way, so they come back in driving order.
- `ST_Buffer(route::geography, 1000)` builds the corridor polygon for the map. PostGIS temporarily
  projects to a local flat CRS, buffers in metres, and projects back.
- Honest caveat: casting to geography means the geometry GiST index isn't used. That's fine for 150
  rows. At scale you'd pre-filter with an index-friendly bounding box, add a geography index, or
  use a projected CRS in metres (for France, Lambert-93, EPSG:2154).

**`GET /route?from&to`.** A **mock** routing engine: a straight line with 13 points. In production
this would be OSRM or Valhalla (open-source routing engines), which follow real roads. The mock
keeps the interface realistic and is `lru_cache`d, a simple in-memory cache for repeated requests.

**`GET /h3/cell?lon&lat&res&k`.** Returns:
- the cell id, area and boundary polygon (`cell_to_boundary`, with lat/lng flipped back to
  GeoJSON's lng/lat)
- the res7 parent
- optionally `grid_disk(cell, k)`: the cell plus all cells within k steps (k=1 gives 7 cells).

n8n uses it for coarsening; the map uses it to draw your cells.

**`GET /analytics/demand-h3`.**
- `GROUP BY h3_res7` over *all* customers: consented customers have an h3_res7, and so do
  non-consented ones. A point-based join would see only the consented customers, while H3
  aggregation sees all of them. `db/queries.sql` query 4 shows that gap in numbers.
- Then **k-anonymity suppression**: areas with fewer than 3 customers are hidden. Publishing
  "1 customer in this hexagon" could identify someone.

**`POST /ingest/customer-location`.** The write path described in step ③. It validates, enforces
consent, serializes Avro and publishes to `customer.location.v1` with an idempotent producer.

**`POST /agent/ask`.** Hosts the agent loop (step ⑥). The agent calls the other endpoints over HTTP
like any other client.

**Interview angle:** "System, process and experience APIs, in MuleSoft terms: `/ingest` is a system
API over Kafka; `/restaurants/along-route` is a process API combining data with spatial logic; the
map's calls are experience-level. APIs and events coexist: events distribute *state*, APIs answer
*questions*."

---

## ⑥ Channels

### Web map (`web/index.html`)

**MapLibre GL JS** is an open-source WebGL map renderer. The page:
- uses **raster tiles** from OpenStreetMap for the base map (pre-drawn 256×256 PNG images, fetched
  by zoom/x/y)
- adds GeoJSON **sources** (data) and **layers** (how to draw them: circles for points, fills for
  hexagons, lines for routes)

Every `fetch()` in the file maps to one API endpoint. The page never sees Kafka or PostGIS.

What each interaction shows:
- **Click the map:** your res9 cell (small, solid outline) and its res7 parent (large, dashed), plus
  lines to the 5 nearest dealers. H3's hierarchy, made visible.
- **Demand layer:** res7 hexagons coloured by customer count. Small areas are missing because of
  k-anonymity.
- **Route:** the dashed line, the translucent corridor polygon, and the restaurants inside it.
- **Agent:** the full trace in the sidebar, and its result drawn on the map.

### AI agent (`agent/agent.py`)

This is the **tool-use loop** that real LLM agents use, with the model mocked:

```
loop (max 6 steps):
    decision = mock_model(question, history)     # the only mocked part
    if end_turn  -> return answer
    if tool_use  -> call_tool() via HTTP -> append result to history
```

Concepts this shows:
- **Tools as API contracts.** `TOOLS` has a name, a description and a JSON-schema `input_schema`:
  exactly the format LLM APIs take. Each tool maps to one public endpoint.
- **Tool boundary.** The agent can't query the database or Kafka. It inherits the API's validation
  and privacy rules, so an agent can't leak what the API won't give out.
- **Grounding.** The answer is built only from tool results; it can't invent restaurants.
- **Auditability.** Every thought, tool call and result is in the trace.
- **Step limit.** Stops runaway loops (and cost) in a real agent.
- **Swap-ability.** Replace `mock_model()` with one LLM call using the same `TOOLS` list, and
  nothing else changes.

Example run: "restaurants from Part-Dieu to the airport"
1. `plan_route` → a 17.6 km line
2. `find_restaurants_along_route` with a 1 km corridor → results
3. `end_turn` → the top 3 by score

**Interview angle:** "For agentic AI my architecture rule is: agents are just another channel. They
get least-privilege tools over governed APIs, every call is logged, and the data platform's
guarantees apply automatically."

---

## Kafka UI (side box, dashed)

This is a read-only window onto Kafka and the registry: topics, messages (Avro decoded via the
registry), schemas and versions, and **consumer groups**.

The metric to know is **consumer lag**: the newest offset in a partition minus the group's committed
offset, i.e. how many messages behind the consumer is. When you run the sign-ups you can watch
`postgis-sink` lag rise and fall back to 0.

In production you'd alert when lag keeps growing (the consumer is stuck or too slow). You'd also
watch under-replicated partitions, producer error rates and DLQ volume.

---

## D-0001's whole journey in one line

`data/dealers.geojson` (raw lon/lat) → **validated** (in region, confidence 0.97) → **enriched**
(`891f90242afffff` / `871f90242ffffff`, WKT) → **Avro**, with registry schema id 1 → **Kafka**
`poi.dealer.v1`, key `D-0001`, a fixed partition → **sink** decodes and upserts → **PostGIS**
`poi.dealer` row with `geometry(Point,4326)` → **API** `/dealers/nearest` → **map / agent** shows it.

**Practice:** take one sign-up from step ③ and trace its journey the same way, out loud. If you can
do both, you can explain the whole platform.
