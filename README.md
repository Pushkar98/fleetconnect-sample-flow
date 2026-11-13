# FleetConnect

A small, fully local reference platform for **location-aware consumer experience**, built to
learn and demonstrate data and integration architecture:

- **Product data, event-driven:** Kafka + Avro + Schema Registry, with a data-quality gate and a dead-letter topic
- **Consumer data, workflow-driven:** n8n consent workflow; precise location only with consent
- **Storage:** PostGIS (stand-in for Snowflake) with spatial indexes and H3 columns
- **API layer:** one Location API that every channel uses
- **Channels:** a web map and an AI agent (mocked model, real tool-use pattern)

No cloud accounts, licences or API keys. Everything runs in Docker.

**Start with the map of the system: [docs/architecture.md](docs/architecture.md)**, then the
step-by-step technical tour: [docs/walkthrough.md](docs/walkthrough.md)

## Run it (about 5 minutes)

Docker Desktop with ≥ 4 GB RAM and ~6 GB free disk.

```bash
# 0. start the platform (7 containers)
docker compose up -d --build

# 1. generate source data              -> look at data/dealers.geojson
docker compose exec api python pipeline/01_generate_data.py

# 2. validate + publish to Kafka       -> Kafka UI: Topics, Schema Registry
docker compose exec api python pipeline/02_publish_pois.py
docker compose exec api python pipeline/02b_schema_evolution_demo.py

# 3. nothing to run: the `sink` container loads Kafka into PostGIS continuously
docker compose logs sink

# 4. load the n8n consent workflow (once), then send 300 sign-ups through it
docker compose exec n8n n8n import:workflow --input=/home/node/workflows/customer-consent.json
docker compose exec n8n n8n update:workflow --id=FcConsentFlow001 --active=true
docker compose restart n8n
docker compose exec api python pipeline/04_send_signups.py

# 5. explore
open http://localhost:8000                     # web map + agent
docker compose exec api python agent/agent.py "restaurants from Part-Dieu to the airport"
docker compose exec postgis psql -U fleetconnect -d fleetconnect   # paste queries from db/queries.sql
```

### Or do it all by clicking

Open http://localhost:8000 → **Produce & test** tab:
- **Live pipeline:** messages per topic, sink lag, PostGIS rows, schema versions (every 2 s)
- **Click the map to produce:** add a dealer (try low confidence, swapped lon/lat, or outside the
  dashed Lyon box to see rejections) or a customer sign-up (with or without consent) through n8n
- **Run pipeline steps:** the same scripts as above, with their console output
- **Peek at Kafka:** newest messages with partition, offset, key and schema id

Only step 4's n8n import (three commands, once) needs the terminal.

Stop with `docker compose down` (keeps data). Use `docker compose down -v` for a full reset.

## What to open

| URL                             | What you see                                               |
|---------------------------------|------------------------------------------------------------|
| http://localhost:8000           | Web map: dealers, restaurants, H3 demand, routes, agent    |
| http://localhost:8000/docs      | Interactive API docs (try every endpoint)                  |
| http://localhost:8080           | Kafka UI: topics, messages, schemas, consumer lag          |
| http://localhost:5678           | n8n: the consent workflow and every execution (create a local login on first visit) |
| http://localhost:8085/subjects  | Schema Registry: registered contracts                      |

## Layout

```
docker-compose.yml   all 7 services
Dockerfile           one Python image for `api` and `sink`
schemas/             Avro data contracts
pipeline/            common.py (platform standards) + numbered steps 01-04
db/                  init/ (tables, indexes) + queries.sql (spatial + H3 demos)
api/main.py          Location API
agent/agent.py       mock AI agent (tool-use loop, no LLM needed)
web/index.html       MapLibre web map
n8n/                 exported consent workflow
docs/                architecture map, walkthrough, interview questions
```

## Attribution

Base map tiles © OpenStreetMap contributors (ODbL). All dealers, restaurants and customers are synthetic.
