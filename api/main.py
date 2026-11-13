"""FleetConnect Location API (FastAPI) — the ONLY door to the data for every channel.

  Channels          ->  API endpoints                         ->  backing system
  web map           ->  /layers/*, /dealers/nearest, /route, ->  PostGIS (read)
                        /restaurants/along-route, /h3/cell,
                        /analytics/demand-h3
  n8n workflow      ->  /h3/cell, /ingest/customer-location   ->  Kafka (write, Avro)
  AI agent (mock)   ->  /places, /route, /dealers/nearest,    ->  PostGIS (read)
                        /restaurants/along-route

Interactive docs: http://localhost:8000/docs     Web map: http://localhost:8000/
"""
import json
from functools import lru_cache

import h3
import psycopg
from confluent_kafka import Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

from agent.agent import run_agent
from api.control import router as control_router
from pipeline.common import (KAFKA, PG_DSN, PLACES, ROOT, cell_polygon, ensure_topics, h3_fields,
                             haversine_m, in_region, load_schema, now_ms, resolve_place, sr_client,
                             validate_point, wkt_point)

app = FastAPI(title="FleetConnect Location API", version="1.0",
              description="Location services for the FleetConnect CX platform. All coordinates WGS84 lon/lat.")

PT = "ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)"
MIN_GROUP_SIZE = 3  # k-anonymity threshold for published aggregates


def query(sql: str, params: dict | None = None) -> list[dict]:
    with psycopg.connect(PG_DSN, row_factory=dict_row) as conn:
        return conn.execute(sql, params or {}).fetchall()


def to_feature_collection(rows: list[dict]) -> dict:
    """Rows with lon/lat columns -> GeoJSON FeatureCollection (the API's standard response)."""
    feats = []
    for r in rows:
        lon, lat = r.pop("lon"), r.pop("lat")
        feats.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, lat]}, "properties": r})
    return {"type": "FeatureCollection", "features": feats}


# ------------------------------------------------------------------ map layers
@app.get("/layers/dealers", tags=["layers"], summary="All dealers as GeoJSON")
def layer_dealers():
    return to_feature_collection(query(
        "SELECT dealer_id, name, city, tire_sizes, opening_hours, h3_res9, ST_X(geom) lon, ST_Y(geom) lat FROM poi.dealer"))


@app.get("/layers/restaurants", tags=["layers"], summary="All restaurants as GeoJSON")
def layer_restaurants():
    return to_feature_collection(query(
        "SELECT restaurant_id, name, cuisine, recommendation_score, price_band, ST_X(geom) lon, ST_Y(geom) lat "
        "FROM poi.restaurant"))


# ------------------------------------------------------------------- locations
@app.get("/places", tags=["location"], summary="Resolve a place name (mock geocoder)")
def places(q: str | None = None):
    if q:
        name, p = resolve_place(q)
        if not name:
            raise HTTPException(404, f"Unknown place '{q}'. Known: {', '.join(PLACES)}")
        return {"name": name, "lon": p["lon"], "lat": p["lat"]}
    return [{"name": n, "lon": p["lon"], "lat": p["lat"]} for n, p in PLACES.items()]


@app.get("/dealers/nearest", tags=["location"], summary="Nearest dealers, optionally stocking a tire size")
def dealers_nearest(lon: float, lat: float, limit: int = Query(5, le=50), tire_size: str | None = None):
    if not in_region(lon, lat):
        raise HTTPException(422, "Point outside the Lyon service region (remember: lon first, then lat).")
    # Two-step nearest: (1) the GiST index gives a fast shortlist ordered in DEGREES,
    # (2) re-sort that shortlist by true distance in METRES (geography).
    rows = query(f"""
        SELECT * FROM (
            SELECT dealer_id, name, address, tire_sizes, opening_hours,
                   round(ST_Distance(geom::geography, {PT}::geography)) AS distance_m,
                   ST_X(geom) lon, ST_Y(geom) lat
            FROM poi.dealer
            WHERE %(tire)s::text IS NULL OR %(tire)s = ANY (tire_sizes)
            ORDER BY geom <-> {PT}
            LIMIT %(limit)s * 4
        ) shortlist
        ORDER BY distance_m
        LIMIT %(limit)s""", {"lon": lon, "lat": lat, "tire": tire_size, "limit": limit})
    return to_feature_collection(rows)


@lru_cache(maxsize=256)  # repeated route requests are served from memory
def _route(a: str, b: str) -> dict:
    pa, pb = PLACES[a], PLACES[b]
    steps = 12
    coords = [[round(pa["lon"] + (pb["lon"] - pa["lon"]) * i / steps, 6),
               round(pa["lat"] + (pb["lat"] - pa["lat"]) * i / steps, 6)] for i in range(steps + 1)]
    return {"type": "Feature", "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {"from": a, "to": b, "distance_m": round(haversine_m(pa["lon"], pa["lat"], pb["lon"], pb["lat"])),
                           "engine": "mock (straight line). Production: OSRM / Valhalla / a routing API"}}


@app.get("/route", tags=["location"], summary="Driving route between two places (MOCK routing engine)")
def route(from_: str = Query(..., alias="from"), to: str = Query(...)):
    a, _ = resolve_place(from_)
    b, _ = resolve_place(to)
    if not a or not b:
        raise HTTPException(404, f"Unknown place. Known: {', '.join(PLACES)}")
    return _route(a, b)


class AlongRouteIn(BaseModel):
    route: dict = Field(..., description="GeoJSON LineString geometry (lon/lat)")
    buffer_m: int = Field(1000, ge=50, le=5000, description="Corridor half-width in metres")
    limit: int = Field(20, le=100)


@app.post("/restaurants/along-route", tags=["location"], summary="Restaurants inside a corridor around a route")
def restaurants_along_route(body: AlongRouteIn):
    if body.route.get("type") != "LineString":
        raise HTTPException(422, "route must be a GeoJSON LineString geometry")
    params = {"route": json.dumps(body.route), "buf": body.buffer_m, "limit": body.limit}
    rows = query("""
        WITH r AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%(route)s), 4326) AS g)
        SELECT p.restaurant_id, p.name, p.cuisine, p.recommendation_score, p.price_band,
               round(ST_Distance(p.geom::geography, r.g::geography)) AS metres_off_route,
               round(ST_LineLocatePoint(r.g, p.geom)::numeric, 3)    AS position_on_route,
               ST_X(p.geom) lon, ST_Y(p.geom) lat
        FROM poi.restaurant p, r
        WHERE ST_DWithin(p.geom::geography, r.g::geography, %(buf)s)
        ORDER BY position_on_route
        LIMIT %(limit)s""", params)
    corridor = query("SELECT ST_AsGeoJSON(ST_Buffer(ST_SetSRID(ST_GeomFromGeoJSON(%(route)s), 4326)::geography, %(buf)s)::geometry) AS g",
                     params)[0]["g"]
    fc = to_feature_collection(rows)
    fc["corridor"] = json.loads(corridor)  # so the map can draw the searched area
    return fc


@app.get("/h3/cell", tags=["h3"], summary="H3 cell for a point (+ parent and optional neighbour ring)")
def h3_cell(lon: float, lat: float, res: int = Query(9, ge=0, le=15), k: int = Query(0, ge=0, le=3)):
    cell = h3.latlng_to_cell(lat, lon, res)  # h3 is lat-first!
    out = {"cell": cell, "resolution": res, "polygon": cell_polygon(cell),
           "area_km2": round(h3.cell_area(cell, unit="km^2"), 3)}
    if res > 7:
        parent = h3.cell_to_parent(cell, 7)
        out["parent_res7"] = {"cell": parent, "polygon": cell_polygon(parent)}
    if k:
        out["grid_disk"] = [{"cell": c, "polygon": cell_polygon(c)} for c in h3.grid_disk(cell, k)]
    return out


@app.get("/analytics/demand-h3", tags=["analytics"], summary="Customer demand per H3 res7 area (k-anonymised)")
def demand_h3():
    rows = query("""
        SELECT h3_res7, count(*) AS customers, count(*) FILTER (WHERE consent_precise_location) AS consented
        FROM customer.location GROUP BY h3_res7""")
    shown = [r for r in rows if r["customers"] >= MIN_GROUP_SIZE]
    return {"type": "FeatureCollection",
            "features": [{"type": "Feature", "geometry": cell_polygon(r["h3_res7"]), "properties": r} for r in shown],
            "suppressed_areas": len(rows) - len(shown), "min_group_size": MIN_GROUP_SIZE}


# ------------------------------------------------------------------- ingestion
class CustomerLocationIn(BaseModel):
    customer_id: str
    consent_precise_location: bool
    lon: float | None = None
    lat: float | None = None
    h3_res7: str | None = None
    tire_size: str | None = None
    channel: str = "web"


_publisher: tuple | None = None


def publisher():
    """Created on first use so the API starts even if Kafka is still booting."""
    global _publisher
    if _publisher is None:
        ensure_topics()
        _publisher = (Producer({"bootstrap.servers": KAFKA, "enable.idempotence": True, "acks": "all"}),
                      AvroSerializer(sr_client(), load_schema("customer_location.avsc")))
    return _publisher


@app.post("/ingest/customer-location", tags=["ingestion"],
          summary="Publish a consent-checked customer location to Kafka (called by n8n)")
def ingest_customer_location(c: CustomerLocationIn):
    if c.consent_precise_location:
        reason = validate_point(c.lon, c.lat)
        if reason:
            raise HTTPException(422, reason)
        record = {"lon": c.lon, "lat": c.lat, "geometry_wkt": wkt_point(c.lon, c.lat), **h3_fields(c.lon, c.lat)}
    else:
        # Defence in depth: n8n should already have removed coordinates. If not, refuse.
        if c.lon is not None or c.lat is not None:
            raise HTTPException(422, "Precise coordinates must not be sent without consent.")
        if not c.h3_res7 or not h3.is_valid_cell(c.h3_res7) or h3.get_resolution(c.h3_res7) != 7:
            raise HTTPException(422, "Without consent, a valid H3 resolution-7 cell is required.")
        record = {"lon": None, "lat": None, "geometry_wkt": None, "h3_res9": None, "h3_res7": c.h3_res7}
    record |= {"customer_id": c.customer_id, "consent_precise_location": c.consent_precise_location,
               "tire_size": c.tire_size, "channel": c.channel, "event_time": now_ms()}
    producer, serializer = publisher()
    topic = "customer.location.v1"
    producer.produce(topic, key=c.customer_id, value=serializer(record, SerializationContext(topic, MessageField.VALUE)))
    producer.flush(5)
    return {"status": "published", "topic": topic,
            "precision": "point" if c.consent_precise_location else "h3_res7", "record": record}


# ----------------------------------------------------------------------- agent
class AskIn(BaseModel):
    question: str


@app.post("/agent/ask", tags=["agent"], summary="Ask the (mock) AI agent; it answers using the API tools only")
def agent_ask(body: AskIn):
    return run_agent(body.question)


@app.get("/health", tags=["ops"])
def health():
    return {"status": "ok"}


# Demo-only control panel endpoints (/control/*), see api/control.py.
app.include_router(control_router)

# The web map is plain static files served from /web. Mounted LAST so API routes win.
app.mount("/", StaticFiles(directory=ROOT / "web", html=True), name="web")
