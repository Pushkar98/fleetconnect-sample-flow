"""Shared settings and helpers. Every rule here is a *platform standard*:
change it once and the producer, sink and API all follow."""
import json
import math
import os
import time
import unicodedata
from pathlib import Path

import h3
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient

ROOT = Path(__file__).resolve().parent.parent

# Addresses inside the Docker network (see docs/architecture.md, "Address book").
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
PG_DSN = os.getenv("PG_DSN", "postgresql://fleetconnect:fleetconnect@postgis:5432/fleetconnect")

# ---------------------------------------------------------------- data quality
REGION = {"min_lon": 4.6, "max_lon": 5.2, "min_lat": 45.6, "max_lat": 45.9}  # Lyon area
MIN_GEOCODE_CONFIDENCE = 0.6

# ---------------------------------------------------------------------- topics
# Topics are created here, as code, because the broker has auto-create disabled.
# POI topics are *compacted*: Kafka keeps at least the latest event per key
# (dealer_id), so the topic doubles as a replayable "current state" table.
TOPICS = {
    "poi.dealer.v1":        {"partitions": 3, "config": {"cleanup.policy": "compact"}},
    "poi.restaurant.v1":    {"partitions": 3, "config": {"cleanup.policy": "compact"}},
    "customer.location.v1": {"partitions": 3, "config": {"cleanup.policy": "delete",
                                                          "retention.ms": str(7 * 24 * 3600 * 1000)}},
    "poi.rejected.v1":      {"partitions": 1, "config": {"cleanup.policy": "delete"}},
}

# Small gazetteer (place name -> coordinates). Stands in for a geocoding service.
PLACES = {
    "Part-Dieu":             {"lon": 4.8590, "lat": 45.7605, "aliases": ["part-dieu", "part dieu", "partdieu"]},
    "Vieux Lyon":            {"lon": 4.8270, "lat": 45.7625, "aliases": ["vieux lyon", "vieux-lyon", "old town"]},
    "Confluence":            {"lon": 4.8180, "lat": 45.7405, "aliases": ["confluence"]},
    "Gerland":               {"lon": 4.8320, "lat": 45.7270, "aliases": ["gerland"]},
    "Villeurbanne":          {"lon": 4.8800, "lat": 45.7710, "aliases": ["villeurbanne"]},
    "Bron":                  {"lon": 4.9100, "lat": 45.7380, "aliases": ["bron"]},
    "Saint-Priest":          {"lon": 4.9440, "lat": 45.6960, "aliases": ["saint-priest", "saint priest", "st priest"]},
    "Ecully":                {"lon": 4.7780, "lat": 45.7790, "aliases": ["ecully"]},
    "Neuville-sur-Saone":    {"lon": 4.8410, "lat": 45.8760, "aliases": ["neuville", "neuville-sur-saone"]},
    "Saint-Exupery Airport": {"lon": 5.0800, "lat": 45.7260, "aliases": ["airport", "saint-exupery", "saint exupery", "lys"]},
}


def normalise(text: str) -> str:
    """Lower-case and strip accents so 'Écully' matches 'ecully'."""
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower().strip()


def resolve_place(text: str):
    """Return (name, place) for free text like 'the airport', or (None, None)."""
    t = normalise(text)
    for name, p in PLACES.items():
        if any(a in t for a in p["aliases"]):
            return name, p
    return None, None


def in_region(lon: float, lat: float) -> bool:
    return REGION["min_lon"] <= lon <= REGION["max_lon"] and REGION["min_lat"] <= lat <= REGION["max_lat"]


def validate_point(lon, lat, confidence=None):
    """Return a rejection reason, or None if the location is acceptable."""
    if lon is None or lat is None:
        return "MISSING_GEOMETRY"
    if not (-180 <= lon <= 180 and -90 <= lat <= 90):
        return "INVALID_COORDINATE_RANGE"
    if not in_region(lon, lat):
        # Classic bug: a source sent (lat, lon) instead of (lon, lat).
        return "LIKELY_SWAPPED_LON_LAT" if in_region(lat, lon) else "OUT_OF_REGION"
    if confidence is not None and confidence < MIN_GEOCODE_CONFIDENCE:
        return "LOW_GEOCODE_CONFIDENCE"
    return None


# ------------------------------------------------------------------------- H3
def h3_fields(lon: float, lat: float) -> dict:
    # NOTE: h3 takes LATITUDE FIRST. Our standard is lon/lat, so swap here, once.
    res9 = h3.latlng_to_cell(lat, lon, 9)
    return {"h3_res9": res9, "h3_res7": h3.cell_to_parent(res9, 7)}


def cell_polygon(cell: str) -> dict:
    """H3 cell -> GeoJSON Polygon. h3 returns (lat, lng) pairs; GeoJSON wants [lng, lat]."""
    ring = [[lng, lat] for lat, lng in h3.cell_to_boundary(cell)]
    return {"type": "Polygon", "coordinates": [ring + [ring[0]]]}


def wkt_point(lon: float, lat: float) -> str:
    return f"POINT({lon} {lat})"  # WKT is x y = lon lat


def haversine_m(lon1, lat1, lon2, lat2) -> float:
    r = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------- Kafka
def load_schema(filename: str) -> str:
    return (ROOT / "schemas" / filename).read_text()


def sr_client() -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY})


def ensure_topics() -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA})
    existing = admin.list_topics(timeout=15).topics
    missing = [NewTopic(name, num_partitions=c["partitions"], replication_factor=1, config=c["config"])
               for name, c in TOPICS.items() if name not in existing]
    if not missing:
        return
    for name, fut in admin.create_topics(missing).items():
        try:
            fut.result()
            print(f"created topic {name}")
        except Exception as e:  # another process created it first
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise


def pretty(obj) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False, default=str)
