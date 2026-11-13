"""Demo control panel endpoints (/control/*) — used by the "Produce & test" tab of the web map.

These exist only so you can drive and observe the pipeline from a browser. In a real platform
they would NOT be part of the public Location API: producing test data, peeking at topics and
reading consumer lag belong to internal tooling (Kafka UI, CI jobs, ops dashboards).

Every action reuses the real pipeline code, so what you click is exactly what the scripts do.
"""
import importlib.util
import json
import subprocess
import sys
import threading
import time

import httpx
import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from pipeline.common import KAFKA, PG_DSN, ROOT, TOPICS, ensure_topics, load_schema, sr_client

router = APIRouter(prefix="/control", tags=["control (demo only)"])
N8N_WEBHOOK = "http://n8n:5678/webhook/customer-signup"
SINK_GROUP = "postgis-sink"
SINK_TOPICS = ["poi.dealer.v1", "poi.restaurant.v1", "customer.location.v1"]

# The numbered pipeline scripts import `from common import ...`, so make that importable.
sys.path.insert(0, str(ROOT / "pipeline"))
_spec = importlib.util.spec_from_file_location("publish_pois", ROOT / "pipeline" / "02_publish_pois.py")
publish_pois = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(publish_pois)  # gives us build_dealer() and the same validate_point()

_lock = threading.Lock()
_clients: dict = {}


def clients():
    """Kafka/registry clients, created once on first use."""
    with _lock:
        if not _clients:
            ensure_topics()
            sr = sr_client()
            _clients.update(
                sr=sr,
                producer=Producer({"bootstrap.servers": KAFKA, "enable.idempotence": True, "acks": "all"}),
                dealer_ser=AvroSerializer(sr, load_schema("poi_dealer.avsc")),
                deser=AvroDeserializer(sr),
                # Same group id as the sink, but never subscribes: only used to READ its committed offsets.
                lag_reader=Consumer({"bootstrap.servers": KAFKA, "group.id": SINK_GROUP, "enable.auto.commit": False}),
            )
        return _clients


# --------------------------------------------------------- run pipeline steps
SCRIPTS = {
    "generate": "pipeline/01_generate_data.py",
    "publish": "pipeline/02_publish_pois.py",
    "schema-test": "pipeline/02b_schema_evolution_demo.py",
    "signups": "pipeline/04_send_signups.py",
}


@router.post("/run/{step}", summary="Run a numbered pipeline script and return its console output")
def run_step(step: str, n: int = 50):
    if step not in SCRIPTS:
        raise HTTPException(404, f"Unknown step. Choose one of {list(SCRIPTS)}")
    args = [SCRIPTS[step]] + ([str(n)] if step == "signups" else [])
    p = subprocess.run([sys.executable, *args], cwd=ROOT, capture_output=True, text=True, timeout=600)
    return {"command": "python " + " ".join(args), "exit_code": p.returncode, "output": (p.stdout + p.stderr)[-8000:]}


# ----------------------------------------------------- produce single records
class DealerIn(BaseModel):
    lon: float
    lat: float
    geocode_confidence: float = Field(0.95, ge=0, le=1)
    swap_lon_lat: bool = Field(False, description="Simulate the classic source bug: send (lat, lon)")
    tire_sizes: list[str] = ["205/55R16", "225/45R17"]


@router.post("/dealer", summary="Publish ONE dealer from a map click (through the real validation + Avro path)")
def add_dealer(d: DealerIn):
    c = clients()
    lon, lat = (d.lat, d.lon) if d.swap_lon_lat else (d.lon, d.lat)
    dealer_id = f"D-U{int(time.time() * 1000) % 10**8}"
    props = {"dealer_id": dealer_id, "name": f"Clicked dealer {dealer_id}", "address": "added from the map",
             "city": "n/a", "tire_sizes": d.tire_sizes, "opening_hours": "Mo-Fr 08:00-18:00",
             "geocode_match_level": "rooftop" if d.geocode_confidence >= 0.9 else "street" if d.geocode_confidence >= 0.6 else "city",
             "geocode_confidence": d.geocode_confidence}
    delivered = {}

    def on_delivery(err, msg):
        delivered.update(error=str(err) if err else None, partition=msg.partition(), offset=msg.offset())

    reason = publish_pois.validate_point(lon, lat, d.geocode_confidence)
    if reason:
        topic = "poi.rejected.v1"
        feature = {"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, lat]}, "properties": props}
        c["producer"].produce(topic, key=dealer_id, on_delivery=on_delivery,
                              value=json.dumps({"source_topic": "poi.dealer.v1", "reason": reason, "record": feature}))
        c["producer"].flush(10)
        return {"status": "rejected", "reason": reason, "sent_coordinates": [lon, lat], "topic": topic,
                "key": dealer_id, **delivered}

    record = publish_pois.build_dealer(props, lon, lat)
    topic = "poi.dealer.v1"
    value = c["dealer_ser"](record, SerializationContext(topic, MessageField.VALUE))
    c["producer"].produce(topic, key=dealer_id, value=value, on_delivery=on_delivery)
    c["producer"].flush(10)
    return {"status": "published", "topic": topic, "key": dealer_id, **delivered,
            "schema_id": int.from_bytes(value[1:5], "big"), "avro_bytes": len(value),
            "json_bytes": len(json.dumps(record)), "record": record}


class SignupIn(BaseModel):
    lon: float
    lat: float
    consent_precise_location: bool
    tire_size: str = "205/55R16"


@router.post("/signup", summary="Send ONE customer sign-up to the n8n webhook (the real consent workflow)")
def signup(s: SignupIn):
    body = {"customer_id": f"C-U{int(time.time() * 1000) % 10**8}", "lon": round(s.lon, 6), "lat": round(s.lat, 6),
            "consent_precise_location": s.consent_precise_location, "tire_size": s.tire_size, "channel": "web"}
    r = httpx.post(N8N_WEBHOOK, json=body, timeout=20)
    if r.status_code == 404:
        raise HTTPException(503, "n8n workflow is not active. Import + activate it (README step 4).")
    try:
        out = r.json()
    except ValueError:
        out = r.text
    return {"sent_to_n8n": body, "n8n_status": r.status_code, "result": out}


# ------------------------------------------------------------- observe it all
@router.get("/status", summary="Live numbers: messages per topic, sink lag, PostGIS rows, schema versions")
def status():
    c = clients()
    with _lock:
        meta = c["lag_reader"].list_topics(timeout=5)
        topics = {}
        for name in TOPICS:
            if name not in meta.topics:
                continue
            parts = [TopicPartition(name, p) for p in meta.topics[name].partitions]
            marks = [c["lag_reader"].get_watermark_offsets(tp, timeout=5) for tp in parts]
            info = {"partitions": len(parts), "messages": sum(hi - lo for lo, hi in marks)}
            if name in SINK_TOPICS:  # lag = newest offset - offset the sink has committed
                committed = c["lag_reader"].committed(parts, timeout=5)
                info["sink_lag"] = sum(hi - (cm.offset if cm.offset >= 0 else lo) for (lo, hi), cm in zip(marks, committed))
            topics[name] = info
    with psycopg.connect(PG_DSN) as conn:
        db = dict(zip(["dealers", "restaurants", "customers", "customers_point", "customers_area_only"], conn.execute("""
            SELECT (SELECT count(*) FROM poi.dealer), (SELECT count(*) FROM poi.restaurant),
                   (SELECT count(*) FROM customer.location),
                   (SELECT count(*) FROM customer.location WHERE geom IS NOT NULL),
                   (SELECT count(*) FROM customer.location WHERE geom IS NULL)""").fetchone()))
    schemas = {s: c["sr"].get_latest_version(s).version for s in c["sr"].get_subjects()}
    return {"topics": topics, "postgis": db, "schemas": schemas}


@router.get("/messages", summary="Peek at the newest messages on a topic (decoded)")
def messages(topic: str, n: int = 5):
    if topic not in TOPICS:
        raise HTTPException(404, f"Unknown topic. Choose one of {list(TOPICS)}")
    c = clients()
    peek = Consumer({"bootstrap.servers": KAFKA, "group.id": "fc-ui-peek", "enable.auto.commit": False})
    try:
        parts, want = [], 0
        for p in peek.list_topics(topic, timeout=5).topics[topic].partitions:
            lo, hi = peek.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
            start = max(lo, hi - n)
            if hi > start:
                parts.append(TopicPartition(topic, p, start))
                want += hi - start
        peek.assign(parts)
        got, deadline = [], time.time() + 5
        while len(got) < want and time.time() < deadline:
            got += [m for m in peek.consume(num_messages=want, timeout=1) if not m.error()]
    finally:
        peek.close()

    out = []
    for m in sorted(got, key=lambda m: m.timestamp()[1], reverse=True)[:n]:
        raw = m.value()
        if topic == "poi.rejected.v1":
            value, fmt = json.loads(raw), "JSON"
        else:
            value = c["deser"](raw, SerializationContext(topic, MessageField.VALUE))
            fmt = f"Avro, schema id {int.from_bytes(raw[1:5], 'big')}"
        out.append({"partition": m.partition(), "offset": m.offset(), "key": m.key().decode() if m.key() else None,
                    "timestamp_ms": m.timestamp()[1], "format": fmt, "bytes": len(raw), "value": value})
    return out
