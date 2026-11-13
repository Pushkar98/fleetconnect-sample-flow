"""STEP 2 — Validate, enrich and publish POIs to Kafka as Avro.

For every GeoJSON feature:
  1. validate   CRS/range, region bounds, geocode confidence  (data quality gate)
  2. enrich     add h3_res9, h3_res7, geometry_wkt              (platform standards)
  3. serialize  Avro, schema looked up / registered in Schema Registry (data contract)
  4. publish    key = POI id  -> poi.dealer.v1 / poi.restaurant.v1
                invalid       -> poi.rejected.v1 (JSON with the reason; a dead-letter topic)

Watch it in Kafka UI: http://localhost:8080 -> Topics.
"""
import json

from confluent_kafka import Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from common import (KAFKA, ROOT, ensure_topics, h3_fields, load_schema, now_ms, pretty,
                    sr_client, validate_point, wkt_point)


def build_dealer(p, lon, lat):
    return {"dealer_id": p["dealer_id"], "name": p["name"], "address": p["address"], "city": p["city"],
            "tire_sizes": p["tire_sizes"], "opening_hours": p["opening_hours"],
            "lon": lon, "lat": lat, "geometry_wkt": wkt_point(lon, lat), **h3_fields(lon, lat),
            "geocode_match_level": p["geocode_match_level"], "geocode_confidence": p["geocode_confidence"],
            "event_time": now_ms(), "source": "dealer-master-export"}


def build_restaurant(p, lon, lat):
    return {"restaurant_id": p["restaurant_id"], "name": p["name"], "cuisine": p["cuisine"],
            "recommendation_score": p["recommendation_score"], "price_band": p["price_band"],
            "lon": lon, "lat": lat, "geometry_wkt": wkt_point(lon, lat), **h3_fields(lon, lat),
            "geocode_match_level": p["geocode_match_level"], "geocode_confidence": p["geocode_confidence"],
            "event_time": now_ms(), "source": "restaurant-guide-export"}


FEEDS = [  # (input file, topic, schema, id field, builder)
    ("dealers.geojson", "poi.dealer.v1", "poi_dealer.avsc", "dealer_id", build_dealer),
    ("restaurants.geojson", "poi.restaurant.v1", "poi_restaurant.avsc", "restaurant_id", build_restaurant),
]

if __name__ == "__main__":
    ensure_topics()
    # enable.idempotence: retries can't create duplicates or reorder messages within a partition
    producer = Producer({"bootstrap.servers": KAFKA, "enable.idempotence": True, "acks": "all"})
    registry = sr_client()

    for filename, topic, schema_file, id_field, build in FEEDS:
        serializer = AvroSerializer(registry, load_schema(schema_file))  # registers schema on first use
        features = json.loads((ROOT / "data" / filename).read_text())["features"]
        ok, rejected, example = 0, [], None

        for f in features:
            props = f["properties"]
            lon, lat = (f["geometry"] or {}).get("coordinates", [None, None])
            reason = validate_point(lon, lat, props.get("geocode_confidence"))
            if reason:
                rejected.append((props[id_field], reason))
                producer.produce("poi.rejected.v1", key=props[id_field],
                                 value=json.dumps({"source_topic": topic, "reason": reason, "record": f}))
                continue
            record = build(props, lon, lat)
            example = example or record
            producer.produce(topic, key=record[id_field],
                             value=serializer(record, SerializationContext(topic, MessageField.VALUE)))
            ok += 1
        producer.flush()

        print(f"\n{topic}: published {ok}, rejected {len(rejected)}")
        for rid, reason in rejected:
            print(f"   rejected {rid}: {reason}")
        print(f"example {topic} message value (before Avro encoding):\n{pretty(example)}")

    print("\nRegistered subjects:", registry.get_subjects())
