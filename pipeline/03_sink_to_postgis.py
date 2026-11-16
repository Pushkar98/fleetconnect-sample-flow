"""STEP 3 — Kafka -> PostGIS sink (runs continuously as the `sink` container).

Reads poi.dealer.v1, poi.restaurant.v1 and customer.location.v1, decodes Avro using the
schema id embedded in each message, and UPSERTs rows into PostGIS.

Delivery guarantee: offsets are committed only AFTER the DB commit  ->  at-least-once.
A crash can replay a message, but UPSERT by id makes the write idempotent, so a replay
produces the same row. The `event_time <=` guard ignores stale (out-of-order) events.

Watch it: Kafka UI -> Consumers -> postgis-sink (lag should drop to 0).
"""
import signal
import time

import psycopg
from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from common import KAFKA, PG_DSN, ensure_topics, sr_client

POINT = "ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)"  # MakePoint takes (x=lon, y=lat)

UPSERTS = {
    "poi.dealer.v1": f"""
        INSERT INTO poi.dealer (dealer_id, name, address, city, tire_sizes, opening_hours,
               geocode_match_level, geocode_confidence, h3_res9, h3_res7, geom, event_time, source)
        VALUES (%(dealer_id)s, %(name)s, %(address)s, %(city)s, %(tire_sizes)s, %(opening_hours)s,
               %(geocode_match_level)s, %(geocode_confidence)s, %(h3_res9)s, %(h3_res7)s, {POINT},
               %(event_time)s, %(source)s)
        ON CONFLICT (dealer_id) DO UPDATE SET
            name = EXCLUDED.name, address = EXCLUDED.address, city = EXCLUDED.city,
            tire_sizes = EXCLUDED.tire_sizes, opening_hours = EXCLUDED.opening_hours,
            geocode_match_level = EXCLUDED.geocode_match_level, geocode_confidence = EXCLUDED.geocode_confidence,
            h3_res9 = EXCLUDED.h3_res9, h3_res7 = EXCLUDED.h3_res7, geom = EXCLUDED.geom,
            event_time = EXCLUDED.event_time, source = EXCLUDED.source, loaded_at = now()
        WHERE poi.dealer.event_time <= EXCLUDED.event_time""",
    "poi.restaurant.v1": f"""
        INSERT INTO poi.restaurant (restaurant_id, name, cuisine, recommendation_score, price_band,
               geocode_match_level, geocode_confidence, h3_res9, h3_res7, geom, event_time, source)
        VALUES (%(restaurant_id)s, %(name)s, %(cuisine)s, %(recommendation_score)s, %(price_band)s,
               %(geocode_match_level)s, %(geocode_confidence)s, %(h3_res9)s, %(h3_res7)s, {POINT},
               %(event_time)s, %(source)s)
        ON CONFLICT (restaurant_id) DO UPDATE SET
            name = EXCLUDED.name, cuisine = EXCLUDED.cuisine, recommendation_score = EXCLUDED.recommendation_score,
            price_band = EXCLUDED.price_band, geocode_match_level = EXCLUDED.geocode_match_level,
            geocode_confidence = EXCLUDED.geocode_confidence, h3_res9 = EXCLUDED.h3_res9,
            h3_res7 = EXCLUDED.h3_res7, geom = EXCLUDED.geom, event_time = EXCLUDED.event_time,
            source = EXCLUDED.source, loaded_at = now()
        WHERE poi.restaurant.event_time <= EXCLUDED.event_time""",
    "customer.location.v1": f"""
        INSERT INTO customer.location (customer_id, consent_precise_location, geom, h3_res9, h3_res7,
               tire_size, channel, event_time)
        VALUES (%(customer_id)s, %(consent_precise_location)s,
               CASE WHEN %(lon)s::float8 IS NULL THEN NULL ELSE {POINT} END,
               %(h3_res9)s, %(h3_res7)s, %(tire_size)s, %(channel)s, %(event_time)s)
        ON CONFLICT (customer_id) DO UPDATE SET
            consent_precise_location = EXCLUDED.consent_precise_location, geom = EXCLUDED.geom,
            h3_res9 = EXCLUDED.h3_res9, h3_res7 = EXCLUDED.h3_res7, tire_size = EXCLUDED.tire_size,
            channel = EXCLUDED.channel, event_time = EXCLUDED.event_time, loaded_at = now()
        WHERE customer.location.event_time <= EXCLUDED.event_time""",
}

if __name__ == "__main__":
    ensure_topics()
    deserialize = AvroDeserializer(sr_client())  # no schema given: uses the writer's schema by id
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "postgis-sink",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe(list(UPSERTS))
    print("postgis-sink: waiting for messages on", ", ".join(UPSERTS), flush=True)

    # Graceful shutdown: `docker compose stop/restart` sends SIGTERM. Closing the consumer
    # tells Kafka "I'm leaving the group" so a new sink gets the partitions at once,
    # instead of Kafka waiting ~45 s (session timeout) to notice this one is gone.
    running = True

    def stop(signum, frame):
        global running
        running = False

    signal.signal(signal.SIGTERM, stop)

    while running:
        try:
            with psycopg.connect(PG_DSN) as conn:
                counts = {}
                while running:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        if counts:
                            print("loaded", counts, flush=True)
                            counts = {}
                        continue
                    if msg.error():
                        print("kafka error:", msg.error(), flush=True)
                        continue
                    record = deserialize(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                    conn.execute(UPSERTS[msg.topic()], record)
                    conn.commit()                                   # 1. database first
                    consumer.commit(message=msg, asynchronous=False)  # 2. then the Kafka offset
                    counts[msg.topic()] = counts.get(msg.topic(), 0) + 1
        except psycopg.OperationalError as e:
            print("database unavailable, retrying in 5s:", e, flush=True)
            time.sleep(5)

    consumer.close()  # leave the group cleanly (offsets are already committed)
    print("postgis-sink: stopped cleanly", flush=True)
