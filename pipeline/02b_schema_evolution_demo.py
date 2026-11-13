"""STEP 2b — Schema governance: which changes does the registry allow?

The registry for this platform is set to BACKWARD compatibility:
  "a consumer using the NEW schema must still be able to read data written with the OLD one."

We take the registered Dealer schema and try three changes against subject poi.dealer.v1-value.
Nothing is changed in Kafka; the safe change is only *tested*, the breaking one is *attempted*
so you can see the registry refuse it (HTTP 409).
"""
import copy
import json

from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError

from common import load_schema, sr_client

SUBJECT = "poi.dealer.v1-value"
base = json.loads(load_schema("poi_dealer.avsc"))


def variant(change):
    s = copy.deepcopy(base)
    change(s["fields"])
    return Schema(json.dumps(s), "AVRO")


CASES = [
    ("SAFE: add optional field 'dealer_tier' with a default",
     variant(lambda f: f.append({"name": "dealer_tier", "type": "string", "default": "standard"}))),
    ("BREAKING: add required field 'dealer_tier' (no default) - old messages don't have it",
     variant(lambda f: f.append({"name": "dealer_tier", "type": "string"}))),
    ("BREAKING: change 'geocode_confidence' from double to string",
     variant(lambda f: [x.update(type="string") for x in f if x["name"] == "geocode_confidence"])),
]

if __name__ == "__main__":
    sr = sr_client()
    print("Global compatibility level:", sr.get_compatibility())  # set in docker-compose.yml
    print(f"Latest registered version of {SUBJECT}: v{sr.get_latest_version(SUBJECT).version}\n")
    for title, schema in CASES:
        ok = sr.test_compatibility(SUBJECT, schema)
        print(f"{'ACCEPTED' if ok else 'REJECTED':9} {title}")

    print("\nNow actually trying to REGISTER the first breaking change:")
    try:
        sr.register_schema(SUBJECT, CASES[1][1])
        print("  registered (unexpected!)")
    except SchemaRegistryError as e:
        print(f"  Schema Registry said HTTP {e.http_status_code}: {e.error_message[:160]}...")
