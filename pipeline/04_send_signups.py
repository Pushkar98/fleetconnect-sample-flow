"""STEP 4 — Simulate customer sign-ups hitting the n8n webhook.

Each sign-up carries a precise location and a consent flag (~60% consent).
n8n decides what is allowed to leave the workflow (see n8n/customer-consent.json):
  consent = true   -> precise point published
  consent = false  -> point converted to an H3 res7 area; coordinates are dropped
"""
import random
import sys
import time

import httpx

WEBHOOK = "http://n8n:5678/webhook/customer-signup"
AREAS = [(4.835, 45.760), (4.880, 45.771), (4.886, 45.697), (4.910, 45.738), (4.944, 45.696),
         (4.921, 45.778), (4.778, 45.779), (5.003, 45.767), (4.898, 45.820), (4.807, 45.714)]
SIZES = ["195/65R15", "205/55R16", "215/60R16", "225/45R17", "235/55R18"]

if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 300
    run = time.strftime("%H%M%S")  # unique per run, so each run adds new customers
    stats = {"point": 0, "h3_res7": 0, "error": 0}
    shown = set()  # print one example of each path
    with httpx.Client(timeout=10) as client:
        for i in range(1, n + 1):
            lon, lat = random.choice(AREAS)
            body = {"customer_id": f"C-{run}-{i:04d}",
                    "lon": round(random.gauss(lon, 0.015), 6), "lat": round(random.gauss(lat, 0.01), 6),
                    "consent_precise_location": random.random() < 0.6,
                    "tire_size": random.choice(SIZES), "channel": random.choice(["web", "app"])}
            r = client.post(WEBHOOK, json=body)
            if r.status_code == 404:
                sys.exit("n8n webhook not found: import + activate the workflow first (README step 4).")
            if r.status_code >= 300:
                stats["error"] += 1
                print("error", r.status_code, r.text[:200])
                continue
            out = r.json()
            stats[out.get("precision", "error")] += 1
            if out.get("precision") not in shown:
                shown.add(out.get("precision"))
                print(f"\n[{out.get('precision')}] sent to n8n:  {body}\n[{out.get('precision')}] on Kafka:     {out.get('record')}")
    print(f"\n{n} sign-ups -> {stats}")
