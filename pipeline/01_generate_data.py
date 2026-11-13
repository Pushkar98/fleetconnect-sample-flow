"""STEP 1 — Generate source data (what upstream systems would export).

Writes two GeoJSON FeatureCollections into data/:
  dealers.geojson      ~200 synthetic tire dealers
  restaurants.geojson  ~150 synthetic restaurants (some placed along sample routes)
Plus a handful of deliberately BAD records so step 2 has something to reject.

Note: no H3, no WKT here. Raw sources don't have them; the producer enriches.
"""
import json
import random

from common import PLACES, ROOT, pretty

random.seed(42)  # same data every run -> reproducible demo

TOWNS = [  # (name, lon, lat, postcode, weight)
    ("Lyon", 4.835, 45.760, "69002", 6), ("Villeurbanne", 4.880, 45.771, "69100", 3),
    ("Venissieux", 4.886, 45.697, "69200", 2), ("Bron", 4.910, 45.738, "69500", 2),
    ("Saint-Priest", 4.944, 45.696, "69800", 2), ("Vaulx-en-Velin", 4.921, 45.778, "69120", 2),
    ("Ecully", 4.778, 45.779, "69130", 1), ("Oullins", 4.807, 45.714, "69600", 1),
    ("Meyzieu", 5.003, 45.767, "69330", 1), ("Rillieux-la-Pape", 4.898, 45.820, "69140", 1),
    ("Decines", 4.959, 45.769, "69150", 1), ("Tassin", 4.760, 45.762, "69160", 1),
    ("Neuville-sur-Saone", 4.841, 45.876, "69250", 1), ("Saint-Genis-Laval", 4.793, 45.695, "69230", 1),
]
STREETS = ["Rue de la Republique", "Avenue Jean Jaures", "Rue Victor Hugo", "Route de Vienne",
           "Avenue de la Liberation", "Rue du Commerce", "Boulevard de l'Europe", "Rue des Ateliers"]
TIRE_SIZES = ["195/65R15", "205/55R16", "215/60R16", "225/45R17", "235/55R18", "315/80R22.5"]
CUISINES = ["Lyonnaise", "French bistro", "Italian", "Japanese", "Lebanese", "Vegetarian", "Seafood", "Brasserie"]


def jitter_point(lon, lat, spread=0.012):
    return round(random.gauss(lon, spread), 6), round(random.gauss(lat, spread * 0.7), 6)


def pick_town():
    return random.choices(TOWNS, weights=[t[4] for t in TOWNS])[0]


def geocode_quality():
    level = random.choices(["rooftop", "street", "city"], weights=[70, 25, 5])[0]
    conf = {"rooftop": (0.9, 1.0), "street": (0.7, 0.9), "city": (0.6, 0.7)}[level]
    return level, round(random.uniform(*conf), 2)


def feature(lon, lat, props):
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, lat]}, "properties": props}


def make_dealers(n=200):
    out = []
    for i in range(1, n + 1):
        town, tlon, tlat, postcode, _ = pick_town()
        lon, lat = jitter_point(tlon, tlat)
        level, conf = geocode_quality()
        out.append(feature(lon, lat, {
            "dealer_id": f"D-{i:04d}",
            "name": f"{random.choice(['Rapid', 'Atlas', 'Rhone', 'Central', 'Route', 'Express'])} Tyres {town}",
            "address": f"{random.randint(1, 180)} {random.choice(STREETS)}, {postcode} {town}",
            "city": town,
            "tire_sizes": sorted(random.sample(TIRE_SIZES, random.randint(2, 5))),
            "opening_hours": random.choice(["Mo-Fr 08:00-18:00; Sa 08:00-12:00", "Mo-Sa 08:30-19:00"]),
            "geocode_match_level": level, "geocode_confidence": conf,
        }))
    # --- deliberately bad records (the validation demo) ---
    bad = [
        ("D-9001", [45.7605, 4.8590], 0.95, "lon/lat swapped by the source system"),
        ("D-9002", [2.3522, 48.8566], 0.95, "valid point, but in Paris: outside our region"),
        ("D-9003", [4.8600, 45.7500], 0.30, "geocoder only matched the city: confidence 0.30"),
        ("D-9004", [4.8600, 95.0000], 0.95, "latitude 95 does not exist"),
    ]
    for did, coords, conf, why in bad:
        out.append(feature(coords[0], coords[1], {
            "dealer_id": did, "name": f"Bad record ({why})", "address": "n/a", "city": "n/a",
            "tire_sizes": ["205/55R16"], "opening_hours": "Mo-Fr 08:00-18:00",
            "geocode_match_level": "city" if conf < 0.6 else "rooftop", "geocode_confidence": conf,
        }))
    return out


def make_restaurants(n=150):
    out, i = [], 0

    def add(lon, lat):
        nonlocal i
        i += 1
        level, conf = geocode_quality()
        out.append(feature(lon, lat, {
            "restaurant_id": f"R-{i:04d}",
            "name": f"{random.choice(['Le Comptoir', 'La Table', 'Le Bistrot', 'Le Bouchon', 'La Cantine', 'Le Relais'])} "
                    f"{random.choice(['du Rhone', 'des Halles', 'de la Gare', 'du Parc', 'des Canuts', 'de Saone'])}",
            "cuisine": random.choice(CUISINES),
            "recommendation_score": round(random.uniform(3.0, 5.0), 1),
            "price_band": random.choice(["€", "€€", "€€", "€€€", "€€€€"]),
            "geocode_match_level": level, "geocode_confidence": conf,
        }))

    # ~40 restaurants close to two sample routes, so the "along my route" demo finds results
    for a, b in [("Part-Dieu", "Saint-Exupery Airport"), ("Vieux Lyon", "Neuville-sur-Saone")]:
        pa, pb = PLACES[a], PLACES[b]
        for _ in range(20):
            t = random.random()
            add(*jitter_point(pa["lon"] + t * (pb["lon"] - pa["lon"]), pa["lat"] + t * (pb["lat"] - pa["lat"]), 0.004))
    while i < n:
        _, tlon, tlat, _, _ = pick_town()
        add(*jitter_point(tlon, tlat))
    # one bad record: no geometry at all
    out.append({"type": "Feature", "geometry": None, "properties": {
        "restaurant_id": "R-9001", "name": "Bad record (no coordinates)", "cuisine": "n/a",
        "recommendation_score": 4.0, "price_band": "€€", "geocode_match_level": "none", "geocode_confidence": 0.0}})
    return out


if __name__ == "__main__":
    (ROOT / "data").mkdir(exist_ok=True)
    for name, feats in [("dealers", make_dealers()), ("restaurants", make_restaurants())]:
        path = ROOT / "data" / f"{name}.geojson"
        path.write_text(json.dumps({"type": "FeatureCollection", "features": feats}, indent=1, ensure_ascii=False))
        print(f"wrote {len(feats):>3} features -> data/{path.name}")
    print("\nExample feature (a GeoJSON Point; coordinates are [longitude, latitude]):")
    print(pretty(json.loads((ROOT / "data/dealers.geojson").read_text())["features"][0]))
