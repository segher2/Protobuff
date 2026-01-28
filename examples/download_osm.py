import requests
import json
import time
import random
import os

# -----------------------------
# Overpass endpoints
# -----------------------------

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
]

def fetch_overpass(query, retries=6, base_sleep=5):
    last_error = None

    for attempt in range(1, retries + 1):
        url = random.choice(OVERPASS_ENDPOINTS)

        try:
            response = requests.post(url, data=query, timeout=180)

            if response.status_code == 200:
                return response.json()

            print(
                f"[Overpass] HTTP {response.status_code} "
                f"from {url} (attempt {attempt}/{retries})"
            )
            last_error = response

        except requests.exceptions.RequestException as e:
            print(
                f"[Overpass] Network error from {url} "
                f"(attempt {attempt}/{retries}): {e}"
            )
            last_error = e

        sleep_time = base_sleep * attempt
        print(f"[Overpass] Sleeping {sleep_time}s before retry…")
        time.sleep(sleep_time)

    if hasattr(last_error, "raise_for_status"):
        last_error.raise_for_status()
    else:
        raise RuntimeError("Overpass request failed after retries") from last_error

# -----------------------------
# User parameters
# -----------------------------

BBOX = (52.006, 4.350, 52.030, 4.390)
MAX_FEATURES = 1000
ATTRIBUTE_PROFILE = "few"  # none | few | medium | many

# -----------------------------
# Attribute schema
# -----------------------------

ATTRIBUTE_SCHEMA = {
    "id": lambda e: e["id"],
    "osm_type": lambda e: e["type"],
    "kind": lambda e: (
        e.get("tags", {}).get("amenity")
        or e.get("tags", {}).get("highway")
        or e.get("tags", {}).get("building")
    ),
    "name": lambda e: e.get("tags", {}).get("name"),
    "level": lambda e: e.get("tags", {}).get("level"),
    "height": lambda e: e.get("tags", {}).get("height"),
    "surface": lambda e: e.get("tags", {}).get("surface"),
    "lanes": lambda e: e.get("tags", {}).get("lanes"),
    "source": lambda e: e.get("tags", {}).get("source"),
}

ATTRIBUTE_PROFILES = {
    "none": [],
    "few": ["id", "kind"],
    "medium": ["id", "osm_type", "kind", "name"],
    "many": list(ATTRIBUTE_SCHEMA.keys()),
}

ACTIVE_ATTRIBUTES = ATTRIBUTE_PROFILES[ATTRIBUTE_PROFILE]

# -----------------------------
# Overpass query
# -----------------------------

query = f"""
[out:json][timeout:60];
(
  node["amenity"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
  way["highway"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
  way["building"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
);
out geom;
"""

# -----------------------------
# Fetch data (robust)
# -----------------------------

CACHE_FILE = "examples/data/overpass_cache.json"

if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    print("[Overpass] Loaded cached response")
else:
    data = fetch_overpass(query)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f)
    print("[Overpass] Cached response")

# -----------------------------
# Build GeoJSON
# -----------------------------

features = []

for element in data["elements"]:
    geom = None

    if element["type"] == "node":
        geom = {
            "type": "Point",
            "coordinates": [element["lon"], element["lat"]],
        }

    elif element["type"] == "way":
        coords = [[pt["lon"], pt["lat"]] for pt in element.get("geometry", [])]
        if not coords:
            continue

        if "building" in element.get("tags", {}):
            geom = {"type": "Polygon", "coordinates": [coords]}
        else:
            geom = {"type": "LineString", "coordinates": coords}

    if not geom:
        continue

    properties = {
        key: ATTRIBUTE_SCHEMA[key](element)
        for key in ACTIVE_ATTRIBUTES
    }

    features.append({
        "type": "Feature",
        "geometry": geom,
        "properties": properties,
    })

# -----------------------------
# Feature limit
# -----------------------------

features = features[:MAX_FEATURES]

geojson = {
    "type": "FeatureCollection",
    "features": features,
}

# -----------------------------
# Write output
# -----------------------------

filename = f"examples/data/osm_mixed_{ATTRIBUTE_PROFILE}.geojson"

with open(filename, "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False)

print(f"Wrote {len(features)} features to {filename}")
print(f"Attribute profile: {ATTRIBUTE_PROFILE}")
